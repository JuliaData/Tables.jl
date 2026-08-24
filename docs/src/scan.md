# Scan Requests

`Tables.Scan` is a logical request for projection, filtering, row bounds, and
output conversion. It does not require a physical full-table scan. A source can
use an index, statistics, partition pruning, or any other exact optimization.

The API has two roles:

- Consumers construct a [`Tables.Scan`](@ref) and pass it to a source or to
  [`Tables.scan`](@ref).
- Source implementations inspect, resolve, and consume supported parts of the
  request while they read data.

## Consumer API

```@example scan
using Tables

table = (
    id = [1, 2, 3, 4],
    status = ["trial", "active", "active", "closed"],
    price = [5, 20, 12, 30],
)

request = Tables.Scan(
    select = (:id, :price => Float64 => :amount),
    filter = Tables.colcmp(==, Tables.col(:status), "active") &
             (Tables.col(:price) >= 10),
    limit = 10,
)

Tables.scan(table, request)
```

The operation order is fixed:

1. Resolve references against source column names.
2. Evaluate the filter.
3. Apply `offset`, then `limit`, to qualifying rows.
4. Select, rename, and convert output columns.

Filters always use source names. A rename does not change the name visible to
the filter.

### Selection

`select=Tables.All()` is the default and keeps every column. `select=()` selects
zero columns while preserving the result's row count.

A selection accepts names, indices, regular expressions, `Tables.All()`, and
`Tables.Not(...)`. Pair forms add a type override or output name:

```julia
Tables.Scan(select = (
    :id,
    r"^metric_",
    :price => Float64,
    :qty => Int => :quantity,
))
```

Selection order defines output order. Duplicate output names are an error.

### Filter expressions

`Tables.col(ref)` creates a column reference. The supported predicates are:

- `Tables.colcmp(op, column, value)` for `==`, `!=`, `<`, `<=`, `>`, and `>=`.
- Ordered shorthand such as `Tables.col(:price) >= 10`.
- `Tables.colin(column, values)` for membership.
- `Tables.isnull(column)` and its negation for `missing` checks.
- `startswith`, `endswith`, and `contains` for strings.
- `&`, `|`, and `!` for Boolean composition.

Expression nodes contain plain data. They do not store callbacks. This makes a
request inspectable and suitable for serialization, pushdown, and static
compilation.

### Missing values

Filter evaluation uses SQL-like three-valued logic:

- Comparisons, membership, and string predicates lift a missing column value to
  `missing`.
- `Tables.isnull(column)` returns `true` for `missing` and `false` otherwise.
- `&`, `|`, and `!` propagate `missing` with three-valued Boolean rules.
- The top-level filter keeps a row only when its result is exactly `true`.

These lifting rules are deliberate. Julia functions other than comparison
operators do not generally lift `missing` on their own.

The name `Tables.isnull` also avoids defining `Base.ismissing(::Tables.Col)`.
Such a method would specialize a broad Base fallback and can invalidate
unrelated compiled code when Tables loads.

### Unmatched references

The default `validate=true` rejects any selection or filter reference that does
not match the source schema. With `validate=false`, an unmatched selection is
dropped and an unmatched filter column behaves as an all-missing column. This
mode supports schema evolution when fields can be absent.

## Source Implementations

A source does not need to support every operation. It can consume the parts it
can implement exactly and pass the rest to [`Tables.scan`](@ref). It can also
reject a request that it cannot safely execute.

[`Tables.resolve`](@ref) resolves a request against source names. Its
`BoundScan` result contains:

- `columns`: selected source indices, output names, and type overrides.
- `filter`: a filter with positional references normalized to source names.
- `filtercols`: source indices required by the filter.
- `limit`, `offset`, and `validate`: the remaining row and validation settings.

The normalized filter can be evaluated over a table containing only the
`filtercols` columns:

```julia
bound = Tables.resolve(request, source_names)
mask = Tables.filtermask(bound, predicate_columns)
```

If a source consumes an axis, it removes that axis from the residual. The
axes compose in a fixed order — filter, then row bounds, then projection —
so an axis can only be removed together with every axis that executes before
it. A source that evaluated the filter itself (for example with
`Tables.filtermask`) hands the rest to the generic executor:

```julia
residual = Tables.Scan(request; filter=nothing)
result = Tables.scan(filtered_columns, residual)
```

A source that also applied `limit`/`offset` to the qualifying rows strips
those too (`Tables.Scan(request; filter=nothing, limit=nothing, offset=0)`,
leaving only projection). Two constraints follow from the ordering:

- `limit`/`offset` count qualifying rows, after the filter. A source that
  cannot consume the filter must leave the row bounds in the residual as
  well.
- Projection may be stripped (`select=Tables.All()`, the projection identity)
  only when no residual filter references a column the projection dropped or
  renamed — the residual filter still uses source column names.

Only remove work that the source performed exactly.

A statistics check that only prunes impossible partitions does not consume the
filter.

`Tables.OpNode(name, args)` is the extension point for source-specific,
plain-data operations. A source can recognize and consume a named operation
before it calls `Tables.resolve`. Resolution and the generic executor reject an
unconsumed `OpNode`.

Zero-column results retain their row count. Sources should preserve the same
property when they return a fully pushed result.

```@docs; canonical = false
Tables.Scan
Tables.scan
Tables.col
Tables.colcmp
Tables.colin
Tables.isnull
Tables.resolve
Tables.filtermask
Tables.describe
```
