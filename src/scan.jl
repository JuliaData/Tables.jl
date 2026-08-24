# Tables.Scan: a plain-data scan request — column selection/renaming/type
# overrides, row predicates, and limits — one shared vocabulary that any
# data source can accept and push down. Two pieces:
#
#     Tables.Scan(...)             the request (a value; no Function fields)
#     Tables.scan(table, scan)     the generic executor over any Tables.jl table
#
# Sources that can push a scan down accept it as a keyword — `CSV.File(path;
# scan=Tables.Scan(...))`, `Arrow.Table(path; scan=...)` — and apply what they
# can WHILE materializing (skipping columns never parsed, rows never decoded).
# What a source cannot push down it either rejects with an ArgumentError, or
# hands to `Tables.scan` as a residual (`Tables.Scan(scan; select=All())`
# strips the axes it already handled). Sources are free to support only a
# subset — the value is the shared vocabulary and the pushdown, not a
# capability-negotiation protocol.
#
# Design commitments (each learned the hard way by prior systems):
#   * Every node is a plain value — no `Function` fields anywhere. Closures are
#     opaque to pushdown and to static compilation; expression values are not.
#   * The source's answer is the residual itself, not a capability
#     advertisement. Pushed and residual work may overlap (a source that
#     prunes by a predicate but cannot guarantee exactness keeps the filter in
#     the residual).
#   * Missing follows SQL WHERE: a predicate evaluating to `missing` excludes
#     the row. `isnull`/`!isnull` are first-class nodes; comparisons never
#     match missing.
#   * Filters reference SOURCE column names (pre-rename); the pipeline order
#     is fixed: resolve → filter → offset/limit → project/rename/type.
#   * Selection order defines output order.

# --- column references & selection items --------------------------------------

import DataAPI: All

"""
    Tables.Not(ref)

Selection item excluding `ref` (a name, index, `Regex`, or a `Tuple`/`Vector`
of those) from the full column set. `Not` items cannot be mixed with positive
selections in the same `select`.
"""
struct Not{T}
    ref::T
end

const ColRef = Union{Symbol, String, Int, Regex}

"""
    Tables.SelectItem

One resolved element of `Scan.select`: a column reference plus optional type
override and output name. Users never construct these directly — the `Scan`
constructor lowers `ref`, `ref => name`, `ref => Type`, and
`ref => Type => name` forms.
"""
struct SelectItem
    ref::Union{Symbol, String, Int, Regex, Not, All}
    type::Union{Nothing, Type}
    rename::Union{Nothing, Symbol}
end

function _selectitem(x::Not)
    refs = x.ref isa Union{Tuple, AbstractVector} ? x.ref : (x.ref,)
    all(r -> r isa ColRef, refs) ||
        throw(ArgumentError("Not references must be Symbol/String/Int/Regex values"))
    return SelectItem(x, nothing, nothing)
end
function _selectitem(x::All)
    # DataAPI.All(cols...) with arguments is deprecated DataFrames syntax;
    # silently selecting every column would be a data-loss trap
    isempty(x.cols) ||
        throw(ArgumentError("Tables.All() takes no arguments in a scan selection; " *
                            "list the columns directly instead of All(cols...)"))
    return SelectItem(x, nothing, nothing)
end
_selectitem(x::Union{Symbol, String, Int, Regex, Not}) = SelectItem(x, nothing, nothing)
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, Symbol}) = SelectItem(p.first, nothing, p.second)
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, String}) = SelectItem(p.first, nothing, Symbol(p.second))
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, <:Type}) = SelectItem(p.first, p.second, nothing)
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, <:Pair{<:Type, Symbol}}) =
    SelectItem(p.first, p.second.first, p.second.second)
_selectitem(x) = throw(ArgumentError("unsupported select item $(repr(x)); expected a column " *
    "reference (Symbol/String/Int/Regex/Not/All), or ref => name, ref => Type, ref => Type => name"))

# --- the expression algebra ----------------------------------------------------
#
# A small closed vocabulary — the intersection every surveyed engine supports
# and every statistics pruner can use. Growth happens through `OpNode`
# (name + children), never through new struct kinds every consumer must learn.

abstract type ScanExpr end

struct Col <: ScanExpr
    ref::Union{Symbol, String, Int}
end
"""
    Tables.col(ref)

A column reference inside a `Scan` filter: `col(:price) > 100`. Comparisons
against literals, `colin`, `isnull`, `startswith`/`endswith`/`contains`, and
`&`/`|`/`!` build plain expression values.
"""
col(r::Union{Symbol, AbstractString, Int}) = Col(r isa AbstractString ? String(r) : r)

@enum ComparisonOperator::UInt8 begin
    OP_EQ = 0x01
    OP_NE = 0x02
    OP_LT = 0x03
    OP_LE = 0x04
    OP_GT = 0x05
    OP_GE = 0x06
end
const _OPNAMES = ("==", "!=", "<", "<=", ">", ">=")
_colcolerr() = throw(ArgumentError("column-to-column comparisons are not supported; " *
                                   "compare a column against a literal value"))

struct Cmp{T} <: ScanExpr
    op::ComparisonOperator
    lhs::Col
    rhs::T
    function Cmp(op::ComparisonOperator, lhs::Col, rhs::T) where {T}
        rhs isa Col && _colcolerr()
        return new{T}(op, lhs, rhs)
    end
end
function Cmp(op::Integer, lhs::Col, rhs)
    0x01 <= op <= 0x06 || throw(ArgumentError("unknown comparison operator code $op"))
    return Cmp(ComparisonOperator(op), lhs, rhs)
end

_comparisonoperator(::typeof(==)) = OP_EQ
_comparisonoperator(::typeof(!=)) = OP_NE
_comparisonoperator(::typeof(<)) = OP_LT
_comparisonoperator(::typeof(<=)) = OP_LE
_comparisonoperator(::typeof(>)) = OP_GT
_comparisonoperator(::typeof(>=)) = OP_GE
_comparisonoperator(op) = throw(ArgumentError(
    "unsupported comparison function $(repr(op)); expected ==, !=, <, <=, >, or >=",
))
"""
    Tables.colcmp(op, col, value)

Build a comparison predicate for a column and a literal value. `op` must be
`==`, `!=`, `<`, `<=`, `>`, or `>=`. Ordered comparisons also support the
shorthand `col(:x) < value` for numeric, string, and character values.

Use `colcmp` for equality so `==` and `isequal` on expression objects keep
their normal Boolean contracts.
"""
colcmp(op, c::Col, value) = Cmp(_comparisonoperator(op), c, value)

struct In{T} <: ScanExpr
    lhs::Col
    values::T
    function In(lhs::Col, values::T) where {T}
        values isa Col && _colcolerr()
        return new{T}(lhs, values)
    end
end

"""
    Tables.colin(col, values)

Build a membership predicate: `colin(col(:status), ("active", "trial"))`.
If the column value is `missing`, the predicate result is also `missing`.
"""
colin(c::Col, values) = In(c, values)

struct IsNull <: ScanExpr
    lhs::Col
    negated::Bool
end

"""
    Tables.isnull(col)

Build a predicate that is true when the column value is Julia's `missing`.
Use `!Tables.isnull(col)` to match values that are not `missing`.

The `isnull` name is deliberate. Defining `Base.ismissing(::Col)` would
specialize Base's broad fallback and invalidate unrelated precompiled code
when Tables loads. Use `isnull` rather than `colcmp(==, col(:x), missing)`,
which follows SQL semantics and matches no row.
"""
function isnull end
isnull(c::Col) = IsNull(c, false)

@enum StringPredicate::UInt8 begin
    STR_STARTSWITH = 0x01
    STR_ENDSWITH = 0x02
    STR_CONTAINS = 0x03
end

struct StrPred <: ScanExpr
    kind::StringPredicate
    lhs::Col
    s::String
    function StrPred(kind::StringPredicate, lhs::Col, s::AbstractString)
        return new(kind, lhs, String(s))
    end
end
function StrPred(kind::Integer, lhs::Col, s::AbstractString)
    0x01 <= kind <= 0x03 || throw(ArgumentError("unknown string predicate code $kind"))
    return StrPred(StringPredicate(kind), lhs, s)
end

struct AndExpr <: ScanExpr
    args::Vector{ScanExpr}
end
struct OrExpr <: ScanExpr
    args::Vector{ScanExpr}
end
struct NotExpr <: ScanExpr
    arg::ScanExpr
end
struct AlwaysTrue <: ScanExpr end
struct AlwaysFalse <: ScanExpr end

"""
    Tables.OpNode(name, args)

The expression algebra's growth channel. Every node has a symbolic `name` and
plain-data arguments. A source can recognize and consume that name before
generic resolution. An unconsumed `OpNode` is rejected by `Tables.resolve` and
the generic executor.
"""
struct OpNode <: ScanExpr
    name::Symbol
    args::Vector{Any}
end

for T in (Number, AbstractString, AbstractChar)
    @eval begin
        Base.:<(c::Col, v::$T) = colcmp(<, c, v)
        Base.:<(v::$T, c::Col) = colcmp(>, c, v)
        Base.:<=(c::Col, v::$T) = colcmp(<=, c, v)
        Base.:<=(v::$T, c::Col) = colcmp(>=, c, v)
    end
end
Base.:(==)(a::Col, b::Col) = a.ref == b.ref
Base.isequal(a::Col, b::Col) = isequal(a.ref, b.ref)
Base.hash(c::Col, h::UInt) = hash(c.ref, h + 0x6f38a7d1)
Base.:<(::Col, ::Col) = _colcolerr()
Base.:<=(::Col, ::Col) = _colcolerr()

Base.startswith(c::Col, s::AbstractString) = StrPred(STR_STARTSWITH, c, String(s))
Base.endswith(c::Col, s::AbstractString) = StrPred(STR_ENDSWITH, c, String(s))
Base.contains(c::Col, s::AbstractString) = StrPred(STR_CONTAINS, c, String(s))

_ands(e::AndExpr) = e.args
_ands(e::ScanExpr) = ScanExpr[e]
_ors(e::OrExpr) = e.args
_ors(e::ScanExpr) = ScanExpr[e]
Base.:&(a::ScanExpr, b::ScanExpr) = AndExpr(vcat(_ands(a), _ands(b)))
Base.:|(a::ScanExpr, b::ScanExpr) = OrExpr(vcat(_ors(a), _ors(b)))
Base.:!(e::ScanExpr) = NotExpr(e)
Base.:!(e::NotExpr) = e.arg
Base.:!(e::IsNull) = IsNull(e.lhs, !e.negated)

# a bare Col is not a predicate; catch the likely mistake early
_checkpredicate(::Nothing) = nothing
function _checkpredicate(e::Union{AndExpr, OrExpr})
    foreach(_checkpredicate, e.args)
    return e
end
function _checkpredicate(e::NotExpr)
    _checkpredicate(e.arg)
    return e
end
_checkpredicate(e::ScanExpr) = e
_checkpredicate(e::Col) =
    throw(ArgumentError("a bare column reference is not a predicate; compare it against a value"))
_checkpredicate(x) =
    throw(ArgumentError("filter must be a Tables.ScanExpr built from Tables.col(...), got $(typeof(x))"))

# --- the Scan value --------------------------------------------------------------

"""
    Tables.Scan(; select=Tables.All(), filter=nothing, limit=nothing, offset=nothing, validate=true)

A scan request: what to keep, what to call it, how to type it, which rows
qualify, and how many. Plain data all the way down — see `Tables.scan` for
the generic executor and the module comment for how sources push it down.

  * `select`: a column reference or tuple/vector of select items
    (`ref`, `ref => name`, `ref => Type`, `ref => Type => name`;
    refs are `Symbol`/`String`/`Int`/`Regex`/`Tables.Not`/`Tables.All`).
    `Tables.All()` keeps every column; `()` selects zero columns. Selection
    order = output order.
  * `filter`: an expression built from `Tables.col`; a row is kept iff the
    predicate evaluates to exactly `true` (`missing` excludes, SQL-style).
    Filters see source column names, before renames.
  * `limit`/`offset`: applied to qualifying rows, after the filter.
  * `validate`: error on select/filter references that match no column.
    `false` is the schema-evolution knob: unmatched SELECT references are
    silently dropped, and an unmatched FILTER reference evaluates as an
    all-missing column (`isnull(col(:gone))` keeps every row; comparisons
    against it exclude, SQL-style).
"""
struct Scan
    select::Vector{SelectItem}
    filter::Union{Nothing, ScanExpr}
    limit::Union{Nothing, Int}
    offset::Int
    validate::Bool
    function Scan(select::Vector{SelectItem},
                  filter, limit::Union{Nothing, Int},
                  offset::Int, validate::Bool)
        limit === nothing || limit >= 0 ||
            throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
        offset >= 0 || throw(ArgumentError("offset must be ≥ 0 (got $offset)"))
        nots = count(it -> it.ref isa Not, select)
        0 < nots < length(select) &&
            throw(ArgumentError("Not(...) selections cannot be mixed with positive selections"))
        return new(select, _checkpredicate(filter), limit, offset, validate)
    end
end

_selectitems(select::Vector{SelectItem}) = select
_selectitems(select::Union{Tuple, AbstractVector}) = SelectItem[_selectitem(x) for x in select]
_selectitems(select) = SelectItem[_selectitem(select)]

function Scan(; select=All(), filter=nothing, limit::Union{Nothing, Integer}=nothing,
                offset::Union{Nothing, Integer}=nothing, validate::Bool=true)
    items = _selectitems(select)
    lim = limit === nothing ? nothing : Int(limit)
    off = offset === nothing ? 0 : Int(offset)
    return Scan(items, filter, lim, off, validate)
end

"""
    Tables.Scan(scan::Scan; select=scan.select, filter=scan.filter,
                limit=scan.limit, offset=scan.offset, validate=scan.validate)

Copy a scan with some axes replaced — how a source builds the RESIDUAL it
hands to `Tables.scan` after pushing the rest down: a reader that handled
projection, types and limits itself but cannot filter does
`Tables.scan(table, Tables.Scan(scan; select=All(), limit=nothing, offset=0))`.
"""
function Scan(s::Scan; select=s.select, filter=s.filter,
              limit::Union{Nothing, Integer}=s.limit,
              offset::Union{Nothing, Integer}=s.offset, validate::Bool=s.validate)
    items = _selectitems(select)
    lim = limit === nothing ? nothing : Int(limit)
    off = offset === nothing ? 0 : Int(offset)
    return Scan(items, filter, lim, off, validate)
end

function _isallselection(select::Vector{SelectItem})
    length(select) == 1 || return false
    item = only(select)
    return item.ref isa All && item.type === nothing && item.rename === nothing
end

_isidentity(s::Scan) =
    _isallselection(s.select) && s.filter === nothing && s.limit === nothing && s.offset == 0


# --- schema resolution -----------------------------------------------------------

"""
    Tables.BoundColumn

One output column after `Tables.resolve`: the source column index, the output
name (post-rename, uniqueness enforced), and the optional type override.
"""
struct BoundColumn
    index::Int
    name::Symbol
    type::Union{Nothing, Type}
end

"""
    Tables.BoundScan

A `Scan` resolved against a concrete schema: output columns in order, the
filter with positional references normalized to source names, the source
column indices it references, and the row bounds. Sources can use this after
resolving a raw `Scan`.
"""
struct BoundScan
    columns::Vector{BoundColumn}
    filter::Union{Nothing, ScanExpr}
    filtercols::Vector{Int}
    limit::Union{Nothing, Int}
    offset::Int
    validate::Bool
end

_findcol(names, r::Symbol) = findfirst(==(r), names)
_findcol(names, r::String) = findfirst(==(Symbol(r)), names)
_findcol(names, r::Int) = 1 <= r <= length(names) ? r : nothing
function _findcols(names, r::Regex)
    return findall(nm -> occursin(r, String(nm)), names)
end
function _findcols(names, r)
    i = _findcol(names, r)
    return i === nothing ? Int[] : Int[i]
end
_refstr(r) = r isa Regex ? "r$(repr(r.pattern))" : repr(r)

function _expand!(out::Vector{BoundColumn}, names, it::SelectItem, validate::Bool)
    r = it.ref
    if r isa All
        append!(out, BoundColumn(i, nm, it.type) for (i, nm) in enumerate(names))
    else
        found = _findcols(names, r)
        if isempty(found)
            validate && throw(ArgumentError("select reference $(_refstr(r)) matches no column " *
                                            "(pass validate=false to skip unmatched references)"))
            return
        end
        r isa Regex && it.rename !== nothing && length(found) > 1 &&
            throw(ArgumentError("pattern rename $(repr(it.rename)) applies to multiple columns"))
        append!(out, BoundColumn(i, it.rename === nothing ? names[i] : it.rename, it.type)
                     for i in found)
    end
    return
end

_notrefs(x::Union{Tuple, AbstractVector}) = collect(Any, x)
_notrefs(x) = Any[x]

function _resolvefilter(e::ScanExpr, names, refs::Vector{Int}, validate::Bool)
    if e isa Col
        i = _findcol(names, e.ref)
        i === nothing && validate &&
            throw(ArgumentError("filter references unknown column $(_refstr(e.ref))"))
        i === nothing && return e
        i in refs || push!(refs, i)
        return Col(names[i])
    elseif e isa Cmp
        return Cmp(e.op, _resolvefilter(e.lhs, names, refs, validate), e.rhs)
    elseif e isa In
        return In(_resolvefilter(e.lhs, names, refs, validate), e.values)
    elseif e isa IsNull
        return IsNull(_resolvefilter(e.lhs, names, refs, validate), e.negated)
    elseif e isa StrPred
        return StrPred(e.kind, _resolvefilter(e.lhs, names, refs, validate), e.s)
    elseif e isa AndExpr || e isa OrExpr
        args = ScanExpr[_resolvefilter(a, names, refs, validate) for a in e.args]
        return e isa AndExpr ? AndExpr(args) : OrExpr(args)
    elseif e isa NotExpr
        return NotExpr(_resolvefilter(e.arg, names, refs, validate))
    elseif e isa OpNode
        throw(ArgumentError("cannot resolve extension node $(repr(e.name)); " *
                            "a source must consume it before calling Tables.resolve"))
    end
    return e
end

"""
    Tables.resolve(scan::Scan, names) -> BoundScan

Resolve a `Scan` against a source's column names (any iterable of `Symbol`s).
Errors on unmatched references (under `validate`), mixed `Not`/positive
selections, and duplicate output names. Regex references expand in file
order; selection order defines output order. Positional filter references are
replaced with source names so the resolved filter remains valid over a subset
containing only its referenced columns.
"""
function resolve(scan::Scan, names)
    nms = collect(Symbol, names)
    cols = BoundColumn[]
    if _isallselection(scan.select)
        append!(cols, BoundColumn(i, nm, nothing) for (i, nm) in enumerate(nms))
    elseif !isempty(scan.select) && scan.select[1].ref isa Not
        excluded = Set{Int}()
        for it in scan.select, r in _notrefs((it.ref::Not).ref)
            found = _findcols(nms, r)
            scan.validate && isempty(found) && throw(ArgumentError(
                "Not(...) $(r isa Regex ? "pattern" : "reference") $(_refstr(r)) matches no column",
            ))
            union!(excluded, found)
        end
        append!(cols, BoundColumn(i, nm, nothing) for (i, nm) in enumerate(nms) if !(i in excluded))
    else
        foreach(it -> _expand!(cols, nms, it, scan.validate), scan.select)
    end
    seen = Set{Symbol}()
    for c in cols
        c.name in seen && throw(ArgumentError("duplicate output column name $(repr(c.name)); " *
                                              "rename one of the selections"))
        push!(seen, c.name)
    end
    refs = Int[]
    filter = scan.filter === nothing ? nothing :
             _resolvefilter(scan.filter, nms, refs, scan.validate)
    return BoundScan(cols, filter, refs, scan.limit, scan.offset, scan.validate)
end

# --- generic evaluation ------------------------------------------------------------

_getcol(cols, ref::Symbol) = getcolumn(cols, ref)
_getcol(cols, ref::String) = getcolumn(cols, Symbol(ref))
_getcol(cols, ref::Int) = getcolumn(cols, ref)

# Under `strict`, an unknown filter reference is an error. Otherwise, it
# resolves to an all-missing column. `isnull` then keeps every row, comparisons
# exclude every row, and Boolean expressions use three-valued logic.
function _resolvecol(cols, ref, strict::Bool)
    i = _findcol(columnnames(cols), ref)
    i === nothing || return getcolumn(cols, i)
    strict && throw(ArgumentError("filter references unknown column $(_refstr(ref))"))
    return nothing
end

# Three-valued vectorized evaluation; the top level keeps rows where the
# result is exactly `true` (SQL WHERE). Comparison kernels close over their
# literal so collection-valued rows compare whole values per row instead of
# broadcasting the literal against the column. The fallback uses only the
# scalar-indexing contract required by Tables.jl; the AbstractVector path
# keeps broadcast's optimized kernels.
_columnmap(f, c::AbstractVector, ::Int) = f.(c)
_columnmap(f, c, n::Int) = [f(c[i]) for i in 1:n]

function _evalexpr(e::ScanExpr, cols, strict::Bool=true)
    if e isa Cmp
        a = _resolvecol(cols, e.lhs.ref, strict)
        a === nothing && return fill(missing, rowcount(cols))
        n = rowcount(cols)
        return e.op == OP_EQ ? _columnmap(x -> x == e.rhs, a, n) :
               e.op == OP_NE ? _columnmap(x -> x != e.rhs, a, n) :
               e.op == OP_LT ? _columnmap(x -> x < e.rhs, a, n) :
               e.op == OP_LE ? _columnmap(x -> x <= e.rhs, a, n) :
               e.op == OP_GT ? _columnmap(x -> x > e.rhs, a, n) :
               e.op == OP_GE ? _columnmap(x -> x >= e.rhs, a, n) :
               throw(ArgumentError("unknown comparison operator code $(e.op)"))
    elseif e isa In
        a = _resolvecol(cols, e.lhs.ref, strict)
        a === nothing && return fill(missing, rowcount(cols))
        return _columnmap(x -> ismissing(x) ? missing : in(x, e.values), a, rowcount(cols))
    elseif e isa IsNull
        a = _resolvecol(cols, e.lhs.ref, strict)
        a === nothing &&
            return e.negated ? falses(rowcount(cols)) : trues(rowcount(cols))
        return e.negated ? _columnmap(x -> !ismissing(x), a, rowcount(cols)) :
                           _columnmap(ismissing, a, rowcount(cols))
    elseif e isa StrPred
        a = _resolvecol(cols, e.lhs.ref, strict)
        a === nothing && return fill(missing, rowcount(cols))
        f = e.kind == STR_STARTSWITH ? startswith :
            e.kind == STR_ENDSWITH ? endswith :
            e.kind == STR_CONTAINS ? contains :
            throw(ArgumentError("unknown string predicate code $(e.kind)"))
        return _columnmap(x -> ismissing(x) ? missing : f(x, e.s), a, rowcount(cols))
    elseif e isa AndExpr
        isempty(e.args) && return trues(rowcount(cols))
        return mapreduce(a -> _evalexpr(a, cols, strict), (x, y) -> x .& y, e.args)
    elseif e isa OrExpr
        isempty(e.args) && return falses(rowcount(cols))
        return mapreduce(a -> _evalexpr(a, cols, strict), (x, y) -> x .| y, e.args)
    elseif e isa NotExpr
        return .!_evalexpr(e.arg, cols, strict)
    elseif e isa AlwaysTrue
        return trues(rowcount(cols))
    elseif e isa AlwaysFalse
        return falses(rowcount(cols))
    end
    throw(ArgumentError("cannot generically evaluate $(typeof(e))"))
end

# The Bool-mask comprehension runs behind a FUNCTION BARRIER: `_evalexpr`
# returns an abstractly-typed vector (its result type depends on the
# expression tree), and `Bool[x === true for x in v]` over an abstract `v`
# melts into per-element dynamic dispatch — measured 100× slower than the
# same loop behind a barrier (62 ms → 0.6 ms over a 1M-row mask). The
# barrier costs ONE dynamic dispatch per mask instead. The column kernels
# inside `_evalexpr` specialize on the concrete container at their own call
# boundary.
@noinline _boolmask(v::AbstractVector) = Bool[x === true for x in v]
"""
    Tables.filtermask(scan_or_expr, table) -> AbstractVector{Bool}

Evaluate a scan's filter over a table's columns: `mask[i]` is `true` iff row
`i` qualifies (a `missing` predicate result excludes the row). Sources use
this for their own pushdown implementations. The bare-expression form is
strict about unknown column references. The `Scan` form follows the scan's
`validate` setting. The `BoundScan` form uses its resolved filter and is safe
to evaluate over a table containing only `filtercols`.
"""
filtermask(e::ScanExpr, table) = _boolmask(_evalexpr(_checkpredicate(e), columns(table)))
filtermask(s::Scan, table) = s.filter === nothing ?
    trues(rowcount(columns(table))) :
    _boolmask(_evalexpr(s.filter, columns(table), s.validate))
filtermask(s::BoundScan, table) = s.filter === nothing ?
    trues(rowcount(columns(table))) :
    _boolmask(_evalexpr(s.filter, columns(table), s.validate))

_converted(::Nothing, c::AbstractVector) = c
function _converted(::Type{T}, c::AbstractVector) where {T}
    # eltype Union{} (necessarily empty) is vacuously <: everything, but the
    # requested output schema still owes the override type
    if eltype(c) !== Union{} && eltype(c) <: Union{T, Missing}
        return c
    end
    anymissing = any(ismissing, c)
    E = anymissing ? Union{T, Missing} : T
    out = allocatecolumn(E, length(c))
    @inbounds for i in eachindex(c)
        x = c[i]
        out[i] = ismissing(x) ? missing : convert(T, x)
    end
    return out
end

_takecolumn(c::AbstractVector, idx) = c[idx]
_takecolumn(c, idx) = [c[i] for i in idx]

struct _ScanTable{T <: NamedTuple} <: AbstractColumns
    columns::T
    nrows::Int
end
columnnames(t::_ScanTable) = propertynames(getfield(t, :columns))
getcolumn(t::_ScanTable, i::Int) = getfield(getfield(t, :columns), i)
getcolumn(t::_ScanTable, name::Symbol) = getproperty(getfield(t, :columns), name)
rowcount(t::_ScanTable) = getfield(t, :nrows)

function _zerocolumnpredicate(e)
    e === nothing && return true
    e isa AlwaysTrue && return true
    e isa AlwaysFalse && return false
    e isa IsNull && return !e.negated
    if e isa AndExpr
        sawmissing = false
        for arg in e.args
            result = _zerocolumnpredicate(arg)
            result === false && return false
            result === missing && (sawmissing = true)
        end
        return sawmissing ? missing : true
    elseif e isa OrExpr
        sawmissing = false
        for arg in e.args
            result = _zerocolumnpredicate(arg)
            result === true && return true
            result === missing && (sawmissing = true)
        end
        return sawmissing ? missing : false
    elseif e isa NotExpr
        result = _zerocolumnpredicate(e.arg)
        return result === missing ? missing : !result
    elseif e isa OpNode
        throw(ArgumentError("cannot generically evaluate extension node $(repr(e.name))"))
    end
    return missing
end

function _windowcount(n::Int, filterresult, offset::Int, limit::Union{Nothing, Int})
    filterresult === true || return 0
    skipped = min(offset, n)
    available = n - skipped
    return limit === nothing ? available : min(limit, available)
end

"""
    Tables.scan(table, scan::Scan) -> table′

Apply a `Scan` generically over any Tables.jl table: filter, then offset/limit,
then projection, renames, and type overrides. A filter keeps only exact `true`;
`missing` excludes the row. The input table is returned unchanged for an
identity request. Other results are column tables. A zero-column result keeps
its row count.

This is the reference behavior every pushdown must preserve. A source can hand
its unconsumed residual request to this function.
"""
function scan(table, scan::Scan)
    _isidentity(scan) && return table
    cols = columns(table)
    b = resolve(scan, columnnames(cols))
    nrows = rowcount(cols)
    if isempty(columnnames(cols))
        taken = _windowcount(nrows, _zerocolumnpredicate(b.filter), b.offset, b.limit)
        return _ScanTable(NamedTuple(), taken)
    elseif b.filter !== nothing
        keep = filtermask(b, cols)
        idx = findall(keep)
    else
        idx = 1:nrows
    end
    skipped = min(b.offset, length(idx))
    available = length(idx) - skipped
    taken = b.limit === nothing ? available : min(b.limit, available)
    idx = idx[(skipped + 1):(skipped + taken)]
    outnames = Tuple(c.name for c in b.columns)
    isempty(outnames) && return _ScanTable(NamedTuple(), taken)
    outcols = Tuple(_converted(c.type, _takecolumn(getcolumn(cols, c.index), idx)) for c in b.columns)
    return NamedTuple{outnames}(outcols)
end

# --- display -----------------------------------------------------------------------

function _exprstr(e::ScanExpr)
    e isa Cmp && return "colcmp($(_OPNAMES[Int(e.op)]), col($(repr(e.lhs.ref))), $(repr(e.rhs)))"
    e isa In && return "colin(col($(repr(e.lhs.ref))), $(repr(e.values)))"
    e isa IsNull && return (e.negated ? "!isnull(" : "isnull(") * "col($(repr(e.lhs.ref))))"
    e isa StrPred && return (e.kind == STR_STARTSWITH ? "startswith" :
                             e.kind == STR_ENDSWITH ? "endswith" : "contains") *
                            "(col($(repr(e.lhs.ref))), $(repr(e.s)))"
    e isa AndExpr && return isempty(e.args) ? "true" :
        join(("(" * _exprstr(a) * ")" for a in e.args), " & ")
    e isa OrExpr && return isempty(e.args) ? "false" :
        join(("(" * _exprstr(a) * ")" for a in e.args), " | ")
    e isa NotExpr && return "!(" * _exprstr(e.arg) * ")"
    e isa AlwaysTrue && return "true"
    e isa AlwaysFalse && return "false"
    e isa OpNode && return "OpNode($(repr(e.name)), …)"
    return string(e)
end

function Base.show(io::IO, s::Scan)
    print(io, "Tables.Scan(")
    parts = String[]
    if !_isallselection(s.select)
        sel = join((begin
            r = it.ref isa Not ? "Not($(_refstr(it.ref.ref)))" :
                it.ref isa All ? "All()" : _refstr(it.ref)
            it.type !== nothing && (r *= " => $(it.type)")
            it.rename !== nothing && (r *= " => $(repr(it.rename))")
            r
        end for it in s.select), ", ")
        push!(parts, "select = ($sel)")
    end
    s.filter === nothing || push!(parts, "filter = " * _exprstr(s.filter))
    s.limit === nothing || push!(parts, "limit = $(s.limit)")
    s.offset == 0 || push!(parts, "offset = $(s.offset)")
    s.validate || push!(parts, "validate = false")
    print(io, join(parts, ", "), ")")
end

"""
    Tables.describe([io,] scan, residual)

Print a scan next to the residual a source handed to `Tables.scan` — the
EXPLAIN affordance for pushdown debugging.
"""
function describe(io::IO, scan::Scan, residual::Scan)
    println(io, "scan:     ", scan)
    println(io, "residual: ", _isidentity(residual) ? "(empty — fully pushed down)" : residual)
end
describe(scan::Scan, residual::Scan) = describe(stdout, scan, residual)
