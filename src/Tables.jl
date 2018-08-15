module Tables

using Requires

export rowtable, columntable

function __init__()
    @require DataValues="e7dc6d0d-1eca-5fa6-8ad6-5aecde8b7ea5" include("datavalues.jl")
    @require QueryOperators="2aef5ad7-51ca-5a8f-8e88-e75cf067b44b" include("enumerable.jl")
end

# helper functions
names(::Type{NamedTuple{nms, typs}}) where {nms, typs} = nms
types(::Type{NamedTuple{nms, typs}}) where {nms, typs} = Tuple(typs.parameters)

"Abstract row type with a simple required interface: row values are accessible via `getproperty(row, field)`; for example, a NamedTuple like `nt = (a=1, b=2, c=3)` can access it's value for `a` like `nt.a` which turns into a call to the function `getproperty(nt, :a)`"
abstract type Row end

"""
The Tables.jl package provides four useful interface functions for working with tabular data in a variety of formats.

```julia
    Tables.schema(table) => NamedTuple{names, types}
    Tables.AccessStyle(table_type) => Tables.RowAccess() | Tables.ColumnAccess()
    Tables.rows(table) => iterator with values accessible vai getproperty(row, columnname)
    Tables.columns(table) => Collection of iterators, with each column accessible via getproperty(x, columnname)
```

Essentially, for any table type that implements the interface requirements, one can get access to:
    1. the schema of a table, returned as a NamedTuple _type_, with parameters of `names` which are the column names, and `types` which is a `Tuple{...}` type of types
    2. rows of the table via a Row-iterator
    3. the columns of the table via a collection of iterators, where the individual column iterators are accessible via getproperty(table, columnname)

So how does one go about satisfying these interface functions? It mainly depends on the `Tables.AccessStyle(T)` of your table:

* `Tables.schema(x)`: given an _instance_ of a table type, generate a `NamedTuple` type, with a tuple of symbols for column names (e.g. `(:a, :b, :c)`), and a tuple type of types as the 2nd parameter (e.g. `Tuple{Int, Float64, String}`); like `NamedTuple{(:a, :b, :c), Tuple{Int, Float64, String}}`

* `Tables.RowAccess()`:
  * overload `Tables.rows` for your table type (e.g. `Tables.rows(t::MyTableType)`), and return an iterator of `Row`s. Where a `Row` type is any object with keys accessible via `getproperty(obj, key)` (like a NamedTuple type)

* `Tables.ColumnAccess()`:
  * overload `Tables.columns` for your table type (e.g. `Tables.columns(t::MyTableType)`), returning a collection of iterators, with individual column iterators accessible via column names by `getproperty(x, columnname)` (a NamedTuple of Vectors, for example, would satisfy the interface)

The final question is how `MyTableType` can be a "sink" for any other table type:

* Define a function or constructor that takes, at a minimum, a single, untyped argument and then calls `Tables.rows` or `Tables.columns` on that argument to construct an instance of `MyTableType`

For example, if `MyTableType` is a row-oriented format, I might define my "sink" function like:
```julia
function MyTableType(x)
    mytbl = MyTableType(Tables.schema(x)) # custom constructor that creates an "empty" MyTableType w/ the right schema
    for row in Tables.rows(x)
        append!(mytbl, row)
    end
    return mytbl
end
```

Alternatively, if `MyTableType` is column-oriented, perhaps my definition would be more like:

```julia
function MyTableType(x)
    cols = Tables.columns(x)
    return MyTableType(collect(map(String, keys(cols))), [col for col in cols])
end
```

Obviously every table type is different, but via a combination of `Tables.schema`, `Tables.rows`, and `Tables.columns`, each table type should be able to construct an instance of itself.
"""
abstract type Table end

abstract type AccessStyle end
struct RowAccess <: AccessStyle end
struct ColumnAccess <: AccessStyle end
AccessStyle(x) = RowAccess()

"Tables.schema(s) => NamedTuple{names, types}"
function schema end
schema(x) = eltype(x)

include("namedtuples.jl")

## generic fallbacks; if a table provides Tables.rows or Tables.columns,
## provide the inverse interface function

# generic row iteration of columns
struct RowIterator{NT, S}
    source::S
end

RowIterator(c::T) where {T <: ColumnTable} = RowIterator{schema(c), T}(c)

Base.eltype(rows::RowIterator{NT, S}) where {NT, S} = NT
Base.length(x::RowIterator) = ntlength(x.source)

function Base.iterate(rows::RowIterator{NamedTuple{names, types}, S}, st=1) where {names, types, S}
    if @generated
        vals = Tuple(:(rows.source[$(Meta.QuoteNode(nm))][st]) for nm in names)
        q = quote
            st > length(rows) && return nothing
            return NamedTuple{names, types}(($(vals...),)), st + 1
        end
        # @show q
        return q
    else
        st > length(rows) && return nothing
        return NamedTuple{names, types}(Tuple(rows.source[nm][st] for nm in names)), st + 1
    end
end

function rows(x::T) where {T}
    if AccessStyle(T) === ColumnAccess()
        return RowIterator(columntable(Tables.columns(x)))
    else
        return x # assume x implicitly implements row interface
    end
end

function buildcolumns(::Type{NamedTuple{names, types}}, rows) where {names, types}
    if @generated
        vals = Tuple(:(Vector{$typ}(undef, 0)) for typ in types.parameters)
        innerloop = Expr(:block)
        for nm in names
            push!(innerloop.args, :(push!(nt[$(Meta.QuoteNode(nm))], getproperty(row, $(Meta.QuoteNode(nm))))))
        end
        q = quote
            nt = NamedTuple{names}(($(vals...),))
            for row in rows
                $innerloop
            end
            return nt
        end
        # @show q
        return q
    else
        nt = NamedTuple{names}(Tuple(Vector{typ}(undef, 0) for typ in T.parameters))
        for row in rows
            for key in keys(nt)
                push!(nt[key], getproperty(row, key))
            end
        end
        return nt
    end
end

function columns(x::T) where {T}
    @assert AccessStyle(T) === RowAccess()
    sch = Tables.schema(x)
    return buildcolumns(sch, Tables.rows(x))
end

end # module
