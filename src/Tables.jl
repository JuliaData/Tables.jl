module Tables

using IteratorInterfaceExtensions

export rowtable, columntable

# helper functions
names(::Type{NamedTuple{nms, typs}}) where {nms, typs} = nms
types(::Type{NamedTuple{nms, typs}}) where {nms, typs} = Tuple(typs.parameters)

"Abstract row type with a simple required interface: row values are accessible via `getproperty(row, field)`; for example, a NamedTuple like `nt = (a=1, b=2, c=3)` can access it's value for `a` like `nt.a` which turns into a call to the function `getproperty(nt, :a)`"
abstract type Row end

"""
    The Tables.jl package provides three useful interface functions for working with tabular data in a variety of formats.

    `Tables.schema(table) => NamedTuple{names, types}`
    `Tables.rows(table) => Row-iterator`
    `Tables.columns(table) => NamedTuple of AbstractVectors`

Essentially, for any table type that implements the interface requirements, one can get access to:
    1) the schema of a table, returned as a NamedTuple _type_, with parameters of `names` which are the column names, and `types` which is a `Tuple{...}` type of types
    2) rows of the table via a Row-iterator
    3) the columns of the table via a NamedTuple of AbstractVectors, where the NamedTuple keys are the column names.

So how does one go about satisfying the Tables.Table interface? We must ensure the three methods get satisfied for a given table type.

    `Tables.schema`: given an _instance_ of a table type, generate a `NamedTuple` type, with a tuple of symbols for column names (e.g. `(:a, :b, :c)`), and a tuple type of types as the 2nd parameter (e.g. `Tuple{Int, Float64, String}`); like `NamedTuple{(:a, :b, :c), Tuple{Int, Float64, String}}`

    `Tables.rows`:
        - overload `Tables.rows` directly for your table type (e.g. `Tables.rows(t::MyTableType)`), and return an iterator of `Row`s. Where a `Row` type is any object with keys accessible via `getproperty(obj, key)`
        - define `Tables.producescells(::Type{<:MyTableType}) = true`, as well as `Tables.getcell(t::MyTableType, ::Type{T}, row::Int, col::Int)` and `Tables.isdonefunction(::Type{<:MyTableType})::Function`; a generic `Tables.rows(x)` implementation will use these definitions to generate a valid `Row` iterator for `MyTableType`

    `Tables.columns`:
        - overload `Tables.columns` directly for your table type (e.g. `Tables.columns(t::MyTableType)`), returning a NamedTuple of AbstractVectors, with keys being the column names
        - define `Tables.producescolumns(::Type{<:MyTableType}) = true`, as well as `Tables.getcolumn(t::MyTableType, ::Type{T}, col::Int)`; a generic `Tables.columns(x)` implementation will use these definitions to generate a valid NamedTuple of AbstractVectors for `MyTableType`
        - define `Tables.producescells(::Type{<:MyTableType}) = true`, as well as `Tables.getcell(t::MyTableType, ::Type{T}, row::Int, col::Int)` and `Tables.isdonefunction(::Type{<:MyTableType})::Function`; again, the generic `Tables.columns(x)` implementation can use these definitions to generate a valid NamedTuple of AbstractVectors for `MyTableType`

The final question is how `MyTableType` can be a "sink" for any other table type:

    - Define a function or constructor that takes, at a minimum, a single, untyped argument and then calls `Tables.rows` or `Tables.columns` on that argument to construct an instance of `MyTableType`

For example, if `MyTableType` is a row-oriented format, I might define my "sink" function like:
```julia
function MyTableType(x)
    mytbl = MyTableType(Tables.schema(x))
    for row in Tables.rows(x)
        append!(mytbl, row)
    end
    return mytbl
end
```
Alternatively, if `MyTableType` is column-oriented, perhaps my definition would be more like:
```
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

## generic fallbacks; if a table provides Tables.rows or Tables.columns,
## provide the inverse interface function

# Row iteration
struct RowIterator{NT, S}
    source::S
end

Base.eltype(rows::RowIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{<:RowIterator}) = Base.SizeUnknown()

function Base.iterate(rows::RowIterator{NamedTuple{names, types}, S}, states=ntuple(x->(), length(names))) where {names, types, S}
    if @generated
        itrblock = Expr(:block)
        for (i, key) in enumerate(names)
            push!(itrblock.args, quote
                $(Symbol("x_$key")) = getproperty(rows.source, $(Meta.QuoteNode(key)))
                $(Symbol("val_$key")) = iterate($(Symbol("x_$key")), states[$i]...)
                $(Symbol("val_$key")) === nothing && return nothing
                $(Symbol("value_$key")), $(Symbol("state_$key")) = $(Symbol("val_$key"))
            end)
        end
        vals = Tuple(Symbol("value_$key") for key in names)
        states = Tuple(Symbol("state_$key") for key in names)
        q = quote
            $itrblock
            return ($(NamedTuple{names, types}))(($(vals...),)), ($(states...),)
        end
        # @show q
        return q
    else
        values = []
        states = []
        for (i, key) in enumerate(names)
            x = getproperty(rows.source, key)
            val = iterate(x, states[i]...)
            val === nothing && return nothing
            push!(values, val[1])
            push!(states, val[2])
        end
        return NamedTuple{names, types}(Tuple(values)), Tuple(states)
    end
end

function rows(x::T) where {T}
    if AccessStyle(T) === ColumnAccess()
        return RowIterator{schema(x), T}(x)
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
    return buildcolumns(sch, x)
end

include("namedtuples.jl")

end # module
