Base.@pure function nondatavaluenamedtuple(::Type{NT}) where {names, NT <: NamedTuple{names}}
    TT = Tuple{Any[ DataValueInterfaces.nondatavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

Base.@pure function datavaluenamedtuple(::Tables.Schema{names, types}) where {names, types}
    TT = Tuple{Any[ DataValueInterfaces.datavaluetype(fieldtype(types, i)) for i = 1:fieldcount(types) ]...}
    return NamedTuple{names, TT}
end

# IteratorWrapper takes an input Row iterators, it will unwrap any DataValue elements as plain Union{T, Missing}
struct IteratorWrapper{S}
    x::S
end

"""
    Tables.nondatavaluerows(x)

Takes any Queryverse-compatible `NamedTuple` iterator source and 
converts to a Tables.jl-compatible `AbstractRow` iterator. Will automatically
unwrap any `DataValue`s, replacing `NA` with `missing`.
Useful for translating Query.jl results back to non-`DataValue`-based tables.
"""
nondatavaluerows(x) = IteratorWrapper(IteratorInterfaceExtensions.getiterator(x))
Tables.istable(::Type{<:IteratorWrapper}) = true
Tables.rowaccess(::Type{<:IteratorWrapper}) = true
Tables.rows(x::IteratorWrapper) = x

function Tables.schema(dv::IteratorWrapper{S}) where {S}
    eT = eltype(dv.x)
    (!(eT <: NamedTuple) || eT === Union{}) && return schema(dv.x)
    return Tables.Schema(nondatavaluenamedtuple(eT))
end

Base.IteratorEltype(::Type{I}) where {S, I<:IteratorWrapper{S}} = Base.IteratorEltype(S)
Base.eltype(::Type{I}) where {S, I<:IteratorWrapper{S}} = IteratorRow{eltype(S)}
Base.IteratorSize(::Type{I}) where {S, I<:IteratorWrapper{S}} = Base.IteratorSize(S)
Base.length(rows::IteratorWrapper) = length(rows.x)
Base.size(rows::IteratorWrapper) = size(rows.x)

@noinline invalidtable(::T, ::S) where {T, S} = throw(ArgumentError("'$T' iterates '$S' values, which doesn't satisfy the Tables.jl `AbstractRow` interface"))

@inline function Base.iterate(rows::IteratorWrapper)
    x = iterate(rows.x)
    x === nothing && return nothing
    row, st = x
    columnnames(row) === () && invalidtable(rows.x, row)
    return IteratorRow(row), st
end

@inline function Base.iterate(rows::IteratorWrapper, st)
    x = iterate(rows.x, st)
    x === nothing && return nothing
    row, st = x
    return IteratorRow(row), st
end

struct IteratorRow{T} <: AbstractRow
    row::T
end

getrow(r::IteratorRow) = getfield(r, :row)
wrappedtype(::Type{I}) where {T, I<:IteratorRow{T}} = T
wrappedtype(::Type{T}) where {T} = T

unwrap(::Type{T}, x) where {T} = convert(T, x)
unwrap(::Type{Any}, x) = x.hasvalue ? x.value : missing

nondv(T) = DataValueInterfaces.nondatavaluetype(T)
undatavalue(x::T) where {T} = T == nondv(T) ? x : unwrap(nondv(T), x)

@inline getcolumn(r::IteratorRow, ::Type{T}, col::Int, nm::Symbol) where {T} = undatavalue(getcolumn(getrow(r), T, col, nm))
@inline getcolumn(r::IteratorRow, nm::Symbol) = undatavalue(getcolumn(getrow(r), nm))
@inline getcolumn(r::IteratorRow, i::Int) = undatavalue(getcolumn(getrow(r), i))
columnnames(r::IteratorRow) = columnnames(getrow(r))

# DataValueRowIterator wraps a Row iterator and will wrap `Union{T, Missing}` typed fields in DataValues
struct DataValueRowIterator{NT, sch, S}
    x::S
end

"""
    Tables.datavaluerows(x) => NamedTuple iterator

Takes any table input `x` and returns a `NamedTuple` iterator
that will replace missing values with `DataValue`-wrapped values;
this allows any table type to satisfy the TableTraits.jl 
Queryverse integration interface by defining: 

```
IteratorInterfaceExtensions.getiterator(x::MyTable) = Tables.datavaluerows(x)
```
"""
function datavaluerows(x)
    r = Tables.rows(x)
    s = Tables.schema(r)
    s === nothing && error("Schemaless sources cannot be passed to datavaluerows.")
    return DataValueRowIterator{datavaluenamedtuple(s), typeof(s), typeof(r)}(r)
end

Base.eltype(::Type{D}) where {NT, D<:DataValueRowIterator{NT}} = NT
Base.IteratorSize(::Type{D}) where {NT, sch, S, D<:DataValueRowIterator{NT, sch, S}} = Base.IteratorSize(S)
Base.length(rows::DataValueRowIterator) = length(rows.x)
Base.size(rows::DataValueRowIterator) = size(rows.x)

function Base.iterate(rows::DataValueRowIterator{NamedTuple{names, dtypes}, Schema{names, rtypes}, S}, st=()) where {names, dtypes, rtypes, S}
    # use of @generated justified here because Queryverse has stated only support for "reasonable amount of columns"
    if @generated
        vals = Any[ :(convert($(fieldtype(dtypes, i)), getcolumn(row, $(fieldtype(rtypes, i)), $i, $(Meta.QuoteNode(names[i]))))) for i = 1:length(names) ]
        ret = Expr(:new, :(NamedTuple{names, dtypes}), vals...)
        q = quote
            x = iterate(rows.x, st...)
            x === nothing && return nothing
            row, st = x
            return $ret, (st,)
        end
        # @show q
        return q
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{names, dtypes}(Tuple(convert(fieldtype(dtypes, i), getcolumn(row, fieldtype(rtypes, i), i, names[i])) for i = 1:length(names))), (st,)
    end
end
