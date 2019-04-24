# utilities for converting Row iterators to and from DataValue-based NamedTuple iterators
# the DataValue-specific definitions are in the Tables.jl __init__ DataValues @require block
nondatavaluetype(::Type{T}) where {T} = T
nondatavaluetype(::Type{Union{}}) = Union{}
datavaluetype(::Type{T}) where {T} = T
datavaluetype(::Type{Union{}}) = Union{}

Base.@pure function nondatavaluetype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ nondatavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

Base.@pure function datavaluetype(::Tables.Schema{names, types}) where {names, types}
    TT = Tuple{Any[ datavaluetype(fieldtype(types, i)) for i = 1:fieldcount(types) ]...}
    return NamedTuple{names, TT}
end

unwrap(x) = x
scalarconvert(T, x) = convert(T, x)
scalarconvert(::Type{T}, x::T) where {T} = x

# IteratorWrapper takes an input Row iterators, it will unwrap any DataValue elements as plain Union{T, Missing}
struct IteratorWrapper{S}
    x::S
end

Tables.istable(::Type{<:IteratorWrapper}) = true
Tables.rowaccess(::Type{<:IteratorWrapper}) = true
Tables.rows(x::IteratorWrapper) = x

function Tables.schema(dv::IteratorWrapper)
    eT = eltype(dv.x)
    (!(eT <: NamedTuple) || eT === Union{}) && return nothing
    return Tables.Schema(nondatavaluetype(eT))
end
Base.eltype(rows::IteratorWrapper) = IteratorRow{eltype(rows.x)}
Base.IteratorSize(::Type{IteratorWrapper{S}}) where {S} = Base.IteratorSize(S)
Base.length(rows::IteratorWrapper) = length(rows.x)

@noinline invalidtable(::T, ::S) where {T, S} = throw(ArgumentError("'$T' iterates '$S' values, which don't satisfy the Tables.jl Row-iterator interface"))

function Base.iterate(rows::IteratorWrapper)
    x = iterate(rows.x)
    x === nothing && return nothing
    row, st = x
    propertynames(row) === () && invalidtable(rows.x, row)
    return IteratorRow(row), st
end

function Base.iterate(rows::IteratorWrapper, st)
    x = iterate(rows.x, st)
    x === nothing && return nothing
    row, st = x
    return IteratorRow(row), st
end

struct IteratorRow{T}
    row::T
end

Base.getproperty(d::IteratorRow, ::Type{T}, col::Int, nm::Symbol) where {T} = unwrap(getproperty(getfield(d, 1), T, col, nm))
Base.getproperty(d::IteratorRow, nm::Symbol) = unwrap(getproperty(getfield(d, 1), nm))
Base.propertynames(d::IteratorRow) = propertynames(getfield(d, 1))

# DataValueRowIterator wraps a Row iterator and will wrap `Union{T, Missing}` typed fields in DataValues
struct DataValueRowIterator{NT, S}
    x::S
end
DataValueRowIterator(::Type{NT}, x::S) where {NT <: NamedTuple, S} = DataValueRowIterator{NT, S}(x)

"Returns a DataValue-based NamedTuple-iterator"
DataValueRowIterator(::Type{Schema{names, types}}, x::S) where {names, types, S} = DataValueRowIterator{datavaluetype(NamedTuple{names, types}), S}(x)

function datavaluerows(x)
    r = Tables.rows(x)
    s = Tables.schema(r)
    s === nothing && error("Schemaless sources cannot be passed to datavaluerows.")
    return DataValueRowIterator(datavaluetype(s), r)
end

_iteratorsize(x) = x
_iteratorsize(::Base.HasShape{1}) = Base.HasLength()

Base.eltype(rows::DataValueRowIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{DataValueRowIterator{NT, S}}) where {NT, S} = _iteratorsize(Base.IteratorSize(S))
Base.length(rows::DataValueRowIterator) = length(rows.x)

function Base.iterate(rows::DataValueRowIterator{NT, S}, st=()) where {NT <: NamedTuple{names}, S} where {names}
    if @generated
        vals = Tuple(:(scalarconvert($(fieldtype(NT, i)), getproperty(row, $(nondatavaluetype(fieldtype(NT, i))), $i, $(Meta.QuoteNode(names[i]))))) for i = 1:fieldcount(NT))
        q = quote
            x = iterate(rows.x, st...)
            x === nothing && return nothing
            row, st = x
            return $NT(($(vals...),)), (st,)
        end
        # @show q
        return q
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        return NT(Tuple(scalarconvert(fieldtype(NT, i), getproperty(row, nondatavaluetype(fieldtype(NT, i)), i, names[i])) for i = 1:fieldcount(NT))), (st,)
    end
end
