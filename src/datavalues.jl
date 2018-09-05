using .DataValues

# DataValue-compatible row iteration for Data.Sources
nondatavaluetype(::Type{DataValue{T}}) where {T} = Union{T, Missing}
nondatavaluetype(::Type{T}) where {T} = T
Base.@pure function nondatavaluetype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ nondatavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

unwrap(x) = x
unwrap(x::DataValue) = isna(x) ? missing : DataValues.unsafe_get(x)

datavaluetype(::Type{T}) where {T <: DataValue} = T
datavaluetype(::Type{T}) where {T} = T
datavaluetype(::Type{Union{T, Missing}}) where {T} = DataValue{T}
Base.@pure function datavaluetype(::Tables.Schema{names, types}) where {names, types}
    TT = Tuple{Any[ datavaluetype(fieldtype(types, i)) for i = 1:fieldcount(types) ]...}
    return NamedTuple{names, TT}
end

struct DataValueRowIterator{NT, S}
    x::S
end
DataValueRowIterator(::Type{NT}, x::S) where {NT <: NamedTuple, S} = DataValueRowIterator{NT, S}(x)

"Returns a DataValue-based NamedTuple-iterator"
DataValueRowIterator(::Type{Schema{names, types}}, x::S) where {names, types, S} = DataValueRowIterator{datavaluetype(NamedTuple{names, types}), S}(x)
function datavaluerows(x)
    r = Tables.rows(x)
    #TODO: add support for unknown schema
    return DataValueRowIterator(datavaluetype(Tables.schema(r)), r)
end

Base.eltype(rows::DataValueRowIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{DataValueRowIterator{NT, S}}) where {NT, S} = Base.IteratorSize(S)
Base.length(rows::DataValueRowIterator) = length(rows.x)

function Base.iterate(rows::DataValueRowIterator{NT}, st=()) where {NT}
    state = iterate(rows.x, st...)
    state === nothing && return nothing
    row, st = state
    return DataValueRow{NT, typeof(row)}(row), (st,)
end

struct DataValueRow{NT, T}
    row::T
end

@inline Base.getproperty(dvr::DataValueRow{NamedTuple{names, types}}, nm::Symbol) where {names, types} = getproperty(dvr, Tables.columntype(names, types, nm), Tables.columnindex(names, nm), nm)
@inline Base.getproperty(dvr::DataValueRow, ::Type{T}, col::Int, nm::Symbol) where {T} = T(getproperty(getfield(dvr, 1), nondatavaluetype(T), col, nm))
