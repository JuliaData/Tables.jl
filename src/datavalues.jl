using .DataValues

# DataValue overloads for query.jl definitions
nondatavaluetype(::Type{DataValue{T}}) where {T} = Union{T, Missing}
unwrap(x::DataValue) = isna(x) ? missing : DataValues.unsafe_get(x)
datavaluetype(::Type{T}) where {T <: DataValue} = T
datavaluetype(::Type{Union{T, Missing}}) where {T} = DataValue{T}

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

function Base.iterate(rows::DataValueRowIterator{NT, S}, st=()) where {NT <: NamedTuple{names}, S} where {names}
    if @generated
        vals = Tuple(:($(fieldtype(NT, i))(getproperty(row, $(nondatavaluetype(fieldtype(NT, i))), $i, $(Meta.QuoteNode(names[i]))))) for i = 1:fieldcount(NT))
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
        return NT(Tuple(fieldtype(NT, i)(getproperty(row, nondatavaluetype(fieldtype(NT, i)), i, names[i])) for i = 1:fieldcount(NT))), (st,)
    end
end

# Alternative lazy row implementation; currently though, Query.jl relies on
# being able to infer return types via all the type information of NamedTuples

# function Base.iterate(rows::DataValueRowIterator{NT}, st=()) where {NT}
#     state = iterate(rows.x, st...)
#     state === nothing && return nothing
#     row, st = state
#     return DataValueRow{NT, typeof(row)}(row), (st,)
# end

# struct DataValueRow{NT, T}
#     row::T
# end

# @inline Base.getproperty(dvr::DataValueRow{NamedTuple{names, types}}, nm::Symbol) where {names, types} = getproperty(dvr, Tables.columntype(names, types, nm), Tables.columnindex(names, nm), nm)
# @inline Base.getproperty(dvr::DataValueRow, ::Type{T}, col::Int, nm::Symbol) where {T} = T(getproperty(getfield(dvr, 1), nondatavaluetype(T), col, nm))
