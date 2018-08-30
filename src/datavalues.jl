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
datavaluetype(::Type{T}) where {T} = DataValue{T}
datavaluetype(::Type{Union{T, Missing}}) where {T} = DataValue{T}
Base.@pure function datavaluetype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ datavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

struct DataValueRowIterator{NT, S}
    x::S
end

# Should maybe make this return a custom DataValueRow type to allow lazier
# DataValue wrapping; but need to make sure Query/QueryOperators support first
Base.eltype(rows::DataValueRowIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{DataValueRowIterator{NT, S}}) where {NT, S} = Base.IteratorSize(S)
Base.length(rows::DataValueRowIterator) = length(rows.x)

"Returns a DataValue-based NamedTuple-iterator"
DataValueRowIterator(::Type{NT}, x::S) where {NT <: NamedTuple, S} = DataValueRowIterator{datavaluetype(NT), S}(x)

function Base.iterate(rows::DataValueRowIterator{NT, S}, st=()) where {NT <: NamedTuple{names}, S} where {names}
    if @generated
        vals = Tuple(:(getproperty(row, $(fieldtype(NT, i)), $i, $(Meta.QuoteNode(names[i])))) for i = 1:fieldcount(NT))
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
        return NT(Tuple(getproperty(row, fieldtype(NT, i), i, names[i]) for i = 1:fieldcount(NT))), (st,)
    end
end

datavaluerows(x) = DataValueRowIterator(schema(x), rows(x))
