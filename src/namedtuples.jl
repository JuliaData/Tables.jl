# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

rows(x::RowTable) = x
schema(x::RowTable{T}) where {T} = T

rowtable(itr) = collect(rows(itr))

# NamedTuple of Vectors
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

function schema(::NamedTuple{names, T}) where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
    if @generated
        types = Tuple{(eltype(x) for x in T.parameters)...}
        return :(NamedTuple{$names, $types})
    else
        return NamedTuple{names, Tuple{(eltype(x) for x in T.parameters)...}}
    end
end

AccessStyle(::Type{<:ColumnTable}) = ColumnAccess()
columns(x::ColumnTable) = x

columntable(rows) = columns(rows)
