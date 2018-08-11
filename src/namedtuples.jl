# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

rows(x::RowTable) = x
schema(x::RowTable{T}) where {T} = T

producescells(::Type{<:RowTable}) = true
getcell(source::RowTable, ::Type{T}, row, col) where {T} = source[row][col]
isdonefunction(::Type{<:RowTable}) = (x, row) -> row > length(x)

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

producescells(::Type{<:ColumnTable}) = true
getcell(source::ColumnTable, ::Type{T}, row, col) where {T} = source[col][row]
isdonefunction(::Type{<:ColumnTable}) = (x, row) -> row > (length(x) > 0 ? length(x[1]) : 0)

producescolumns(::Type{<:ColumnTable}) = true
getcolumn(source::ColumnTable, ::Type{T}, col) where {T} = source[col]

columntable(rows) = Tables.columns(rows)
