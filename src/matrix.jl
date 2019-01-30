istable(::Type{<:Matrix}) = false

rows(m::T) where {T <: Matrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.astable` for ways to treat a Matrix as a table"))
columns(m::T) where {T <: Matrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.astable` for ways to treat a Matrix as a table"))

struct MatrixTable{T}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::Union{Matrix{T}, Adjoint{T, Matrix{T}}}
end

istable(::Type{<:MatrixTable}) = true
names(m::MatrixTable) = getfield(m, 1)

# row interface
struct MatrixRow{T}
    row::Int
    source::MatrixTable{T}
end

Base.getproperty(m::MatrixRow, ::Type{T}, col::Int, nm::Symbol) where {T} =
    getfield(getfield(m, 2), 3)[getfield(m, 1), col]
Base.getproperty(m::MatrixRow, nm::Symbol) =
    getfield(getfield(m, 2), 3)[getfield(m, 1), getfield(getfield(m, 2), 2)[nm]]
Base.propertynames(m::MatrixRow) = names(getfield(m, 2))

rowaccess(::Type{<:MatrixTable}) = true
schema(m::MatrixTable{T}) where {T} = Schema(Tuple(names(m)), NTuple{size(getfield(m, 3), 2), T})
rows(m::MatrixTable) = m
Base.eltype(m::MatrixTable{T}) where {T} = MatrixRow{T}
Base.length(m::MatrixTable) = size(getfield(m, 3), 1)

function Base.iterate(m::MatrixTable, st=1)
    st > length(m) && return nothing
    return MatrixRow(st, m), st + 1
end

# column interface
columnaccess(::Type{<:MatrixTable}) = true
columns(m::MatrixTable) = m
Base.getproperty(m::MatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = getfield(m, 3)[:, col]
Base.getproperty(m::MatrixTable, nm::Symbol) = getfield(m, 3)[:, getfield(m, 2)[nm]]

function astable(m::Union{Adjoint, Matrix}; header::Vector{Symbol}=[Symbol("Column$i") for i = 1:size(m, 2)])
    length(header) == size(m, 2) || throw(ArgumentError("provided column names `header` length must match number of columns in matrix ($(size(m, 2))"))
    lookup = Dict(nm=>i for (i, nm) in enumerate(header))
    return MatrixTable(header, lookup, m)
end

function asmatrix(table)
    cols = Tables.columns(table)
    T = reduce(promote_type, schema(cols).types)
    # TODO: finish this
end
