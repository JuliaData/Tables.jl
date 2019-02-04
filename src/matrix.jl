istable(::Type{<:AbstractMatrix}) = false

rows(m::T) where {T <: AbstractMatrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.table` for ways to treat an AbstractMatrix as a table"))
columns(m::T) where {T <: AbstractMatrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.table` for ways to treat an AbstractMatrix as a table"))

struct MatrixTable{T <: AbstractMatrix}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::T
end

istable(::Type{<:MatrixTable}) = true
names(m::MatrixTable) = getfield(m, :names)

# row interface
struct MatrixRow{T}
    row::Int
    source::MatrixTable{T}
end

Base.getproperty(m::MatrixRow, ::Type, col::Int, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), col]
Base.getproperty(m::MatrixRow, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), getfield(getfield(m, :source), :lookup)[nm]]
Base.propertynames(m::MatrixRow) = names(getfield(m, :source))

rowaccess(::Type{<:MatrixTable}) = true
schema(m::MatrixTable{T}) where {T} = Schema(Tuple(names(m)), NTuple{size(getfield(m, :matrix), 2), eltype(T)})
rows(m::MatrixTable) = m
Base.eltype(m::MatrixTable{T}) where {T} = MatrixRow{T}
Base.length(m::MatrixTable) = size(getfield(m, :matrix), 1)

function Base.iterate(m::MatrixTable, st=1)
    st > length(m) && return nothing
    return MatrixRow(st, m), st + 1
end

# column interface
columnaccess(::Type{<:MatrixTable}) = true
columns(m::MatrixTable) = m
Base.getproperty(m::MatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = getfield(m, :matrix)[:, col]
Base.getproperty(m::MatrixTable, nm::Symbol) = getfield(m, :matrix)[:, getfield(m, :lookup)[nm]]
Base.propertynames(m::MatrixTable) = names(m)

"""
Tables.table(m::AbstractMatrix; [header::Vector{Symbol}])

Wrap an `AbstractMatrix` (`Matrix`, `Adjoint`, etc.) in a `MatrixTable`, which satisfies
the Tables.jl interface. This allows accesing the matrix via `Tables.rows` and
`Tables.columns`. An optional keyword argument `header` can be passed as a `Vector{Symbol}`
to be used as the column names. Note that no copy of the `AbstractMatrix` is made.
"""
function table(m::AbstractMatrix; header::Vector{Symbol}=[Symbol("Column$i") for i = 1:size(m, 2)])
    length(header) == size(m, 2) || throw(ArgumentError("provided column names `header` length must match number of columns in matrix ($(size(m, 2))"))
    lookup = Dict(nm=>i for (i, nm) in enumerate(header))
    return MatrixTable(header, lookup, m)
end

"""
Tables.matrix(table)

Materialize any table source input as a `Matrix`. If the table column types are not homogenous,
they will be promoted to a common type in the materialized `Matrix`. Note that column names are
ignored in the conversion.
"""
function matrix(table)
    cols = Tables.columns(table)
    types = schema(cols).types
    T = reduce(promote_type, types)
    n, p = rowcount(cols), length(types)
    mat = Matrix{T}(undef, n, p)
    for (i, col) in enumerate(Tables.eachcolumn(cols))
        copyto!(mat, n * (i - 1) + 1, col)
    end
    return mat
end
