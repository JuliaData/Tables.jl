istable(::Type{<:AbstractSparseMatrix}) = false
istable(::AbstractSparseMatrix) = false

rows(m::T) where {T <: AbstractSparseMatrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.table` for ways to treat an AbstractSparseMatrix as a table"))
columns(m::T) where {T <: AbstractSparseMatrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.table` for ways to treat an AbstractSparseMatrix as a table"))

struct SparseMatrixTable{T <: AbstractSparseMatrix}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::T
end

istable(::Type{<:SparseMatrixTable}) = true
names(m::SparseMatrixTable) = getfield(m, :names)

# row interface
struct SparseMatrixRow{T}
    row::Int
    source::SparseMatrixTable{T}
end

Base.getproperty(m::SparseMatrixRow, ::Type, col::Int, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), col]
Base.getproperty(m::SparseMatrixRow, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), getfield(getfield(m, :source), :lookup)[nm]]
Base.propertynames(m::SparseMatrixRow) = names(getfield(m, :source))

rowaccess(::Type{<:SparseMatrixTable}) = true
schema(m::SparseMatrixTable{T}) where {T} = Schema(Tuple(names(m)), NTuple{size(getfield(m, :matrix), 2), eltype(T)})
rows(m::SparseMatrixTable) = m
Base.eltype(m::SparseMatrixTable{T}) where {T} = SparseMatrixRow{T}
Base.length(m::SparseMatrixTable) = size(getfield(m, :matrix), 1)

function Base.iterate(m::SparseMatrixTable, st=1)
    st > length(m) && return nothing
    return SparseMatrixRow(st, m), st + 1
end

# column interface
columnaccess(::Type{<:SparseMatrixTable}) = true
columns(m::SparseMatrixTable) = m
Base.getproperty(m::SparseMatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = getfield(m, :matrix)[:, col]
Base.getproperty(m::SparseMatrixTable, nm::Symbol) = getfield(m, :matrix)[:, getfield(m, :lookup)[nm]]
Base.propertynames(m::SparseMatrixTable) = names(m)

"""
Tables.table(m::AbstractSparseMatrix; [header::Vector{Symbol}])

Wrap an `AbstractSparseMatrix` (`SparseMatrixCSC`, etc.) in a `SparseMatrixTable`, which satisfies
the Tables.jl interface. Thiss allows accesing the matrix via `Tables.rows` and
`Tables.columns`. An optional keyword argument `header` can be passed as a `Vector{Symbol}`
to be used as the column names. Note that no copy of the `AbstractSparseMatrix` is made.
"""
function table(m::AbstractSparseMatrix; header::Vector{Symbol}=[Symbol("Column$i") for i = 1:size(m, 2)])
    length(header) == size(m, 2) || throw(ArgumentError("provided column names `header` length must match number of columns in matrix ($(size(m, 2))"))
    lookup = Dict(nm=>i for (i, nm) in enumerate(header))
    return SparseMatrixTable(header, lookup, m)
end

"""
Tables.sparsematrix(table; transpose::Bool=false)

Materialize any table source input as a `SparseMatrixCSC`.
All elements must be numeric,
and if the table column types are not homogenous,
they will be promoted to a common type in the materialized `SparseMatrixCSC`.
Note that column names are ignored in the conversion.
By default, input table columns will be materialized as
corresponding matrix columns;
passing `transpose=true` will transpose the input with input columns as matrix rows.
"""
function sparsematrix(table; transpose::Bool=false)
    cols = Tables.columns(table)
    types = schema(cols).types
    T = reduce(promote_type, types)
    n, p = rowcount(cols), length(types)
    if !transpose
        mat = spzeros(T, n, p)
        for (i, col) in enumerate(Tables.eachcolumn(cols))
            mat[:, i] = col
        end
    else
        mat = spzeros(T, p, n)
        for (i, col) in enumerate(Tables.eachcolumn(cols))
            mat[i, :] = col
        end
    end
    return mat
end
