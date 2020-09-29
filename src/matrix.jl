istable(::AbstractMatrix) = false
istable(::Type{<:AbstractMatrix}) = false

rows(m::T) where {T <: AbstractMatrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.table` for ways to treat an AbstractMatrix as a table"))
columns(m::T) where {T <: AbstractMatrix} = throw(ArgumentError("a '$T' is not a table; see `?Tables.table` for ways to treat an AbstractMatrix as a table"))

struct MatrixTable{T <: AbstractMatrix} <: AbstractColumns
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::T
end

isrowtable(::Type{<:MatrixTable}) = true
names(m::MatrixTable) = getfield(m, :names)

# row interface
struct MatrixRow{T} <: AbstractRow
    row::Int
    source::MatrixTable{T}
end

getcolumn(m::MatrixRow, ::Type, col::Int, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), col]
getcolumn(m::MatrixRow, i::Int) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), i]
getcolumn(m::MatrixRow, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), getfield(getfield(m, :source), :lookup)[nm]]
columnnames(m::MatrixRow) = names(getfield(m, :source))

schema(m::MatrixTable{T}) where {T} = Schema(Tuple(names(m)), NTuple{size(getfield(m, :matrix), 2), eltype(T)})
Base.eltype(m::MatrixTable{T}) where {T} = MatrixRow{T}
Base.length(m::MatrixTable) = size(getfield(m, :matrix), 1)

Base.iterate(m::MatrixTable, st=1) = st > length(m) ? nothing : (MatrixRow(st, m), st + 1)

# column interface
Columns(m::T) where {T <: MatrixTable} = Columns{T}(m)
columnaccess(::Type{<:MatrixTable}) = true
columns(m::MatrixTable) = m
getcolumn(m::MatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = getfield(m, :matrix)[:, col]
getcolumn(m::MatrixTable, nm::Symbol) = getfield(m, :matrix)[:, getfield(m, :lookup)[nm]]
getcolumn(m::MatrixTable, i::Int) = getfield(m, :matrix)[:, i]
columnnames(m::MatrixTable) = names(m)

"""
    Tables.table(matrix::AbstractMatrix; [header])

Wrap an `AbstractMatrix` (`Matrix`, `Adjoint`, etc.) in a `MatrixTable`, which satisfies the
Tables.jl interface. This allows accessing the matrix via `Tables.rows` and `Tables.columns`

# Arguments.
- `matrix::AbstractMatrix`: The matrix to be wrapped in a `MatrixTable`.

# Keywords
- `header=[Symbol("Column\$i") for i = 1:size(matrix, 2)]`: An iterator (`Tuple`, `AbstractVector` etc.) 
    to be used as the column names in  the `MatrixTable`.
- `reuse_header::Bool=false`: if set to `true` and `header::Vector{Symbol}` then the `header` passed is re-used 
    as the column names of the `MatrixTable` else a freshly allocated `Vector{Symbol}` object derived from `header` 
    is used as the column names of the `MatrixTable`.

Note that no copy of the `AbstractMatrix`is made.
"""
function table(matrix::AbstractMatrix; header=[Symbol("Column$i") for i = 1:size(matrix, 2)], reuse_header=false)
    if header isa Vector{Symbol} && reuse_header
	symbol_header = header
    else
	symbol_header = [Symbol(h) for h in header]
    end	
    if length(symbol_header) != size(matrix, 2)
        throw(ArgumentError("provided column names `header` length must match number of columns in matrix ($(size(matrix, 2)))"))
    end
    lookup = Dict(nm=>i for (i, nm) in enumerate(symbol_header))
    return MatrixTable(symbol_header, lookup, matrix)
end

"""
    Tables.matrix(table; transpose::Bool=false)

Materialize any table source input as a new `Matrix` or in the case of a `MatrixTable`
return the originally wrapped matrix. If the table column element types are not homogenous,
they will be promoted to a common type in the materialized `Matrix`. Note that column names are
ignored in the conversion. By default, input table columns will be materialized as corresponding
matrix columns; passing `transpose=true` will transpose the input with input columns as matrix rows
or in the case of a `MatrixTable` apply `permutedims` to the originally wrapped matrix.
"""
function matrix(table; transpose::Bool=false)
    cols = columns(table)
    types = schema(cols).types
    T = reduce(promote_type, types)
    n, p = rowcount(cols), length(types)
    if !transpose
        matrix = Matrix{T}(undef, n, p)
        for (i, col) in enumerate(Columns(cols))
            matrix[:, i] = col
        end
    else
        matrix = Matrix{T}(undef, p, n)
        for (i, col) in enumerate(Columns(cols))
            matrix[i, :] = col
        end
    end
    return matrix
end
	
function matrix(table::MatrixTable; transpose::Bool=false) 
    matrix = getfield(table, :matrix)
    transpose || return matrix
    return permutedims(matrix)
end
