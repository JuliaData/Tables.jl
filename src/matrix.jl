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
getcolumn(m::MatrixTable, i::Int) = view(getfield(m, :matrix), :, i)
getcolumn(m::MatrixTable, ::Type, col::Int, nm::Symbol) = getcolumn(m, col)
getcolumn(m::MatrixTable, nm::Symbol) = getcolumn(m, getfield(m, :lookup)[nm])
columnnames(m::MatrixTable) = names(m)

"""
    Tables.table(m::AbstractMatrix; [header])

Wrap an `AbstractMatrix` (`Matrix`, `Adjoint`, etc.) in a `MatrixTable`, which satisfies the
Tables.jl interface. This allows accessing the matrix via `Tables.rows` and `Tables.columns`.
An optional keyword argument iterator `header` can be passed which will be converted to a
`Vector{Symbol}` to be used as the column names. Note that no copy of the `AbstractMatrix`
is made.
"""
function table(m::AbstractMatrix; header=[Symbol("Column$i") for i = 1:size(m, 2)])
    symbol_header = header isa Vector{Symbol} ? header : [Symbol(h) for h in header]
    if length(symbol_header) != size(m, 2)
        throw(ArgumentError("provided column names `header` length must match number of columns in matrix ($(size(m, 2)))"))
    end
    lookup = Dict(nm=>i for (i, nm) in enumerate(symbol_header))
    return MatrixTable(symbol_header, lookup, m)
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
