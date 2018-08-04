# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

# Tables.rows(x) = x
schema(x::RowTable{T}) where {T} = Schema(T)

producescells(::Type{T}) where {T <: RowTable} = true
getcell(source::RowTable, ::Type{T}, row, col) where {T} = source[row][col]
acceptscells(::Type{T}) where {T <: RowTable} = true

function setcell!(sink::RowTable, val::T, row, col) where {T}
    row > length(sink) && throw(ArgumentError("out of bounds; tried to Tables.setcell! on row $row where this sink only has $(length(sink)) rows"))
    r = sink[row]
    col > length(r) && throw(ArgumentError("out of bounds; tried to Tables.setcell! on col $col where this sink only has $(length(r)) cols"))
    r[col] = val
    return val
end

rowtable(itr) = Base.collect(rows(itr))

# NamedTuple of Vectors
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

schema(df::NamedTuple{names, T}) where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N} =
    Schema((eltype(x) for x in T.parameters), Base.collect(map(string, names)))

producescells(::Type{T}) where {T <: ColumnTable} = true
getcell(source::ColumnTable, ::Type{T}, row, col) where {T} = source[col][row]
acceptscells(::Type{T}) where {T <: ColumnTable} = true

function setcell!(sink::ColumnTable, val::T, row, col) where {T}
    col > length(sink) && throw(ArgumentError("out of bounds; tried to Tables.setcell! on col $col where this sink only has $(length(sink)) cols"))
    c = sink[col]
    row > length(c) && throw(ArgumentError("out of bounds; tried to Tables.setcell! on row $row where this sink only has $(length(c)) rows"))
    c[row] = val
    return val
end

producescolumns(::Type{T}) where {T <: ColumnTable} = true
getcolumn(source::ColumnTable, ::Type{T}, col) where {T} = source[col]
acceptscolumns(::Type{T}) where {T <: ColumnTable} = true

function setcolumn!(sink::ColumnTable, column::AbstractVector{T}, col) where {T}
    col > length(sink) && throw(ArgumentError("out of bounds; tried to Tables.setcolumn! on col $col where this sink only has $(length(sink)) cols"))
    append!(sink[col], column)
    return sink[col]
end

# Row iteration for Data.Sources
struct Rows{S <: ColumnTable, NT}
    source::S
end

Base.eltype(rows::Rows{S, NT}) where {S, NT} = NT
Base.length(rows::Rows) = length(rows.source) == 0 ? 0 : length(getfield(rows.source, 1))

# NamedTuples don't allow duplicate names, so make sure there are no duplicates in header
function makeunique(names::Vector{String})
    nms = [Symbol(nm) for nm in names]
    seen = Set{Symbol}()
    for (i, x) in enumerate(nms)
        x in seen ? setindex!(nms, Symbol("$(x)_$i"), i) : push!(seen, x)
    end
    return (nms...,)
end

"Returns a NamedTuple-iterator"
function rows(source::S) where {S <: ColumnTable}
    sch = schema(source)
    names = makeunique(sch.header)
    typs = types(sch)
    return Rows{S, NamedTuple{names, Tuple{typs...}}}(source)
end

function Base.iterate(rows::Rows{S, NamedTuple{names, types}}, st=1) where {S, names, types}
    if @generated
        vals = Tuple(:(getcell(rows.source, $typ, st, $col)) for (col, typ) in enumerate(types.parameters) )
        q = quote
            st > length(rows) && return nothing
            return ($(NamedTuple{names, types}))(($(vals...),)), st + 1
        end
    else
        st > length(rows) && return nothing
        return NamedTuple{names, types}(Tuple(getcell(rows.source, T, st, col) for (col, T) in enumerate(types.parameters))), st + 1
    end
end
