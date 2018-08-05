# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

# Tables.rows(x) = x
schema(x::RowTable{T}) where {T} = Schema(T)

producescells(::Type{T}) where {T <: RowTable} = true
getcell(source::RowTable, ::Type{T}, row, col) where {T} = source[row][col]

rowtable(itr) = Base.collect(rows(itr))

# NamedTuple of Vectors
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

schema(::NamedTuple{names, T}) where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N} =
    Schema((eltype(x) for x in T.parameters), Base.collect(map(string, names)))

producescells(::Type{T}) where {T <: ColumnTable} = true
getcell(source::ColumnTable, ::Type{T}, row, col) where {T} = source[col][row]

producescolumns(::Type{T}) where {T <: ColumnTable} = true
getcolumn(source::ColumnTable, ::Type{T}, col) where {T} = source[col]
acceptscolumns(::Type{T}) where {T <: ColumnTable} = true

function setcolumn!(sink::ColumnTable, column::AbstractVector{T}, col) where {T}
    col > length(sink) && throw(ArgumentError("out of bounds; tried to Tables.setcolumn! on col $col where this sink only has $(length(sink)) cols"))
    append!(sink[col], column)
    return sink[col]
end

makecolumn(::Type{T}, len=0) where {T} = Vector{T}(undef, len)

function Base.NamedTuple(sch::Schema)
    typs = types(sch)
    return NamedTuple{Tuple(map(Symbol, sch.header))}(Tuple(makecolumn(T) for T in typs))
end

function columntable(rows::Vector{NamedTuple{names, T}}) where {names, T}
    if @generated
        vals = Tuple(:(makecolumn($typ, len)) for typ in T.parameters)
        innerloop = Expr(:block)
        for i = 1:length(names)
            push!(innerloop.args, :(nt[$i][i] = row[$i]))
        end
        q = quote
            len = length(rows)
            nt = NamedTuple{names}(($(vals...),))
            for (i, row) in enumerate(rows)
                $innerloop
            end
            return nt
        end
        @show q
        return q
    else
        nt = NamedTuple{names}(Tuple(makecolumn(typ, length(rows)) for typ in T.parameters))
        for (i, row) in enumerate(rows)
            for (j, val) in enumerate(row)
                nt[j][i] = val
            end
        end
        return nt
    end
end

# Row iteration for Data.Sources
struct Rows{S <: ColumnTable, NT}
    source::S
end

Base.eltype(rows::Rows{S, NT}) where {S, NT} = NT
Base.length(rows::Rows) = length(rows.source)

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
