# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

rows(x::RowTable) = x
schema(x::RowTable{T}) where {T} = T

struct NamedTupleIterator{NT, T}
    x::T
end
NamedTupleIterator(::Type{NT}, x::T) where {NT <: NamedTuple, T} = NamedTupleIterator{NT, T}(x)
Base.eltype(rows::NamedTupleIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{NamedTupleIterator{NT, T}}) where {NT, T} = Base.IteratorSize(T)
Base.length(nt::NamedTupleIterator) = length(nt.x)

function Base.iterate(rows::NamedTupleIterator{NamedTuple{names, types}, T}, st=()) where {names, types, T}
    x = iterate(rows.x, st...)
    x === nothing && return nothing
    row, st = x
    if @generated
        vals = Tuple(:(getproperty(row, $(Meta.QuoteNode(nm)))) for nm in names)
        return :($(NamedTuple{names, types})(($(vals...),)), (st,))
    else
        return NamedTuple{names, types}(Tuple(getproperty(row, nm) for nm in names)), (st,)
    end
end

namedtupleiterator(::Type{T}, sch, rows) where {T <: NamedTuple} = rows
namedtupleiterator(T, sch, rows) = NamedTupleIterator(sch, rows)

function rowtable(itr)
    r = rows(itr)
    return collect(namedtupleiterator(eltype(r), schema(itr), r))
end

# NamedTuple of Vectors
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
rowcount(c::ColumnTable) = length(c) == 0 ? 0 : length(c[1])

_eltype(::Type{AbstractVector{T}}) where {T} = T
schema(ct::T) where {T <: ColumnTable} = schema(T)
Base.@pure function schema(::Type{NT}) where {NT <: NamedTuple{names, T}} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
    TT = Tuple{Any[ _eltype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

AccessStyle(::Type{<:ColumnTable}) = ColumnAccess()
columns(x::ColumnTable) = x
rows(x::ColumnTable) = RowIterator(schema(x), x)

getarray(x::AbstractArray) = x
getarray(x) = collect(x)

columntable(::Type{NamedTuple{names, types}}, x::T) where {names, types, T <: ColumnTable} = x
function columntable(::Type{NamedTuple{names, types}}, cols) where {names, types}
    if @generated
        vals = Tuple(:(getarray(getproperty(cols, $(Meta.QuoteNode(nm))))) for nm in names)
        return :(NamedTuple{names}(($(vals...),)))
    else
        return NamedTuple{names}(Tuple(collect(getproperty(cols, nm)) for nm in names))
    end
end

columntable(rows) = columntable(schema(rows), columns(rows))
columntable(x::ColumnTable) = x
