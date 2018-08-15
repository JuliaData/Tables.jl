# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

rows(x::RowTable) = x
schema(x::RowTable{T}) where {T} = T

struct NamedTupleIterator{NT, T}
    x::T
end
Base.eltype(rows::NamedTupleIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{<:NamedTupleIterator}) = Base.SizeUnknown()

function Base.iterate(rows::NamedTupleIterator{NamedTuple{names, types}, T}) where {names, types, T}
    if @generated
        vals = Tuple(:(getproperty(row, $(Meta.QuoteNode(nm)))) for nm in names)
        return quote
            x = iterate(rows.x)
            x === nothing && return nothing
            row, st = x
            return ($(NamedTuple{names, types})(($(vals...),)), st)
        end
    else
        x = iterate(rows.x)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{names, types}(Tuple(getproperty(row, nm) for nm in names)), st
    end
end

function Base.iterate(rows::NamedTupleIterator{NamedTuple{names, types}, T}, st) where {names, types, T}
    if @generated
        vals = Tuple(:(getproperty(row, $(Meta.QuoteNode(nm)))) for nm in names)
        return quote
            x = iterate(rows.x, st)
            x === nothing && return nothing
            row, st = x
            return ($(NamedTuple{names, types})(($(vals...),)), st)
        end
    else
        x = iterate(rows.x, st)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{names, types}(Tuple(getproperty(row, nm) for nm in names)), st
    end
end

function rowtable(itr)
    r = rows(itr)
    return collect(eltype(r) <: NamedTuple ? r : NamedTupleIterator{schema(itr), typeof(r)}(r))
end

# NamedTuple of Vectors
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
rowcount(c::ColumnTable) = length(c) == 0 ? 0 : length(c[1])

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
rows(x::ColumnTable) = RowIterator(x)

function columntable(::Type{NamedTuple{names, types}}, cols) where {names, types}
    if @generated
        vals = Tuple(:(collect(getproperty(cols, $(Meta.QuoteNode(nm))))) for nm in names)
        return :(NamedTuple{names}(($(vals...),)))
    else
        return NamedTuple{names}(Tuple(collect(getproperty(cols, nm)) for nm in names))
    end
end

columntable(rows) = columntable(schema(rows), columns(rows))
columntable(x::ColumnTable) = x