# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

# interface implementation
istable(x::RowTable) = true
rowaccess(::Type{T}) where {T <: RowTable} = true
rows(x::RowTable) = x
schema(x::Vector{NamedTuple{names, types}}) where {names, types} = Schema(names, types)

struct NamedTupleIterator{sch, S}
    x::S
end
Base.IteratorEltype(::Type{NamedTupleIterator{Schema{N, T}, S}}) where {N, T, S} = T === nothing ? Base.EltypeUnknown() : Base.HasEltype()
Base.eltype(rows::NamedTupleIterator{Schema{names, T}, S}) where {names, T, S} = NamedTuple{names, T}
Base.IteratorSize(::Type{NamedTupleIterator{Schema{N, T}, S}}) where {N, T, S} = Base.IteratorSize(S)
Base.length(nt::NamedTupleIterator) = length(nt.x)
Base.size(nt::NamedTupleIterator) = (length(nt.x),)

function Base.iterate(rows::NamedTupleIterator{Schema{names, T}}, st=()) where {names, T}
    if @generated
        if T === nothing
            vals = Tuple(:(getproperty(row, $(Meta.QuoteNode(names[i])))) for i = 1:length(names))
            NT = NamedTuple{names}
        else
            NT = NamedTuple{names, T}
            vals = Tuple(:(getproperty(row, $(fieldtype(T, i)), $i, $(Meta.QuoteNode(names[i])))) for i = 1:fieldcount(T))
        end
        return quote
            x = iterate(rows.x, st...)
            x === nothing && return nothing
            row, st = x
            return $NT(($(vals...),)), (st,)
        end
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        if T === nothing
            return NamedTuple{names}(Tuple(getproperty(row, nm) for i = 1:length(names))), (st,)
        else
            return NamedTuple{names, T}(Tuple(getproperty(row, fieldtype(T, i), i, nm) for i = 1:fieldcount(T))), (st,)
        end
    end
end

namedtupleiterator(::Type{T}, rows::S) where {T <: NamedTuple, S} = rows
namedtupleiterator(::Type{T}, rows::S) where {T, S} = NamedTupleIterator{typeof(schema(rows)), S}(rows)

function rowtable(itr::T) where {T}
    istable(T) || throw(ArgumentError("Vector of NamedTuples requires a table input"))
    r = rows(itr)
    return collect(namedtupleiterator(eltype(r), r))
end

# NamedTuple of Vectors
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
rowcount(c::ColumnTable) = length(c) == 0 ? 0 : length(c[1])

# interface implementation
istable(::Type{<:ColumnTable}) = true
columnaccess(::Type{<:ColumnTable}) = true
columns(x::ColumnTable) = x
schema(x::T) where {T <: ColumnTable} = Schema(names(T), types(T))

_eltype(::Type{A}) where {A <: AbstractVector{T}} where {T} = T
Base.@pure function types(::Type{NT}) where {NT <: NamedTuple{names, T}} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
    return Tuple{Any[ _eltype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
end

getarray(x::AbstractArray) = x
getarray(x) = collect(x)

columntable(sch::Schema{names, types}, x::T) where {names, types, T <: ColumnTable} = x
function columntable(sch::Schema{names, types}, cols) where {names, types}
    if @generated
        if types === nothing
            vals = Tuple(:(getarray(getproperty(cols, $(Meta.QuoteNode(names[i]))))) for i = 1:length(names))
        else
            vals = Tuple(:(getarray(getproperty(cols, $(fieldtype(types, i)), $i, $(Meta.QuoteNode(names[i]))))) for i = 1:fieldcount(types))
        end
        return :(NamedTuple{names}(($(vals...),)))
    else
        if types === nothing
            return NamedTuple{names}(Tuple(getarray(getproperty(cols, nm)) for i = 1:length(names)))
        else
            return NamedTuple{names}(Tuple(getarray(getproperty(cols, fieldtype(types, i), i, nm)) for i = 1:fieldcount(types)))
        end
    end
end

function columntable(itr::T) where {T}
    istable(T) || throw(ArgumentError("NamedTuple of AbstractVectors requires a table input"))
    cols = columns(itr)
    return columntable(schema(cols), cols)
end
columntable(x::ColumnTable) = x
