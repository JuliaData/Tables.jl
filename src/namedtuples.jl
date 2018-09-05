# Vector of NamedTuples
const RowTable{T} = Vector{T} where {T <: NamedTuple}

# interface implementation
istable(::Type{<:RowTable}) = true
rowaccess(::Type{<:RowTable}) = true
# a Vector of NamedTuple iterates `Row`s itself
rows(x::RowTable) = x
schema(x::Vector{NamedTuple{names, types}}) where {names, types} = Schema(names, types)

# struct to transform `Row`s into NamedTuples
struct NamedTupleIterator{S, T}
    x::T
end
Base.IteratorEltype(::Type{<:NamedTupleIterator{S}}) where {S} = S === nothing ? Base.EltypeUnknown() : Base.HasEltype()
Base.eltype(rows::NamedTupleIterator{Schema{names, T}}) where {names, T} = NamedTuple{names, T}
Base.IteratorSize(::Type{NamedTupleIterator{S, T}}) where {S, T} = Base.IteratorSize(T)
Base.length(nt::NamedTupleIterator) = length(nt.x)
Base.size(nt::NamedTupleIterator) = (length(nt.x),)

function Base.iterate(rows::NamedTupleIterator{Schema{names, T}}, st=()) where {names, T}
    if @generated
        vals = Tuple(:(getproperty(row, $(fieldtype(T, i)), $i, $(Meta.QuoteNode(names[i])))) for i = 1:fieldcount(T))
        return quote
            x = iterate(rows.x, st...)
            x === nothing && return nothing
            row, st = x
            return $(NamedTuple{names, T})(($(vals...),)), (st,)
        end
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{names, T}(Tuple(getproperty(row, fieldtype(T, i), i, nm) for i = 1:fieldcount(T))), (st,)
    end
end

# unknown schema case
function Base.iterate(rows::NamedTupleIterator{nothing, T}, st=()) where {T}
    x = iterate(rows.x, st...)
    x === nothing && return nothing
    row, st = x
    names = propertynames(row)
    return NamedTuple{names}(Tuple(getproperty(row, nm) for nm in names)), (st,)
end

namedtupleiterator(::Type{T}, rows::S) where {T <: NamedTuple, S} = rows
namedtupleiterator(::Type{T}, rows::S) where {T, S} = NamedTupleIterator{typeof(schema(rows)), S}(rows)

# sink function
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
# a NamedTuple of AbstractVectors is itself a `Columns` object
columns(x::ColumnTable) = x
schema(x::T) where {T <: ColumnTable} = Schema(names(T), _types(T))

_eltype(::Type{A}) where {A <: AbstractVector{T}} where {T} = T
Base.@pure function _types(::Type{NT}) where {NT <: NamedTuple{names, T}} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
    return Tuple{Any[ _eltype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
end

getarray(x::AbstractArray) = x
getarray(x) = collect(x)

columntable(sch::Schema{names, types}, x::T) where {names, types, T <: ColumnTable} = x
function columntable(sch::Schema{names, types}, cols) where {names, types}
    if @generated
        vals = Tuple(:(getarray(getproperty(cols, $(fieldtype(types, i)), $i, $(Meta.QuoteNode(names[i]))))) for i = 1:fieldcount(types))
        return :(NamedTuple{names}(($(vals...),)))
    else
        return NamedTuple{names}(Tuple(getarray(getproperty(cols, fieldtype(types, i), i, nm)) for i = 1:fieldcount(types)))
    end
end
# unknown schema case
columntable(::Nothing, cols) = NamedTuple{propertynames(cols)}(Tuple(getarray(col) for col in eachcolumn(cols)))

function columntable(itr::T) where {T}
    istable(T) || throw(ArgumentError("NamedTuple of AbstractVectors requires a table input"))
    cols = columns(itr)
    return columntable(schema(cols), cols)
end
columntable(x::ColumnTable) = x
