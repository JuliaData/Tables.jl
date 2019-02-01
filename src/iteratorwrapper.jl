nondatavaluetype(::Type{T}) where {T} = T
nondatavaluetype(::Type{Union{}}) = Union{}
datavaluetype(::Type{T}) where {T} = T
datavaluetype(::Type{Union{}}) = Union{}

Base.@pure function nondatavaluetype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ nondatavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

Base.@pure function datavaluetype(::Tables.Schema{names, types}) where {names, types}
    TT = Tuple{Any[ datavaluetype(fieldtype(types, i)) for i = 1:fieldcount(types) ]...}
    return NamedTuple{names, TT}
end

unwrap(x) = x

struct IteratorWrapper{S}
    x::S
end

Tables.istable(::Type{<:IteratorWrapper}) = true
Tables.rowaccess(::Type{<:IteratorWrapper}) = true
Tables.rows(x::IteratorWrapper) = x

function Tables.schema(dv::IteratorWrapper)
    eT = eltype(dv.x)
    (!(eT <: NamedTuple) || eT === Union{}) && return nothing
    return Tables.Schema(nondatavaluetype(eT))
end
Base.eltype(rows::IteratorWrapper) = IteratorRow{eltype(rows.x)}
Base.IteratorSize(::Type{IteratorWrapper{S}}) where {S} = Base.IteratorSize(S)
Base.length(rows::IteratorWrapper) = length(rows.x)

@noinline invalidtable(::T, ::S) where {T, S} = throw(ArgumentError("'$T' iterates '$S' values, which don't satisfy the Tables.jl Row-iterator interface"))

function Base.iterate(rows::IteratorWrapper)
    x = iterate(rows.x)
    x === nothing && return nothing
    row, st = x
    propertynames(row) === () && invalidtable(rows.x, row)
    return IteratorRow(row), st
end

function Base.iterate(rows::IteratorWrapper, st)
    x = iterate(rows.x, st)
    x === nothing && return nothing
    row, st = x
    return IteratorRow(row), st
end

struct IteratorRow{T}
    row::T
end

Base.getproperty(d::IteratorRow, ::Type{T}, col::Int, nm::Symbol) where {T} = unwrap(getproperty(getfield(d, 1), T, col, nm))
Base.getproperty(d::IteratorRow, nm::Symbol) = unwrap(getproperty(getfield(d, 1), nm))
Base.propertynames(d::IteratorRow) = propertynames(getfield(d, 1))
