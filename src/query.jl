nondatavaluetype(::Type{T}) where {T} = T
datavaluetype(::Type{T}) where {T} = T

Base.@pure function nondatavaluetype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ nondatavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

Base.@pure function datavaluetype(::Tables.Schema{names, types}) where {names, types}
    TT = Tuple{Any[ datavaluetype(fieldtype(types, i)) for i = 1:fieldcount(types) ]...}
    return NamedTuple{names, TT}
end

unwrap(x) = x

struct DataValueUnwrapper{S}
    x::S
end

Tables.istable(::Type{<:DataValueUnwrapper}) = true
Tables.rowaccess(::Type{<:DataValueUnwrapper}) = true
Tables.rows(x::DataValueUnwrapper) = x

function Tables.schema(dv::DataValueUnwrapper)
    eT = eltype(dv.x)
    !(eT <: NamedTuple) && return nothing
    return Tables.Schema(nondatavaluetype(eT))
end
Base.eltype(rows::DataValueUnwrapper) = DataValueUnwrapRow{eltype(rows.x)}
Base.IteratorSize(::Type{DataValueUnwrapper{S}}) where {S} = Base.IteratorSize(S)
Base.length(rows::DataValueUnwrapper) = length(rows.x)

function Base.iterate(rows::DataValueUnwrapper, st=())
    x = iterate(rows.x, st...)
    x === nothing && return nothing
    row, st = x
    return DataValueUnwrapRow(row), (st,)
end

struct DataValueUnwrapRow{T}
    row::T
end

Base.getproperty(d::DataValueUnwrapRow, ::Type{T}, col::Int, nm::Symbol) where {T} = unwrap(getproperty(getfield(d, 1), T, col, nm))
Base.getproperty(d::DataValueUnwrapRow, nm::Symbol) = unwrap(getproperty(getfield(d, 1), nm))
Base.propertynames(d::DataValueUnwrapRow) = propertynames(getfield(d, 1))