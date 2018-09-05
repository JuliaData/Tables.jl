using .Query

@static if isdefined(Query.QueryOperators, :Enumerable)

import .Query.QueryOperators: Enumerable

Tables.istable(::Type{<:Enumerable}) = true
Tables.rowaccess(::Type{<:Enumerable}) = true
Tables.rows(e::Enumerable) = DataValueUnwrapper(e)

struct DataValueUnwrapper{S}
    x::S
end

Tables.schema(dv::DataValueUnwrapper) = Tables.Schema(nondatavaluetype(eltype(dv.x)))
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

end # isdefined