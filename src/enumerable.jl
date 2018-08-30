using .QueryOperators

nondatavaluetype(::Type{DataValue{T}}) where {T} = Union{T, Missing}
nondatavaluetype(::Type{T}) where {T} = T

Base.@pure function nondatavaluetype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ nondatavaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

struct DataValueUnwrapRow{T}
    row::T
end

unwrap(x) = x
unwrap(x::DataValue) = isna(x) ? missing : DataValues.unsafe_get(x)
Base.getproperty(d::DataValueUnwrapRow, ::Type{T}, col::Int, nm::Symbol) where {T} = unwrap(getproperty(getfield(d, 1), T, col, nm))
Base.getproperty(d::DataValueUnwrapRow, nm::Symbol) = unwrap(getproperty(getfield(d, 1), nm))
Base.propertynames(d::DataValueUnwrapRow) = propertynames(getfield(d, 1))

struct DataValueUnwrapper{NT, S}
    x::S
end

Base.eltype(rows::DataValueUnwrapper) = DataValueUnwrapRow{eltype(rows.x)}
Base.IteratorSize(::Type{DataValueUnwrapper{NT, S}}) where {NT, S} = Base.IteratorSize(S)
Base.length(rows::DataValueUnwrapper) = length(rows.x)

AccessStyle(::Type{E}) where {E <: QueryOperators.Enumerable} = RowAccess()
schema(e::QueryOperators.Enumerable) = nondatavaluetype(eltype(e))
rows(e::E) where {E <: QueryOperators.Enumerable} = DataValueUnwrapper{schema(e), E}(e)

function Base.iterate(rows::DataValueUnwrapper{NT}, st=()) where {NT <: NamedTuple{names}} where {names}
    x = iterate(rows.x, st...)
    x === nothing && return nothing
    row, st = x
    return DataValueUnwrapRow(row), (st,)
end

using IteratorInterfaceExtensions

IteratorInterfaceExtensions.getiterator(x::RowTable) = datavaluerows(x)
IteratorInterfaceExtensions.isiterable(x::RowTable) = true
IteratorInterfaceExtensions.getiterator(x::ColumnTable) = datavaluerows(x)
IteratorInterfaceExtensions.isiterable(x::ColumnTable) = true
