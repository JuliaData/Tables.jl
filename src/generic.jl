"""
    Tables.astable(x)

Wrap a runtime-compatible table so that `Tables.istable` returns `true`.
The wrapper delegates table access to `x` without copying it. This is useful when a
consumer accepts only objects that explicitly opt in to the Tables.jl interface.
"""
astable(x) = GenericTable(x)

struct GenericTable{T}
    source::T
end

astable(x::GenericTable) = x
istable(::Type{<:GenericTable}) = true
rowaccess(::Type{GenericTable{T}}) where {T} = rowaccess(T)
columnaccess(::Type{GenericTable{T}}) where {T} = columnaccess(T)
rows(x::GenericTable) = rows(getfield(x, :source))
columns(x::GenericTable) = columns(getfield(x, :source))
schema(x::GenericTable) = schema(getfield(x, :source))
materializer(x::GenericTable) = materializer(getfield(x, :source))

DataAPI.nrow(x::GenericTable) = DataAPI.nrow(getfield(x, :source))
DataAPI.ncol(x::GenericTable) = DataAPI.ncol(getfield(x, :source))
