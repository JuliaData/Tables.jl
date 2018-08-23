using .DataValues

# DataValue-compatible row iteration for Data.Sources
datavaluetype(::Type{T}) where {T <: DataValue} = T
datavaluetype(::Type{T}) where {T} = DataValue{T}
datavaluetype(::Type{Union{T, Missing}}) where {T} = DataValue{T}

nondatavaluetype(::Type{DataValue{T}}) where {T} = Union{T, Missing}
nondatavaluetype(::Type{T}) where {T} = T

struct DataValueRowIterator{NT, S}
    x::S
end

Base.eltype(rows::DataValueRowIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{<:DataValueRowIterator}) = Base.SizeUnknown()

"Returns a DataValue-based NamedTuple-iterator"
function DataValueRowIterator(sch::Type{NamedTuple{names, T}}, x::S) where {names, T, S}
    NT = NamedTuple{names, Tuple{map(datavaluetype, types(sch))...}}
    return DataValueRowIterator{NT, S}(x)
end

function Base.iterate(rows::DataValueRowIterator{NamedTuple{names, types}, S}) where {names, types, S}
    if @generated
        vals = Tuple(:(($T)(getproperty(row, $(Meta.QuoteNode(nm))))) for (nm, T) in zip(names, types.parameters))
        q = quote
            x = iterate(rows.x)
            x === nothing && return nothing
            row, st = x
            return ($(NamedTuple{names, types})(($(vals...),)), st)
        end
        # @show q
        return q
    else
        x = iterate(rows.x)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{names, types}(Tuple(DataValue{T}(getproperty(row, nm)) for (nm, T) in zip(names, types.parameters))), st
    end
end

function Base.iterate(rows::DataValueRowIterator{NamedTuple{names, types}, S}, st) where {names, types, S}
    if @generated
        vals = Tuple(:(($T)(getproperty(row, $(Meta.QuoteNode(nm))))) for (nm, T) in zip(names, types.parameters))
        q = quote
            x = iterate(rows.x, st)
            x === nothing && return nothing
            row, st = x
            return ($(NamedTuple{names, types})(($(vals...),)), st)
        end
        # @show q
        return q
    else
        x = iterate(rows.x, st)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{names, types}(Tuple(DataValue{T}(getproperty(row, nm)) for (nm, T) in zip(names, types.parameters))), st
    end
end

datavaluerows(x) = DataValueRowIterator(schema(x), rows(x))
