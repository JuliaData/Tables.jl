using .QueryOperators

struct DataValueUnwrapper{NT, S}
    x::S
end

Base.eltype(rows::DataValueUnwrapper{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{<:DataValueUnwrapper}) = Base.SizeUnknown()

function Tables.schema(e::QueryOperators.Enumerable)
    sch = eltype(e)
    return NamedTuple{names(sch), Tuple{map(nondatavaluetype, types(sch))...}}
end
Tables.rows(e::E) where {E <: QueryOperators.Enumerable} = DataValueUnwrapper{schema(e), E}(e)

unwrap(x) = x
unwrap(x::DataValue) = isna(x) ? missing : DataValues.unsafe_get(x)

function Base.iterate(rows::DataValueUnwrapper{NamedTuple{names, types}, S}) where {names, types, S}
    if @generated
        vals = Tuple(:(unwrap(getproperty(row, $(Meta.QuoteNode(nm))))) for (nm, T) in zip(names, types.parameters))
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
        return NamedTuple{names, types}(Tuple(unwrap(getproperty(row, nm)) for (nm, T) in zip(names, types.parameters))), st
    end
end

function Base.iterate(rows::DataValueUnwrapper{NamedTuple{names, types}, S}, st) where {names, types, S}
    if @generated
        vals = Tuple(:(unwrap(getproperty(row, $(Meta.QuoteNode(nm))))) for (nm, T) in zip(names, types.parameters))
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
        return NamedTuple{names, types}(Tuple(unwrap(getproperty(row, nm)) for (nm, T) in zip(names, types.parameters))), st
    end
end

using IteratorInterfaceExtensions
IteratorInterfaceExtensions.getiterator(x::Tables.Table) = Tables.datavaluerows(x)
IteratorInterfaceExtensions.isiterable(x::Tables.Table) = true

IteratorInterfaceExtensions.getiterator(x::Tables.ColumnTable) = Tables.datavaluerows(x)
IteratorInterfaceExtensions.isiterable(x::Tables.ColumnTable) = true
