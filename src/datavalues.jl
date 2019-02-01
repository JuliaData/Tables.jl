using .DataValues

# DataValue overloads for iteratorwrapper.jl definitions
nondatavaluetype(::Type{DataValue{T}}) where {T} = Union{T, Missing}
unwrap(x::DataValue) = isna(x) ? missing : DataValues.unsafe_get(x)
datavaluetype(::Type{T}) where {T <: DataValue} = T
datavaluetype(::Type{Union{T, Missing}}) where {T} = DataValue{T}
datavaluetype(::Type{Missing}) = DataValue{Union{}}
scalarconvert(T, x) = convert(T, x)
scalarconvert(::Type{T}, x::T) where {T} = x
scalarconvert(::Type{T}, ::Missing) where {T <: DataValue} = T()

@require CategoricalArrays="324d7699-5711-5eae-9e2f-1d82baa6b597" begin
    using .CategoricalArrays
    scalarconvert(::Type{DataValue{T}}, x::T) where {T <: CategoricalArrays.CatValue} = DataValue(x)
end

struct DataValueRowIterator{NT, S}
    x::S
end
DataValueRowIterator(::Type{NT}, x::S) where {NT <: NamedTuple, S} = DataValueRowIterator{NT, S}(x)

"Returns a DataValue-based NamedTuple-iterator"
DataValueRowIterator(::Type{Schema{names, types}}, x::S) where {names, types, S} = DataValueRowIterator{datavaluetype(NamedTuple{names, types}), S}(x)

function datavaluerows(x)
    r = Tables.rowtable(x)
    return DataValueRowIterator(datavaluetype(Tables.schema(r)), r)
end

_iteratorsize(x) = x
_iteratorsize(::Base.HasShape{1}) = Base.HasLength()

Base.eltype(rows::DataValueRowIterator{NT, S}) where {NT, S} = NT
Base.IteratorSize(::Type{DataValueRowIterator{NT, S}}) where {NT, S} = _iteratorsize(Base.IteratorSize(S))
Base.length(rows::DataValueRowIterator) = length(rows.x)

function Base.iterate(rows::DataValueRowIterator{NT, S}, st=()) where {NT <: NamedTuple{names}, S} where {names}
    if @generated
        vals = Tuple(:(scalarconvert($(fieldtype(NT, i)), getproperty(row, $(nondatavaluetype(fieldtype(NT, i))), $i, $(Meta.QuoteNode(names[i]))))) for i = 1:fieldcount(NT))
        q = quote
            x = iterate(rows.x, st...)
            x === nothing && return nothing
            row, st = x
            return $NT(($(vals...),)), (st,)
        end
        # @show q
        return q
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        return NT(Tuple(scalarconvert(fieldtype(NT, i), getproperty(row, nondatavaluetype(fieldtype(NT, i)), i, names[i])) for i = 1:fieldcount(NT))), (st,)
    end
end

