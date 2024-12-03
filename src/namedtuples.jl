# Vector of NamedTuples
const RowTable{T} = AbstractVector{T} where {T <: NamedTuple}

# interface implementation
isrowtable(::Type{<:RowTable}) = true
schema(x::AbstractVector{NamedTuple{names, types}}) where {names, types} = Schema(names, types)
materializer(x::RowTable) = rowtable

# struct to transform `Row`s into NamedTuples
struct NamedTupleIterator{schema, T}
    x::T
end

"""
    Tables.namedtupleiterator(x)

Pass any table input source and return a `NamedTuple` iterator

See also [`rows`](@ref) and [`rowtable`](@ref).

Not for use with extremely wide tables with # of columns > 67K; current fundamental compiler limits
prevent constructing `NamedTuple`s that large.
"""
function namedtupleiterator(x)
    r = rows(x)
    sch = schema(r)
    stored(sch) && throw(ArgumentError("input table too wide ($(length(sch.names)) columns) to construct `NamedTuple` rows"))
    return NamedTupleIterator{typeof(sch), typeof(r)}(r)
end

namedtupleiterator(::Type{T}, x) where {T <: NamedTuple} = x
namedtupleiterator(T, x) = namedtupleiterator(x)

Base.IteratorEltype(::Type{NT}) where {names, types, T, NT<:NamedTupleIterator{Schema{names, types}, T}} = Base.HasEltype()
Base.IteratorEltype(::Type{NT}) where {T, NT<:NamedTupleIterator{Nothing, T}} = Base.EltypeUnknown()
Base.eltype(::Type{NT}) where {names, types, T, NT<:NamedTupleIterator{Schema{names, types}, T}} = NamedTuple{map(Symbol, names), types}
Base.IteratorSize(::Type{NT}) where {sch, T, NT<:NamedTupleIterator{sch, T}} = Base.IteratorSize(T)
Base.length(nt::NamedTupleIterator) = length(nt.x)
Base.size(nt::NamedTupleIterator) = (length(nt.x),)

@inline function _iterate(rows::NamedTupleIterator{Schema{names, T}}, st=()) where {names, T}
    # use of @generated justified because it's user-controlled; they explicitly asked for vector of namedtuples
    if @generated
        vals = Any[ :(getcolumn(row, $(fieldtype(T, i)), $i, $(quot(names[i])))) for i = 1:fieldcount(T) ]
        ret = Expr(:new, :(NamedTuple{names, T}), vals...)
        return quote
            x = iterate(rows.x, st...)
            x === nothing && return nothing
            row, st = x
            return $ret, (st,)
        end
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{map(Symbol, names), T}(Tuple(getcolumn(row, fieldtype(T, i), i, names[i]) for i = 1:fieldcount(T))), (st,)
    end
end

@inline function Base.iterate(rows::NamedTupleIterator{Schema{names, T}}, st=()) where {names, T}
    if fieldcount(T) <= SPECIALIZATION_THRESHOLD
        return _iterate(rows, st)
    else
        x = iterate(rows.x, st...)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{map(Symbol, names), T}(Tuple(getcolumn(row, fieldtype(T, i), i, names[i]) for i = 1:fieldcount(T))), (st,)
    end
end

function Base.iterate(rows::NamedTupleIterator{Nothing})
    x = iterate(rows.x)
    x === nothing && return nothing
    row, st = x
    names = Tuple(columnnames(row))
    return NamedTuple{names}(Tuple(getcolumn(row, nm) for nm in names)), (Val(names), (st,))
end

function Base.iterate(rows::NamedTupleIterator{Nothing}, state::Tuple{Val{names}, T}) where {names, T}
    x = iterate(rows.x, state[2]...)
    x === nothing && return nothing
    row, st = x
    return NamedTuple{names}(Tuple(getcolumn(row, nm) for nm in names)), (Val(names), (st,))
end

# sink function
"""
    Tables.rowtable(x) => Vector{NamedTuple}

Take any input table source, and produce a `Vector` of `NamedTuple`s,
also known as a "row table". A "row table" is a kind of default
table type of sorts, since it satisfies the Tables.jl row interface
naturally, i.e. a `Vector` naturally iterates its elements, and
`NamedTuple` satisfies the `AbstractRow` interface by default (allows
indexing value by index, name, and getting all names).

For a lazy iterator over rows see [`rows`](@ref) and [`namedtupleiterator`](@ref).

Not for use with extremely wide tables with # of columns > 67K; current fundamental compiler limits
prevent constructing `NamedTuple`s that large.
"""
function rowtable end

function rowtable(itr::T) where {T}
    r = rows(itr)
    return collect(namedtupleiterator(eltype(r), r))
end

# NamedTuple of arrays of matching dimensionality
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector}} where {N}
rowcount(c::ColumnTable) = length(c) == 0 ? 0 : length(c[1])

function subset(x::ColumnTable, inds; viewhint::Union{Bool,Nothing}=nothing, view::Union{Bool,Nothing}=nothing)
    if view !== nothing
        @warn "`view` keyword argument is deprecated for `Tables.subset`, use `viewhint` instead"
        viewhint = view
    end
    if inds isa Integer
        return map(c -> c[inds], x)
    else
        return viewhint === true ? map(c -> vectorcheck(Base.view(c, inds)), x) : map(c -> vectorcheck(c[inds]), x)
    end
end

# interface implementation
istable(::Type{<:ColumnTable}) = true
columnaccess(::Type{<:ColumnTable}) = true
# a NamedTuple of AbstractVectors is itself a `Columns` object
columns(x::ColumnTable) = x

_eltype(::Type{A}) where {T, A <: AbstractVector{T}} = T
Base.@pure function _eltypes(::Type{NT}) where {NT <: ColumnTable}
    return Tuple{Any[ _eltype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
end

names(::Type{NT}) where {nms, T, NT<:NamedTuple{nms, T}} = nms
types(::Type{NT}) where {nms, T, NT<:NamedTuple{nms, T}} = T

schema(x::T) where {T <: ColumnTable} = Schema(names(T), _eltypes(T))
materializer(x::ColumnTable) = columntable

getarray(x::AbstractArray) = x
getarray(x) = collect(x)

"""
    Tables.columntable(x) => NamedTuple of AbstractVectors

Takes any input table source `x` and returns a `NamedTuple` of `AbstractVector`s,
also known as a "column table". A "column table" is a kind of default
table type of sorts, since it satisfies the Tables.jl column interface
naturally.

Note that if `x` is an object in which columns are stored as vectors, the check that
these vectors use 1-based indexing is not performed (it should be ensured when `x` is constructed).

Not for use with extremely wide tables with # of columns > 67K; current fundamental compiler limits
prevent constructing `NamedTuple`s that large.
"""
function columntable end

function _columntable(sch::Schema{names, types}, cols) where {names, types}
    # use of @generated justified because it's user-controlled; they explicitly asked for namedtuple of vectors
    if @generated
        vals = Tuple(:(getarray(getcolumn(cols, $(fieldtype(types, i)), $i, $(quot(names[i]))))) for i = 1:fieldcount(types))
        return :(NamedTuple{map(Symbol, names)}(($(vals...),)))
    else
        return NamedTuple{map(Symbol, names)}(Tuple(getarray(getcolumn(cols, fieldtype(types, i), i, names[i])) for i = 1:fieldcount(types)))
    end
end

function columntable(sch::Schema{names, types}, cols) where {names, types}
    if fieldcount(types) <= SPECIALIZATION_THRESHOLD
        return _columntable(sch, cols)
    else
        return NamedTuple{map(Symbol, names)}(Tuple(getarray(getcolumn(cols, fieldtype(types, i), i, names[i])) for i = 1:fieldcount(types)))
    end
end

# extremely large tables or schema explicitly stored
function columntable(sch::Schema{nothing, nothing}, cols)
    nms = sch.names
    if nms !== nothing && length(nms) > SPECIALIZATION_THRESHOLD
        throw(ArgumentError("input table too wide ($(length(nms)) columns) to convert to `NamedTuple` of `AbstractVector`s"))
    end
    return NamedTuple{Tuple(map(Symbol, nms))}(Tuple(getarray(getcolumn(cols, nms[i])) for i = 1:length(nms)))
end

# unknown schema case
columntable(::Nothing, cols) =
    NamedTuple{Tuple(map(Symbol, columnnames(cols)))}(Tuple(getarray(getcolumn(cols, col)) for col in columnnames(cols)))

function columntable(itr::T) where {T}
    cols = columns(itr)
    cols isa ColumnTable && return cols
    return columntable(schema(cols), cols)
end
columntable(x::ColumnTable) = x

# implement default nrow and ncol methods for DataAPI.jl

DataAPI.nrow(table::ColumnTable) = isempty(table) ? 0 : length(first(table))
DataAPI.ncol(table::ColumnTable) = length(table)

DataAPI.nrow(table::RowTable) = length(table)
DataAPI.ncol(table::RowTable) = isempty(table) ? 0 : length(first(table))
