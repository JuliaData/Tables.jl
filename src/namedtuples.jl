# Vector of NamedTuples
const RowTable{T} = AbstractVector{T} where {T <: NamedTuple}

# interface implementation
istable(::Type{<:RowTable}) = true
rowaccess(::Type{<:RowTable}) = true
# an AbstractVector of NamedTuple iterates `Row`s itself
rows(x::RowTable) = x
schema(x::AbstractVector{NamedTuple{names, types}}) where {names, types} = Schema(names, types)
materializer(x::RowTable) = rowtable

# struct to transform `Row`s into NamedTuples
struct NamedTupleIterator{schema, T, S}
    x::T
    st::S
end

"""
    Tables.namedtupleiterator(x)

Pass any table input source and return a `NamedTuple` iterator
"""
function namedtupleiterator(x)
    r = rows(x)
    sch = schema(r)
    st = iterate(r)
    if st === nothing
        # input was empty
        return NamedTupleIterator{Schema((), ()), typeof(r), typeof(st)}(r, st)
    end
    row, state = st
    if sch === nothing
        s = Schema(columnnames(row), nothing)
    else
        s = sch
    end
    return NamedTupleIterator{typeof(s), typeof(r), typeof(st)}(r, st)
end

namedtupleiterator(::Type{T}, x) where {T <: NamedTuple} = x
namedtupleiterator(T, x) = namedtupleiterator(x)

Base.IteratorEltype(::Type{NamedTupleIterator{Schema{names, types}, T, S}}) where {names, types, T, S} = Base.HasEltype()
Base.eltype(::Type{NamedTupleIterator{Schema{names, types}, T, S}}) where {names, types, T, S} = types === nothing ? NamedTuple{Base.map(Symbol, names)} : NamedTuple{Base.map(Symbol, names), types}
Base.IteratorSize(::Type{NamedTupleIterator{sch, T, S}}) where {sch, T, S} = Base.IteratorSize(T)
Base.length(nt::NamedTupleIterator) = length(nt.x)
Base.size(nt::NamedTupleIterator) = (length(nt.x),)

@inline function Base.iterate(rows::NamedTupleIterator{Schema{names, T}, T1, T2}) where {names, T, T1, T2}
    if @generated
        vals = Tuple(:(getcolumn(row, $(fieldtype(T, i)), $i, $(quot(names[i])))) for i = 1:fieldcount(T))
        return quote
            Base.@inline_meta
            rows.st === nothing && return nothing
            row, st = rows.st
            return $(NamedTuple{Base.map(Symbol, names), T})(($(vals...),)), st
        end
    else
        rows.st === nothing && return nothing
        row, st = rows.st
        return NamedTuple{Base.map(Symbol, names), T}(Tuple(getcolumn(row, fieldtype(T, i), i, names[i]) for i = 1:fieldcount(T))), st
    end
end

@inline function Base.iterate(rows::NamedTupleIterator{Schema{names, T}}, st) where {names, T}
    if @generated
        vals = Tuple(:(getcolumn(row, $(fieldtype(T, i)), $i, $(quot(names[i])))) for i = 1:fieldcount(T))
        return quote
            Base.@inline_meta
            x = iterate(rows.x, st)
            x === nothing && return nothing
            row, st = x
            return $(NamedTuple{Base.map(Symbol, names), T})(($(vals...),)), st
        end
    else
        x = iterate(rows.x, st)
        x === nothing && return nothing
        row, st = x
        return NamedTuple{Base.map(Symbol, names), T}(Tuple(getcolumn(row, fieldtype(T, i), i, names[i]) for i = 1:fieldcount(T))), st
    end
end

@inline function Base.iterate(rows::NamedTupleIterator{Schema{names, nothing}}) where {names}
    rows.st === nothing && return nothing
    row, st = rows.st
    return NamedTuple{names}(Tuple(getcolumn(row, nm) for nm in names)), st
end

@inline function Base.iterate(rows::NamedTupleIterator{Schema{names, nothing}}, st) where {names}
    x = iterate(rows.x, st)
    x === nothing && return nothing
    row, st = x
    return NamedTuple{names}(Tuple(getcolumn(row, nm) for nm in names)), st
end

# sink function
"""
    Tables.rowtable(x) => Vector{NamedTuple}
    Tables.rowtable(rt, x) => rt

Take any input table source, and produce a `Vector` of `NamedTuple`s,
also known as a "row table". A "row table" is a kind of default
table type of sorts, since it satisfies the Tables.jl row interface
naturally.

The 2nd definition takes
an existing row table and appends the input table source `x`
to the existing row table.
"""
function rowtable end

function rowtable(itr::T) where {T}
    r = rows(itr)
    return collect(namedtupleiterator(eltype(r), r))
end

function rowtable(rt::RowTable, itr::T) where {T}
    r = rows(itr)
    return append!(rt, namedtupleiterator(eltype(r), r))
end

# NamedTuple of arrays of matching dimensionality
const ColumnTable = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractArray{S, D} where S}} where {N, D}
rowcount(c::ColumnTable) = length(c) == 0 ? 0 : length(c[1])

# interface implementation
istable(::Type{<:ColumnTable}) = true
columnaccess(::Type{<:ColumnTable}) = true
# a NamedTuple of AbstractVectors is itself a `Columns` object
columns(x::ColumnTable) = x

_eltype(::Type{A}) where {A <: AbstractVector{T}} where {T} = T
Base.@pure function _eltypes(::Type{NT}) where {NT <: NamedTuple{names, T}} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}
    return Tuple{Any[ _eltype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
end

schema(x::T) where {T <: ColumnTable} = Schema(names(T), _eltypes(T))
materializer(x::ColumnTable) = columntable

getarray(x::AbstractArray) = x
getarray(x) = collect(x)

"""
    Tables.columntable(x) => NamedTuple of Vectors
    Tables.columntable(ct, x) => ct

Takes any input table source `x` and returns a `NamedTuple` of `Vector`s,
also known as a "column table". A "column table" is a kind of default
table type of sorts, since it satisfies the Tables.jl column interface
naturally.

The 2nd definition takes an input table source `x` and appends it to an
existing column table `ct`.
"""
function columntable end

function columntable(sch::Schema{names, types}, cols) where {names, types}
    if @generated
        vals = Tuple(:(getarray(getcolumn(cols, $(fieldtype(types, i)), $i, $(quot(names[i]))))) for i = 1:fieldcount(types))
        return :(NamedTuple{Base.map(Symbol, names)}(($(vals...),)))
    else
        return NamedTuple{Base.map(Symbol, names)}(Tuple(getarray(getcolumn(cols, fieldtype(types, i), i, names[i])) for i = 1:fieldcount(types)))
    end
end

# unknown schema case
columntable(::Nothing, cols) = NamedTuple{Tuple(Base.map(Symbol, columnnames(cols)))}(Tuple(getarray(col) for col in eachcolumn(cols)))

function columntable(itr::T) where {T}
    cols = columns(itr)
    cols isa ColumnTable && return cols
    return columntable(schema(cols), cols)
end
columntable(x::ColumnTable) = x

function ctappend(ct1::NamedTuple{N1, T1}, ct2::NamedTuple{N2, T2}) where {N1, T1, N2, T2}
    if @generated
        appends = Expr(:block, Any[:(append!(ct1[$(quot(nm))], ct2[$(quot(nm))])) for nm in N1]...)
        return quote
            $appends
            return ct1
        end
    else
        foreach(nm->append!(ct1[nm], ct2[nm]), N1)
        return ct1
    end
end

columntable(ct::ColumnTable, itr) = ctappend(ct, columntable(itr))
