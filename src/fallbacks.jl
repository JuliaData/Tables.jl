## generic `Tables.rows` and `Tables.columns` fallbacks
## if a table provides Tables.rows or Tables.columns,
## we'll provide a default implementation of the dual

# generic row iteration of columns
rowcount(cols) = length(getproperty(cols, propertynames(cols)[1]))

struct ColumnsRow{T}
    columns::T # a `Columns` object
    row::Int
end

Base.getproperty(c::ColumnsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getproperty(getfield(c, 1), T, col, nm)[getfield(c, 2)]
Base.getproperty(c::ColumnsRow, nm::Symbol) = getproperty(getfield(c, 1), nm)[getfield(c, 2)]
Base.propertynames(c::ColumnsRow) = propertynames(getfield(c, 1))

struct RowIterator{T}
    columns::T
    len::Int
end
Base.eltype(x::RowIterator{T}) where {T} = ColumnsRow{T}
Base.length(x::RowIterator) = x.len
schema(x::RowIterator) = schema(x.columns)

function Base.iterate(rows::RowIterator, st=1)
    st > length(rows) && return nothing
    return ColumnsRow(rows.columns, st), st + 1
end

function rows(x::T) where {T}
    if columnaccess(T)
        cols = columns(x)
        return RowIterator(cols, rowcount(cols))
    else
        throw(ArgumentError("no default `Tables.rows` implementation for type: $T"))
    end
end

# build columns from rows
haslength(L) = L isa Union{Base.HasShape, Base.HasLength}

"""
    Tables.allocatecolumn(::Type{T}, len) => returns a column type (usually AbstractVector) w/ size to hold `len` elements
    
    Custom column types can override with an appropriate "scalar" element type that should dispatch to their column allocator.
"""
allocatecolumn(T, len) = Vector{T}(undef, len)

@inline function allocatecolumns(::Schema{names, types}, len) where {names, types}
    if @generated
        vals = Tuple(:(allocatecolumn($(fieldtype(types, i)), len)) for i = 1:fieldcount(types))
        return :(NamedTuple{names}(($(vals...),)))
    else
        return NamedTuple{names}(Tuple(allocatecolumn(fieldtype(types, i), len) for i = 1:fieldcount(types)))
    end
end

# add! will push! or setindex! a value depending on if the row-iterator HasLength or not
@inline add!(val, col::Int, nm::Symbol, ::Union{Base.HasLength, Base.HasShape{1}}, nt, row) = setindex!(nt[col], val, row)
@inline add!(val, col::Int, nm::Symbol, T, nt, row) = push!(nt[col], val)

@inline function buildcolumns(schema, rowitr::T) where {T}
    L = Base.IteratorSize(T)
    len = haslength(L) ? length(rowitr) : 0
    nt = allocatecolumns(schema, len)
    for (i, row) in enumerate(rowitr)
        eachcolumn(add!, schema, row, L, nt, i)
    end
    return nt
end

@inline add!(dest::AbstractVector, val, ::Union{Base.HasLength, Base.HasShape{1}}, row) = setindex!(dest, val, row)
@inline add!(dest::AbstractVector, val, T, row) = push!(dest, val)

@inline function add_or_widen!(dest::AbstractVector{T}, val::S, nm::Symbol, L, row, len, updated) where {T, S}
    if S === T || promote_type(S, T) <: T
        add!(dest, val, L, row)
        return dest
    else
        new = allocatecolumn(promote_type(T, S), max(len, length(dest)))
        row > 1 && copyto!(new, 1, dest, 1, row - 1)
        add!(new, val, L, row)
        updated[] = merge(updated[], NamedTuple{(nm,)}((new,)))
        return new
    end
end

@inline function add_or_widen!(val, col, nm, L, columns, row, len, updated)
    @inbounds add_or_widen!(columns[col], val, nm, L, row, len, updated)
    return
end

# when Tables.schema(x) === nothing
function buildcolumns(::Nothing, rowitr::T) where {T}
    state = iterate(rowitr)
    state === nothing && return NamedTuple()
    row, st = state
    names = Tuple(propertynames(row))
    L = Base.IteratorSize(T)
    len = haslength(L) ? length(rowitr) : 0
    sch = Schema(names, nothing)
    columns = NamedTuple{names}(Tuple(Union{}[] for _ = 1:length(names)))
    return _buildcolumns(rowitr, row, st, sch, L, columns, 1, len, Ref{Any}(columns))
end

function _buildcolumns(rowitr, row, st, sch, L, columns, rownbr, len, updated)
    while true
        eachcolumn(add_or_widen!, sch, row, L, columns, rownbr, len, updated)
        rownbr += 1
        state = iterate(rowitr, st)
        state === nothing && break
        row, st = state
        columns !== updated[] && return _buildcolumns(rowitr, row, st, sch, L, updated[], rownbr, len, updated)
    end
    return updated[]
end

@inline function columns(x::T) where {T}
    if rowaccess(T)
        r = rows(x)
        return buildcolumns(schema(r), r)
    else
        throw(ArgumentError("no default `Tables.columns` implementation for type: $T"))
    end
end
