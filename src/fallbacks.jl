## generic `Tables.rows` and `Tables.columns` fallbacks
## if a table provides Tables.rows or Tables.columns,
## we'll provide a default implementation of the dual

# generic row iteration of columns
function rowcount(cols)
    props = propertynames(cols)
    isempty(props) && return 0
    return length(getproperty(cols, props[1]))
end

struct ColumnsRow{T}
    columns::T # a `Columns` object
    row::Int
end

Base.getproperty(c::ColumnsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getproperty(getfield(c, 1), T, col, nm)[getfield(c, 2)]
Base.getproperty(c::ColumnsRow, nm::Int) = getproperty(getfield(c, 1), nm)[getfield(c, 2)]
Base.getproperty(c::ColumnsRow, nm::Symbol) = getproperty(getfield(c, 1), nm)[getfield(c, 2)]
Base.propertynames(c::ColumnsRow) = propertynames(getfield(c, 1))

@generated function Base.isless(c::ColumnsRow{T}, d::ColumnsRow{T}) where {T <: NamedTuple{names}} where names
    exprs = Expr[]
    for n in names
        var1 = Expr(:., :c, QuoteNode(n))
        var2 = Expr(:., :d, QuoteNode(n))
        bl = quote
            a, b = $var1, $var2
            isless(a, b) && return true
            isequal(a, b) || return false
        end
        push!(exprs, bl)
    end
    push!(exprs, :(return false))
    Expr(:block, exprs...)
end

@generated function Base.isequal(c::ColumnsRow{T}, d::ColumnsRow{T}) where {T <: NamedTuple{names}} where names
    exprs = Expr[]
    for n in names
        var1 = Expr(:., :c, QuoteNode(n))
        var2 = Expr(:., :d, QuoteNode(n))
        push!(exprs, :(isequal($var1, $var2) || return false))
    end
    push!(exprs, :(return true))
    Expr(:block, exprs...)
end

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
    elseif IteratorInterfaceExtensions.isiterable(x)
        return nondatavaluerows(x)
    end
    throw(ArgumentError("no default `Tables.rows` implementation for type: $T"))
end

# build columns from rows
"""
    Tables.allocatecolumn(::Type{T}, len) => returns a column type (usually AbstractVector) w/ size to hold `len` elements

    Custom column types can override with an appropriate "scalar" element type that should dispatch to their column allocator.
"""
allocatecolumn(T, len) = DataAPI.defaultarray(T, 1)(undef, len)

@inline function allocatecolumns(::Schema{names, types}, len) where {names, types}
    if @generated
        vals = Tuple(:(allocatecolumn($(fieldtype(types, i)), len)) for i = 1:fieldcount(types))
        return :(NamedTuple{Base.map(Symbol, names)}(($(vals...),)))
    else
        return NamedTuple{Base.map(Symbol, names)}(Tuple(allocatecolumn(fieldtype(types, i), len) for i = 1:fieldcount(types)))
    end
end

# add! will push! or setindex! a value depending on if the row-iterator HasLength or not
@inline add!(val, col::Int, nm, dest::AbstractArray, ::Union{Base.HasLength, Base.HasShape}, row) = setindex!(dest, val, row)
@inline add!(val, col::Int, nm, dest::AbstractArray, L, row) = push!(dest, val)

@inline function buildcolumns(schema, rowitr::T) where {T}
    L = Base.IteratorSize(T)
    len = Base.haslength(T) ? length(rowitr) : 0
    nt = allocatecolumns(schema, len)
    for (i, row) in enumerate(rowitr)
        eachcolumns(add!, schema, row, nt, L, i)
    end
    return nt
end

@inline add!(dest::AbstractArray, val, ::Union{Base.HasLength, Base.HasShape}, row) = setindex!(dest, val, row)
@inline add!(dest::AbstractArray, val, T, row) = push!(dest, val)

replacex(t, col::Int, x) = ntuple(i->i == col ? x : t[i], length(t))

@inline function add_or_widen!(val, col::Int, nm, dest::AbstractArray{T}, row, updated, L) where {T}
    if val isa T
        add!(dest, val, L, row)
        return
    else
        new = allocatecolumn(promote_type(T, typeof(val)), length(dest))
        row > 1 && copyto!(new, 1, dest, 1, row - 1)
        add!(new, val, L, row)
        updated[] = replacex(updated[], col, new)
        return
    end
end

function __buildcolumns(rowitr, st, sch, columns, rownbr, updated)
    while true
        state = iterate(rowitr, st)
        state === nothing && break
        row, st = state
        rownbr += 1
        eachcolumns(add_or_widen!, sch, row, columns, rownbr, updated, Base.IteratorSize(rowitr))
        columns !== updated[] && return __buildcolumns(rowitr, st, sch, updated[], rownbr, updated)
    end
    return updated
end

struct EmptyVector <: AbstractVector{Union{}}
    len::Int
end
Base.IndexStyle(::Type{EmptyVector}) = Base.IndexLinear()
Base.size(x::EmptyVector) = (x.len,)
Base.getindex(x::EmptyVector, i::Int) = throw(UndefRefError())

function _buildcolumns(rowitr, row, st, sch, columns, updated)
    eachcolumns(add_or_widen!, sch, row, columns, 1, updated, Base.IteratorSize(rowitr))
    return __buildcolumns(rowitr, st, sch, updated[], 1, updated)
end

# when Tables.schema(x) === nothing
@inline function buildcolumns(::Nothing, rowitr::T) where {T}
    state = iterate(rowitr)
    state === nothing && return NamedTuple()
    row, st = state
    names = Tuple(propertynames(row))
    len = Base.haslength(T) ? length(rowitr) : 0
    sch = Schema(names, nothing)
    columns = Tuple(EmptyVector(len) for _ = 1:length(names))
    return NamedTuple{Base.map(Symbol, names)}(_buildcolumns(rowitr, row, st, sch, columns, Ref{Any}(columns))[])
end

struct CopiedColumns{T}
    x::T
end

source(x::CopiedColumns) = getfield(x, :x)
istable(::Type{<:CopiedColumns}) = true
columnaccess(::Type{<:CopiedColumns}) = true
columns(x::CopiedColumns) = x
schema(x::CopiedColumns) = schema(source(x))
materializer(x::CopiedColumns) = materializer(source(x))
Base.propertynames(x::CopiedColumns) = propertynames(source(x))
Base.getproperty(x::CopiedColumns, nm::Symbol) = getproperty(source(x), nm)

@inline function columns(x::T) where {T}
    if rowaccess(T)
        r = rows(x)
        return CopiedColumns(buildcolumns(schema(r), r))
    elseif TableTraits.supports_get_columns_copy_using_missing(x)
        return CopiedColumns(TableTraits.get_columns_copy_using_missing(x))
    elseif IteratorInterfaceExtensions.isiterable(x)
        iw = nondatavaluerows(x)
        return CopiedColumns(buildcolumns(schema(iw), iw))
    end
    throw(ArgumentError("no default `Tables.columns` implementation for type: $T"))
end
