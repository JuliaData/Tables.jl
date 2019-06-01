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
@inline add!(val, col::Int, nm::Symbol, ::Union{Base.HasLength, Base.HasShape}, nt, row) = setindex!(nt[col], val, row)
@inline add!(val, col::Int, nm::Symbol, T, nt, row) = push!(nt[col], val)

@inline function buildcolumns(schema, rowitr::T) where {T}
    L = Base.IteratorSize(T)
    len = Base.haslength(T) ? length(rowitr) : 0
    nt = allocatecolumns(schema, len)
    for (i, row) in enumerate(rowitr)
        eachcolumn(add!, schema, row, L, nt, i)
    end
    return nt
end

@inline add!(dest::AbstractArray, val, ::Union{Base.HasLength, Base.HasShape}, row) = setindex!(dest, val, row)
@inline add!(dest::AbstractArray, val, T, row) = push!(dest, val)

function replacex(nt::NamedTuple{names, types}, nm::Symbol, x::AbstractArray{T}) where {names, types, T}
    ind = columnindex(names, nm)
    return NamedTuple{names}(ntuple(i->i == ind ? x : nt[i], length(names)))
end

@inline function add_or_widen!(dest::AbstractArray{T}, val::S, nm::Symbol, row, updated, L) where {T, S}
    if S === T || promote_type(S, T) <: T
        add!(dest, val, L, row)
        return dest
    else
        new = allocatecolumn(promote_type(T, S), length(dest))
        row > 1 && copyto!(new, 1, dest, 1, row - 1)
        add!(new, val, L, row)
        updated[] = replacex(updated[], nm, new)
        return new
    end
end

function _buildcolumns(rowitr, row, st, sch, columns, rownbr, updated)
    while true
        eachcolumn(sch, row, rownbr, columns, updated, Base.IteratorSize(rowitr)) do val, col, nm, rownbr, columns, updated, L
            Base.@_inline_meta
            add_or_widen!(columns[col], val, nm, rownbr, updated, L)
        end
        rownbr += 1
        state = iterate(rowitr, st)
        state === nothing && break
        row, st = state
        columns !== updated[] && return _buildcolumns(rowitr, row, st, sch, updated[], rownbr, updated)
    end
    return updated
end

struct EmptyVector <: AbstractVector{Union{}}
    len::Int
end
Base.IndexStyle(::Type{EmptyVector}) = Base.IndexLinear()
Base.size(x::EmptyVector) = (x.len,)
Base.getindex(x::EmptyVector, i::Int) = throw(UndefRefError())

# when Tables.schema(x) === nothing
function buildcolumns(::Nothing, rowitr::T) where {T}
    state = iterate(rowitr)
    state === nothing && return NamedTuple()
    row, st = state
    names = Tuple(propertynames(row))
    len = Base.haslength(T) ? length(rowitr) : 0
    sch = Schema(names, nothing)
    columns = NamedTuple{names}(Tuple(EmptyVector(len) for _ = 1:length(names)))
    return _buildcolumns(rowitr, row, st, sch, columns, 1, Ref{Any}(columns))[]
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
