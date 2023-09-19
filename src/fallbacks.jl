## generic `Tables.rows` and `Tables.columns` fallbacks
## if a table provides Tables.rows or Tables.columns,
## we'll provide a default implementation of the other

# Turn any AbstractColumns into an AbstractRow iterator

# get the number of rows in the incoming table
function rowcount(cols)
    names = columnnames(cols)
    isempty(names) && return 0
    return length(getcolumn(cols, names[1]))
end

# a lazy row view into a AbstractColumns object
struct ColumnsRow{T} <: AbstractRow
    columns::T # an `AbstractColumns`-compatible object
    row::Int # row number
end

getcolumns(c::ColumnsRow) = getfield(c, :columns)
getrow(c::ColumnsRow) = getfield(c, :row)

# AbstractRow interface
Base.@propagate_inbounds getcolumn(c::ColumnsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getcolumn(getcolumns(c), T, col, nm)[getrow(c)]
Base.@propagate_inbounds getcolumn(c::ColumnsRow, i::Int) = getcolumn(getcolumns(c), i)[getrow(c)]
Base.@propagate_inbounds getcolumn(c::ColumnsRow, nm::Symbol) = getcolumn(getcolumns(c), nm)[getrow(c)]
columnnames(c::ColumnsRow) = columnnames(getcolumns(c))

@generated function Base.isless(c::ColumnsRow{T}, d::ColumnsRow{T}) where {names, T <: NamedTuple{names}}
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

@generated function Base.isequal(c::ColumnsRow{T}, d::ColumnsRow{T}) where {names, T <: NamedTuple{names}}
    exprs = Expr[]
    for n in names
        var1 = Expr(:., :c, QuoteNode(n))
        var2 = Expr(:., :d, QuoteNode(n))
        push!(exprs, :(isequal($var1, $var2) || return false))
    end
    push!(exprs, :(return true))
    Expr(:block, exprs...)
end

# RowIterator wraps an AbstractColumns object and provides row iteration via lazy row views
struct RowIterator{T}
    columns::T
    len::Int
end

Base.eltype(::Type{R}) where {T,R<:RowIterator{T}} = ColumnsRow{T}
Base.length(x::RowIterator) = getfield(x, :len)
Base.getproperty(x::RowIterator, nm::Symbol) = getcolumn(x, nm)
Base.getproperty(x::RowIterator, i::Int) = getcolumn(x, i)
Base.propertynames(x::RowIterator) = columnnames(x)
isrowtable(::Type{<:RowIterator}) = true

columnaccess(::Type{<:RowIterator}) = true
columns(x::RowIterator) = getfield(x, :columns)
columnnames(x::RowIterator) = columnnames(columns(x))
getcolumn(x::RowIterator, nm::Symbol) = getcolumn(columns(x), nm)
getcolumn(x::RowIterator, i::Int) = getcolumn(columns(x), i)
materializer(x::RowIterator) = materializer(columns(x))
schema(x::RowIterator) = schema(columns(x))

@inline function Base.iterate(rows::RowIterator, st=1)
    st > length(rows) && return nothing
    return ColumnsRow(columns(rows), st), st + 1
end

# this is our generic Tables.rows fallback definition
@noinline nodefault(T) = throw(ArgumentError("no default `Tables.rows` implementation for type: $T"))

rows(x::T) where {T} = _rows(x)

# split out so we can re-use it in the matrix fallback
function _rows(x::T) where {T}
    isrowtable(x) && return x
    # because this method is being called, we know `x` didn't define it's own Tables.rows
    # first check if it supports column access, and if so, wrap it in a RowIterator
    if columnaccess(x)
        cols = columns(x)
        return RowIterator(cols, Int(rowcount(cols)))
    # otherwise, if the input is at least iterable, we'll wrap it in an IteratorWrapper
    # which will iterate the input, validating that elements support the AbstractRow interface
    # and unwrapping any DataValues that are encountered
    elseif IteratorInterfaceExtensions.isiterable(x)
        return nondatavaluerows(x)
    end
    nodefault(T)
end

rows(::Type{T}) where {T} = throw(ArgumentError("no `Tables.rows` implementation for: $T. `Tables.rows` expects to work on a table _instance_ rather than a _type_."))

# for AbstractRow iterators, we define a "collect"-like routine to build up columns from iterated rows

"""
    Tables.allocatecolumn(::Type{T}, len) => returns a column type (usually `AbstractVector`) with size to hold `len` elements

Custom column types can override with an appropriate "scalar" element type that should dispatch to their column allocator.
Alternatively, and more generally, custom scalars can overload `DataAPI.defaultarray` to signal the default array type.
In this case the signaled array type must support a constructor accepting `undef` for initialization.
"""
function allocatecolumn(T, len)
    a = DataAPI.defaultarray(T, 1)(undef, len)
    Missing <: T && fill!(a, missing)
    return a
end

@inline function _allocatecolumns(::Schema{names, types}, len) where {names, types}
    if @generated
        vals = Tuple(:(allocatecolumn($(fieldtype(types, i)), len)) for i = 1:fieldcount(types))
        return :(NamedTuple{$(map(Symbol, names))}(($(vals...),)))
    else
        return NamedTuple{map(Symbol, names)}(Tuple(allocatecolumn(fieldtype(types, i), len) for i = 1:fieldcount(types)))
    end
end

@inline function allocatecolumns(sch::Schema{names, types}, len) where {names, types}
    if fieldcount(types) <= SPECIALIZATION_THRESHOLD
        return _allocatecolumns(sch, len)
    else
        return NamedTuple{map(Symbol, names)}(Tuple(allocatecolumn(fieldtype(types, i), len) for i = 1:fieldcount(types)))
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

@inline function add_or_widen!(val, col::Int, nm, dest::AbstractArray{T}, row, updated, L) where {T}
    if val isa T || promote_type(typeof(val), T) <: T
        add!(dest, val, L, row)
        return
    else
        new = allocatecolumn(promote_type(T, typeof(val)), length(dest))
        row > 1 && copyto!(new, 1, dest, 1, row - 1)
        add!(new, val, L, row)
        updated[] = ntuple(i->i == col ? new : updated[][i], length(updated[]))
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
        # little explanation here: we just called add_or_widen! for each column value of our row
        # note that when a column's type is widened, `updated` is set w/ the new set of columns
        # we then check if our current `columns` isn't the same object as our `updated` ref
        # if it isn't, we're going to call __buildcolumns again, passing our new updated ref as
        # columns, which allows __buildcolumns to specialize (i.e. recompile) based on the new types
        # of updated. So a new __buildcolumns will be compiled for each widening event.
        columns !== updated[] && return __buildcolumns(rowitr, st, sch, updated[], rownbr, updated)
    end
    return updated
end

# for the schema-less case, we do one extra step of initializing each column as an `EmptyVector`
# and doing an initial widening for each column in _buildcolumns, before passing the widened
# set of columns on to __buildcolumns
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

if isdefined(Base, :fieldtypes)
    _fieldtypes = fieldtypes
else
    _fieldtypes(T) = (fieldtype(T, i) for i = 1:fieldcount(T))
end

# when Tables.schema(x) === nothing
@inline function buildcolumns(::Nothing, rowitr::T) where {T}
    state = iterate(rowitr)
    if state === nothing
        # empty input iterator; check if it has eltype and maybe we can return a better typed empty NamedTuple
        if Base.IteratorEltype(rowitr) == Base.HasEltype()
            WT = wrappedtype(eltype(rowitr))
            if WT <: Tuple
                return allocatecolumns(Schema((Symbol("Column$i") for i = 1:fieldcount(WT)), _fieldtypes(WT)), 0)
            elseif isconcretetype(WT) && fieldcount(WT) > 0
                return allocatecolumns(Schema(fieldnames(WT), _fieldtypes(WT)), 0)
            end
        end
        return NamedTuple()
    end
    row, st = state
    names = Tuple(columnnames(row))
    len = Base.haslength(T) ? length(rowitr) : 0
    sch = Schema(names, nothing)
    columns = Tuple(EmptyVector(len) for _ = 1:length(names))
    return NamedTuple{map(Symbol, names)}(_buildcolumns(rowitr, row, st, sch, columns, Ref{Any}(columns))[])
end

"""
    Tables.CopiedColumns

For some sinks, there's a concern about whether they can safely "own" columns from the input.
If mutation will be allowed, to be safe, they should always copy input columns, to avoid unintended mutation
to the original source.
When we've called `buildcolumns`, however, Tables.jl essentially built/owns the columns,
and it's happy to pass ownership to the sink. Thus, any built columns will be wrapped
in a `CopiedColumns` struct to signal to the sink that essentially "a copy has already been made"
and they're safe to assume ownership.
"""
struct CopiedColumns{T} <: AbstractColumns
    x::T
end

source(x::CopiedColumns) = getfield(x, :x)
istable(::Type{<:CopiedColumns}) = true
columnaccess(::Type{<:CopiedColumns}) = true
columns(x::CopiedColumns) = x
schema(x::CopiedColumns) = schema(source(x))
materializer(x::CopiedColumns) = materializer(source(x))

getcolumn(x::CopiedColumns, ::Type{T}, col::Int, nm::Symbol) where {T} = getcolumn(source(x), T, col, nm)
getcolumn(x::CopiedColumns, i::Int) = getcolumn(source(x), i)
getcolumn(x::CopiedColumns, nm::Symbol) = getcolumn(source(x), nm)
columnnames(x::CopiedColumns) = columnnames(source(x))

# here's our generic fallback Tables.columns definition
@inline columns(x::T) where {T} = _columns(x)

@inline function _columns(x::T) where {T}
    # because this method is being called, we know `x` didn't define it's own Tables.columns method
    # first check if it explicitly supports row access, and if so, build up the desired columns
    if rowaccess(x)
        r = rows(x)
        return CopiedColumns(buildcolumns(schema(r), r))
    # though not widely supported, if a source supports the TableTraits column interface, use it
    elseif TableTraits.supports_get_columns_copy_using_missing(x)
        return CopiedColumns(TableTraits.get_columns_copy_using_missing(x))
    # otherwise, if the source is at least iterable, we'll wrap it in an IteratorWrapper and
    # build columns from that, which will check if the source correctly iterates valid AbstractRow objects
    # and unwraps DataValues for us
    elseif IteratorInterfaceExtensions.isiterable(x)
        iw = nondatavaluerows(x)
        return CopiedColumns(buildcolumns(schema(iw), iw))
    end
    throw(ArgumentError("no default `Tables.columns` implementation for type: $T"))
end

# implement default nrow and ncol methods for DataAPI.jl

# this covers also MatrixTable
DataAPI.nrow(table::AbstractColumns) = rowcount(table)
DataAPI.ncol(table::AbstractColumns) = length(columnnames(table))

DataAPI.nrow(table::AbstractRowTable) = length(table)
DataAPI.ncol(table::AbstractRowTable) = isempty(table) ? 0 : length(columnnames(first(table)))
