module Tables

using Requires

export rowtable, columntable

function __init__()
    @require DataValues="e7dc6d0d-1eca-5fa6-8ad6-5aecde8b7ea5" include("datavalues.jl")
    @require QueryOperators="2aef5ad7-51ca-5a8f-8e88-e75cf067b44b" include("enumerable.jl")
    @require CategoricalArrays="324d7699-5711-5eae-9e2f-1d82baa6b597" begin
        using .CategoricalArrays
        allocatecolumn(::Type{CategoricalString{R}}, rows) where {R} = CategoricalArray{String, 1, R}(undef, rows)
        allocatecolumn(::Type{Union{CategoricalString{R}, Missing}}, rows) where {R} = 
            CategoricalArray{Union{String, Missing}, 1, R}(undef, rows)
        allocatecolumn(::Type{CategoricalValue{T, R}}, rows) where {T, R} =
            CategoricalArray{T, 1, R}(undef, rows)
        allocatecolumn(::Type{Union{Missing, CategoricalValue{T, R}}}, rows) where {T, R} =
            CategoricalArray{Union{Missing, T}, 1, R}(undef, rows)
    end
    @require WeakRefStrings="ea10d353-3f73-51f8-a26c-33c1cb351aa5" begin
        using .WeakRefStrings
        allocatecolumn(::Type{WeakRefString{T}}, rows) where {T} = StringVector(rows)
        allocatecolumn(::Type{Union{Missing, WeakRefString{T}}}, rows) where {T} = StringVector{Union{Missing, String}}(rows)
    end
    @require IteratorInterfaceExtensions="82899510-4779-5014-852e-03e436cf321d" begin
        using .IteratorInterfaceExtensions
        IteratorInterfaceExtensions.getiterator(x::RowTable) = datavaluerows(x)
        IteratorInterfaceExtensions.isiterable(x::RowTable) = true
        IteratorInterfaceExtensions.getiterator(x::ColumnTable) = datavaluerows(x)
        IteratorInterfaceExtensions.isiterable(x::ColumnTable) = true
    end
end

"Abstract row type with a simple required interface: row values are accessible via `getproperty(row, field)`; for example, a NamedTuple like `nt = (a=1, b=2, c=3)` can access its value for `a` like `nt.a` which turns into a call to the function `getproperty(nt, :a)`"
abstract type Row end

"""
The Tables.jl package provides simple, yet powerful interface functions for working with all kinds tabular data through predictable access patterns.

```julia
    Tables.rows(table) => Rows
    Tables.columns(table) => Columns
```
Where `Rows` and `Columns` are the duals of each other:
* `Rows` is an iterator of property-accessible objects (any type that supports `propertynames(row)` and `getproperty(row, nm::Symbol`)
* `Columns` is a property-accessible object of iterators (i.e. each column is an iterator)

In addition to these `Rows` and `Columns` objects, it's useful to be able to query properties of these objects:
* `Tables.schema(rows_or_columns)`: returns a `Tables.Schema` object
  * column names can be accessed as a tuple of Symbols like `sch.names`
  * column types can be accessed as a tuple of types like `sch.types`, though note that `sch.types` may return `nothing`, indicating column types are not computable or otherwise unknown

A big part of the power in these simple interface functions is that each (`Tables.rows` & `Tables.columns`) is defined for any table type, even if the table type only explicitly implements one interface function or the other.
This is accomplished by providing performant, generic fallback definitions in Tables.jl itself (though obviously nothing prevents a table type from implementing each interface function directly).

With these simple definitions, powerful workflows are enabled:
* A package providing data cleansing, manipulation, visualization, or analysis can automatically handle any number of decoupled input table types
* A tabular file format can have automatic integration with in-memory structures and translation to other file formats

So how does one go about satisfying the Tables.jl interface functions? It mainly depends on what you've alrady defined and the natural access patterns of your table:

To support `Rows`:
* Define `Tables.rowaccess(::Type{MyTable}) = true`: this signals to other types that `MyTable` supports valid `Row`-iteration
* Define `Tables.rows(x::MyTable)`: return a `Row`-iterator object (perhaps the table itself if already defined)

To support `Columns`:
* Define `Tables.columnaccess(::Type{MyTable}) = true`: this signals to other types that `MyTable` supports returning a valid `Columns` object
* Define `Tables.columns(x::MyTable)`: return a `Columns`, property-accessible object (perhaps the table itself if it naturally supports property-access to columns)

In addition, it can be helpful to define the following:
* `Tables.istable(::Type{MyTable}) = true`: there are runtime checks to see if an object implements the interface, but this can provide an explicit affirmation
* `Tables.schema(rows_or_columns)`: allow users to get the column names & types of `MyTable`'s `Rows` or `Columns` objects as a tuple of Symbols and tuple of types; by default, it will call `propertynames` on the first row or `Columns` object directly for names; column types will be `nothing` by default

The final question is how `MyTable` can be a "sink" for any other table type. The answer is quite simple: use the interface functions!

* Define a function or constructor that takes, at a minimum, a single, untyped argument and then calls `Tables.rows` or `Tables.columns` on that argument to construct an instance of `MyTable`

For example, if `MyTable` is a row-oriented format, I might define my "sink" function like:
```julia
function MyTable(x)
    Tables.istable(x) || throw(ArgumentError("MyTable requires a table input"))
    rows = Tables.rows(x)
    sch = Tables.schema(rows)
    names = sch.names
    types = sch.types
    # custom constructor that creates an "empty" MyTable according to given column names & types
    # note that the "unknown" schema case should be considered, i.e. when `sch.types => nothing`
    mytbl = MyTable(names, types)
    for row in rows
        # a convenience function provided in Tables.jl for "unrolling" access to each column/property of a `Row`
        # it works by applying a provided function to each value; see `?Tables.eachcolumn` for more details
        Tables.eachcolumn(sch, row, mytbl) do val, col, name, mytbl
            push!(mytbl[col], val)
        end
    end
    return mytbl
end
```

Alternatively, if `MyTable` is column-oriented, perhaps my definition would be more like:
```julia
function MyTable(x)
    Tables.istable(x) || throw(ArgumentError("MyTable requires a table input"))
    cols = Tables.columns(x)
    # here we use Tables.eachcolumn to iterate over each column in a `Columns` object
    return MyTable(collect(Tables.names(cols)), [collect(col) for col in Tables.eachcolumn(cols)])
end
```

Obviously every table type is different, but via a combination of `Tables.rows` and `Tables.columns` each table type should be able to construct an instance of itself.
"""
abstract type Table end

# default definitions
rowaccess(x) = false
columnaccess(x) = false

function istable(::Type{T}) where {T}
    if hasmethod(Tables.rowaccess, Tuple{T})
        return hasmethod(Tables.rows, Tuple{T})
    elseif hasmethod(Tables.columnaccess, Tuple{T})
        return hasmethod(Tables.columns, Tuple{T})
    end
    return false
end

# Schema implementation
"""
    Tables.Schema(names, types)

Create a `Tables.Schema` object that holds the column names and types for a tabular data object.
`Tables.Schema` is dual-purposed: provide an easy interface for users to query these properties,
as well as provide a convenient "structural" type for code generation. 

To access the names, one can simply call `sch.names` to return the tuple of Symbols.
To access column types, one can similarly call `sch.types`, which will return either a tuple of types (like `(Int64, Float64, String)`)
or `nothing` if the column types are not computable or otherwise unknown.

The actual type definition is
```julia
struct Schema{names, types} end
```
Where `names` is a tuple of Symbols, and `types` is a tuple _type_ of types (like `Tuple{Int64, Float64, String}`).
Encoding the names & types as type parameters allows convenient use of the type in generated functions
and other optimization use-cases.
"""
struct Schema{names, types} end
Schema(names::Tuple{Vararg{Symbol}}, types::Type{T}) where {T <: Tuple} = Schema{names, T}()
Schema(::Type{NamedTuple{names, types}}) where {names, types} = Schema{names, types}()
Schema(names::Tuple{Vararg{Symbol}}, ::Nothing) = Schema{names, nothing}()
Schema(names, ::Nothing) = Schema{Tuple(map(Symbol, names)), nothing}()
Schema(names, types) = Schema{Tuple(map(Symbol, names)), Tuple{types...}}()

function Base.getproperty(sch::Schema{names, types}, field::Symbol) where {names, types}
    if field === :names
        return names
    elseif field === :types
        return types === nothing ? nothing : Tuple(fieldtype(types, i) for i = 1:fieldcount(types))
    else
        throw(ArgumentError("unsupported property for Tables.Schema"))
    end
end

function schema(x::T) where {T}
    if rowaccess(T)
        return Schema(propertynames(first(x)), nothing)
    elseif columnaccess(T)
        return Schema(propertynames(x), nothing)
    end
    return Schema((), nothing)
end

include("utils.jl")
# reference implementations: Vector of NamedTuples and NamedTuple of Vectors
include("namedtuples.jl")

## generic `Tables.rows` and `Tables.columns` fallbacks
## if a table provides Tables.rows or Tables.columns,
## we'll provide a default implementation of the dual

# generic row iteration of columns
struct ColumnsRow{T}
    columns::T # a `Columns` object
    row::Int
end

Base.getproperty(c::ColumnsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getproperty(getfield(c, 1), T, col, nm)[getfield(c, 2)]
Base.getproperty(c::ColumnsRow, nm::Symbol) = getproperty(getfield(c, 1), nm)[getfield(c, 2)]
Base.propertynames(c::ColumnsRow) = propertynames(c.columns)

struct RowIterator{T}
    columns::T
end
Base.eltype(x::RowIterator{T}) where {T} = ColumnsRow{T}
Base.length(x::RowIterator) = length(getproperty(x.columns, propertynames(x.columns)[1]))
schema(x::RowIterator) = schema(x.columns)

function Base.iterate(rows::RowIterator, st=1)
    st > length(rows) && return nothing
    return ColumnsRow(rows.columns, st), st + 1
end

function rows(x::T) where {T}
    if columnaccess(T)
        return RowIterator(columns(x))
    else
        throw(ArgumentError("no default `Tables.rows` implementation for type: $T"))
    end
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

haslength(T) = T === Base.HasLength() || T === Base.HasShape{1}()

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

@inline function add_or_widen!(val::T, col::Int, nm::Symbol, L, columns, row, allocate, len) where {T}
    if !allocate
        @inbounds columns[col] = add_or_widen!(columns[col], val, L, row)
    else
        @inbounds columns[col] = add_or_widen!(allocatecolumn(T, len), val, L, row)
    end
    return
end

@inline function add_or_widen!(dest::AbstractVector{T}, val::S, ::Union{Base.HasLength, Base.HasShape{1}}, row) where {T, S}
    if S === T || val isa T
        @inbounds dest[row] = val
        return dest
    else
        new = allocatecolumn(Base.promote_typejoin(T, S), length(dest))
        copyto!(new, 1, dest, 1, row - 1)
        @inbounds new[row] = val
        return new
    end
end

@inline function add_or_widen!(dest::AbstractVector{T}, val::S, ::Base.SizeUnknown, row) where {T, S}
    if S === T || val isa T
        push!(dest, val)
        return dest
    else
        new = allocatecolumn(Base.promote_typejoin(T, S), length(dest))
        copyto!(new, dest)
        push!(new, el)
        return new
    end
end

# when Tables.types(x) === nothing
function buildcolumns(sch::Schema{names, nothing}, rowitr::T) where {names, T}
    state = iterate(rowitr)
    state === nothing && return NamedTuple{names}(Tuple(Missing[] for _ in names))
    row::eltype(rowitr), st = state
    cols = length(names)
    L = Base.IteratorSize(T)
    len = haslength(L) ? length(rowitr) : 0
    columns = Vector{AbstractVector}(undef, cols)
    eachcolumn(add_or_widen!, sch, row, L, columns, 1, true, len)
    rownbr = 2
    while true
        state = iterate(rowitr, st)
        state === nothing && break
        row, st = state
        eachcolumn(add_or_widen!, sch, row, L, columns, rownbr, false, len)
        rownbr += 1
    end
    return NamedTuple{names}(Tuple(columns))
end

@inline function columns(x::T) where {T}
    if rowaccess(T)
        r = rows(x)
        return buildcolumns(schema(r), r)
    else
        throw(ArgumentError("no default `Tables.columns` implementation for type: $T"))
    end
end

end # module
