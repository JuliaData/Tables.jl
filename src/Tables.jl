module Tables

using LinearAlgebra, DataValueInterfaces, DataAPI, TableTraits, IteratorInterfaceExtensions

export rowtable, columntable

if !hasmethod(getproperty, Tuple{Tuple, Int})
    Base.getproperty(t::Tuple, i::Int) = t[i]
end

"""
    Tables.AbstractColumns

Abstract type provided to allow custom table types to inherit useful and required behavior.

Interface definition:
| Required Methods | Default Definition | Brief Description |
| ---------------- | ------------------ | ----------------- |
| `Tables.getcolumn(table, i::Int)` | getfield(table, i) | Retrieve a column by index |
| `Tables.getcolumn(table, nm::Symbol)` | getproperty(table, nm) | Retrieve a column by name |
| `Tables.columnnames(table)` | propertynames(table) | Return column names for a table as an indexable collection |
| Optional methods | | |
| `Tables.getcolumn(table, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(table, nm) | Given a column eltype `T`, index `i`, and column name `nm`, retrieve the column. Provides a type-stable or even constant-prop-able mechanism for efficiency.

While custom table types aren't required to subtype `Tables.AbstractColumns`, benefits of doing so include:
  * Indexing interface defined (using `getcolumn`); i.e. `tbl[i]` will retrieve the column at index `i`
  * Property access interface defined (using `columnnames` and `getcolumn`); i.e. `tbl.col1` will retrieve column named `col1`
  * Iteration interface defined; i.e. `for col in table` will iterate each column in the table
  * A default `show` method
This allows a custom table type to behave as close as possible to a builtin `NamedTuple` of vectors object.
"""
abstract type AbstractColumns end

"""
    Tables.AbstractRow

Abstract type provided to allow custom row types to inherit useful and required behavior.

Interface definition:
| Required Methods | Default Definition | Brief Description |
| ---------------- | ------------------ | ----------------- |
| `Tables.getcolumn(row, i::Int)` | getfield(row, i) | Retrieve a column value by index |
| `Tables.getcolumn(row, nm::Symbol)` | getproperty(row, nm) | Retrieve a column value by name |
| `Tables.columnnames(row)` | propertynames(row) | Return column names for a row as an indexable collection |
| Optional methods | | |
| `Tables.getcolumn(row, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(row, nm) | Given a column type `T`, index `i`, and column name `nm`, retrieve the column value. Provides a type-stable or even constant-prop-able mechanism for efficiency.

While custom row types aren't required to subtype `Tables.AbstractRow`, benefits of doing so include:
  * Indexing interface defined (using `getcolumn`); i.e. `row[i]` will return the column value at index `i`
  * Property access interface defined (using `columnnames` and `getcolumn`); i.e. `row.col1` will retrieve the value for the column named `col1`
  * Iteration interface defined; i.e. `for x in row` will iterate each column value in the row
  * A default `show` method
This allows the custom row type to behave as close as possible to a builtin `NamedTuple` object.
"""
abstract type AbstractRow <: AbstractColumns end

"""
    Tables.getcolumn(::Columns, nm::Symbol) => Indexable collection with known length
    Tables.getcolumn(::Columns, i::Int) => Indexable collection with known length
    Tables.getcolumn(::Columns, T, i::Int, nm::Symbol) => Indexable collection with known length

    Tables.getcolumn(::Row, nm::Symbol) => Column value
    Tables.getcolumn(::Row, i::Int) => Column value
    Tables.getcolumn(::Row, T, i::Int, nm::Symbol) => Column value

Retrieve an entire column (`Columns`) or single row column value (`Row`) by column name (`nm`), index (`i`),
or if desired, by column type (`T`), index (`i`), and name (`nm`). When called on a `Columns` interface object,
a `Column` is returned, which is an indexable collection with known length. When called on a `Row` interface
object, it returns the single column value. The methods taking a single `Symbol` or `Int` are both required
for the `AbstractColumns` and `AbstractRow` interfaces; the third method is optional if type stability is possible.
The default definition of `Tables.getcolumn(x, i::Int)` is `getfield(x, i)`. The default definition of
`Tables.getcolumn(x, nm::Symbol)` is `getproperty(x, nm)`.
"""
function getcolumn end

getcolumn(x, i::Int) = getfield(x, i)
getcolumn(x, nm::Symbol) = getproperty(x, nm)
getcolumn(x, ::Type{T}, i::Int, nm::Symbol) where {T} = getcolumn(x, nm)
getcolumn(x::NamedTuple{names, types}, ::Type{T}, i::Int, nm::Symbol) where {names, types, T} = Core.getfield(x, i)

"""
    Tables.columnnames(::Union{Columns, Row}) => Indexable collection

Retrieves the list of column names as an indexable collection (like a `Tuple` or `Vector`) for a `Columns` or `Row` interface object. The default definition calls `propertynames(x)`.
"""
function columnnames end

columnnames(x) = propertynames(x)

Base.IteratorSize(::Type{R}) where {R <: AbstractColumns} = Base.HasLength()
Base.length(r::AbstractColumns) = length(columnnames(r))
Base.firstindex(r::AbstractColumns) = 1
Base.lastindex(r::AbstractColumns) = length(r)
Base.getindex(r::AbstractColumns, i::Int) = getcolumn(r, i)
Base.getindex(r::AbstractColumns, nm::Symbol) = getcolumn(r, nm)
Base.getproperty(r::AbstractColumns, nm::Symbol) = getcolumn(r, nm)
Base.getproperty(r::AbstractColumns, i::Int) = getcolumn(r, i)
Base.propertynames(r::AbstractColumns) = columnnames(r)
Base.keys(r::AbstractColumns) = columnnames(r)
Base.values(r::AbstractColumns) = collect(r)
Base.haskey(r::AbstractColumns, key::Union{Integer, Symbol}) = key in columnnames(r)
Base.get(r::AbstractColumns, key::Union{Integer, Symbol}, default) = haskey(r, key) ? getcolumn(r, key) : default
Base.get(f::Base.Callable, r::AbstractColumns, key::Union{Integer, Symbol}) = haskey(r, key) ? getcolumn(r, key) : f()
Base.@propagate_inbounds Base.iterate(r::AbstractColumns, i=1) = i > length(r) ? nothing : (getcolumn(r, i), i + 1)

function Base.show(io::IO, x::T) where {T <: AbstractColumns}
    println(io, "$T:")
    names = collect(columnnames(x))
    values = [getcolumn(row, nm) for nm in names]
    Base.print_matrix(io, hcat(names, values))
end

"""
The Tables.jl package provides simple, yet powerful interface functions for working with all kinds of tabular data through predictable access patterns.

```julia
    Tables.rows(table) => Row iterator (also known as a Rows object)
    Tables.columns(table) => Columns
```
Where `Row` and `Columns` are objects that support a common interface:
  * `Tables.getcolumn(x, col::Union{Int, Symbol})`: Retrieve an entire column (`Columns`), or single column value (`Row`) by column index (as an `Int`), or by column name (as a `Symbol`)
  * `Tables.columnnames(x)`: Retrieve the possible column names for a `Row` or `Columns` object

In addition to these `Row` and `Columns` objects, it's useful to be able to query properties of these objects:
* `Tables.schema(x::Union{Rows, Columns}) => Union{Tables.Schema, Nothing}`: returns a `Tables.Schema` object, or `nothing` if the table's schema is unknown
* For the `Tables.Schema` object:
  * column names can be accessed as a tuple of Symbols like `sch.names`
  * column types can be accessed as a tuple of types like `sch.types`
  * See `?Tables.Schema` for more details on this type

A big part of the power in these simple interface functions is that each (`Tables.rows` & `Tables.columns`) is defined for any table type, even if the table type only explicitly implements one interface function or the other.
This is accomplished by providing performant, generic fallback definitions in Tables.jl itself (though obviously nothing prevents a table type from implementing each interface function directly if so desired).

With these simple definitions, powerful workflows are enabled:
* A package providing data cleansing, manipulation, visualization, or analysis can automatically handle any number of decoupled input table types
* A tabular file format can have automatic integration with in-memory structures and translation to other file formats

So how does one go about satisfying the Tables.jl interface functions? It mainly depends on what you've already defined and the natural access patterns of your table:

First:
* `Tables.istable(::Type{<:MyTable}) = true`: this provides an explicit affirmation that your type implements the Tables interface

To support `Rows`:
* Define `Tables.rowaccess(::Type{<:MyTable}) = true`: this signals to other types that `MyTable` supports valid `Row`-iteration
* Define `Tables.rows(x::MyTable)`: return a `Row`-iterator object (perhaps the table itself if already defined)
* Define `Tables.schema(Tables.rows(x::MyTable))` to either return a `Tables.Schema` object, or `nothing` if the schema is unknown or non-inferrable for some reason

To support `Columns`:
* Define `Tables.columnaccess(::Type{<:MyTable}) = true`: this signals to other types that `MyTable` supports returning a valid `Columns` object
* Define `Tables.columns(x::MyTable)`: return a `Columns`, property-accessible object (perhaps the table itself if it naturally supports property-access to columns)
* Define `Tables.schema(Tables.columns(x::MyTable))` to either return a `Tables.Schema` object, or `nothing` if the schema is unknown or non-inferrable for some reason

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
        Tables.eachcolumn(sch, row) do val, col, name
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
    return MyTable(collect(propertynames(cols)), [collect(col) for col in Tables.eachcolumn(cols)])
end
```

Obviously every table type is different, but via a combination of `Tables.rows` and `Tables.columns` each table type should be able to construct an instance of itself.
"""
abstract type Table end

# default definitions
istable(x::T) where {T} = istable(T) || TableTraits.isiterabletable(x) === true
istable(::Type{T}) where {T} = false
rowaccess(x::T) where {T} = rowaccess(T)
rowaccess(::Type{T}) where {T} = false
columnaccess(x::T) where {T} = columnaccess(T)
columnaccess(::Type{T}) where {T} = false
schema(x) = nothing
materializer(x) = columntable

# Schema implementation
"""
    Tables.Schema(names, types)

Create a `Tables.Schema` object that holds the column names and types for a tabular data object.
`Tables.Schema` is dual-purposed: provide an easy interface for users to query these properties,
as well as provide a convenient "structural" type for code generation.

To get a table's schema, one can call `Tables.schema(tbl)`, but also note that a table may return `nothing`,
indicating that it's column names and/or column types are unknown (usually not inferrable). This is similar
to the `Base.EltypeUnknown()` trait for iterators when `Base.IteratorEltype` is called. Users should account
for the `Tables.schema(tbl) => nothing` case by using the properties of the results of `Tables.rows(x)` and `Tables.columns(x)`
directly.

To access the names, one can simply call `sch.names` to return the tuple of Symbols.
To access column types, one can similarly call `sch.types`, which will return a tuple of types (like `(Int64, Float64, String)`).

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

# pass through Ints to allow Tuples to act as rows
sym(x) = Symbol(x)
sym(x::Int) = x

Schema(names, ::Nothing) = Schema{Tuple(Base.map(sym, names)), nothing}()
Schema(names, types) = Schema{Tuple(Base.map(sym, names)), Tuple{types...}}()

function Base.show(io::IO, sch::Schema{names, types}) where {names, types}
    println(io, "Tables.Schema:")
    Base.print_matrix(io, hcat(collect(names), types === nothing ? fill(nothing, length(names)) : collect(fieldtype(types, i) for i = 1:fieldcount(types))))
end

function Base.getproperty(sch::Schema{names, types}, field::Symbol) where {names, types}
    if field === :names
        return names
    elseif field === :types
        return types === nothing ? nothing : Tuple(fieldtype(types, i) for i = 1:fieldcount(types))
    else
        throw(ArgumentError("unsupported property for Tables.Schema"))
    end
end

Base.propertynames(sch::Schema) = (:names, :types)

# helper functions
include("utils.jl")

# reference implementations: Vector of NamedTuples and NamedTuple of Vectors
include("namedtuples.jl")

# generic fallback definitions
include("fallbacks.jl")

# allow any valid iterator to be a table
include("tofromdatavalues.jl")

# simple table operations on table inputs
include("operations.jl")

# matrix integration
include("matrix.jl")

"Return the column index (1-based) of a `colname` in a table with a known schema; returns 0 if `colname` doesn't exist in table"
columnindex(table, colname) = columnindex(schema(table), colname)

"Return the column type of a `colname` in a table with a known schema; returns Union{} if `colname` doesn't exist in table"
columntype(table, colname) = columntype(schema(table), colname)

end # module
