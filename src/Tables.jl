module Tables

using LinearAlgebra, DataValueInterfaces, DataAPI, TableTraits, IteratorInterfaceExtensions, OrderedCollections

export rowtable, columntable

if !hasmethod(getproperty, Tuple{Tuple, Int})
    Base.getproperty(t::Tuple, i::Int) = t[i]
end

import Base: ==

"""
    Tables.AbstractColumns

An interface type defined as an ordered set of columns that support
retrieval of individual columns by name or index. A retrieved column
must be a 1-based indexable collection with known length, i.e. an object
that supports `length(col)` and `col[i]` for any `i = 1:length(col)`.
`Tables.columns` must return an object that satisfies the `Tables.AbstractColumns` interface.
While `Tables.AbstractColumns` is an abstract type that custom "columns" types may subtype for
useful default behavior (indexing, iteration, property-access, etc.), users should not use it
for dispatch, as Tables.jl interface objects **are not required** to subtype, but only
implement the required interface methods.

Interface definition:

| Required Methods                                         | Default Definition          | Brief Description                                                                                                                                            |
|----------------------------------------------------------|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Tables.getcolumn(table, i::Int)`                        | getfield(table, i)          | Retrieve a column by index                                                                                                                                   |
| `Tables.getcolumn(table, nm::Symbol)`                    | getproperty(table, nm)      | Retrieve a column by name                                                                                                                                    |
| `Tables.columnnames(table)`                              | propertynames(table)        | Return column names for a table as a 1-based indexable collection                                                                                                   |
| **Optional methods**                                     |                             |                                                                                                                                                              |
| `Tables.getcolumn(table, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(table, nm) | Given a column eltype `T`, index `i`, and column name `nm`, retrieve the column. Provides a type-stable or even constant-prop-able mechanism for efficiency. |

Note that subtypes of `Tables.AbstractColumns` **must** overload all required methods listed
above instead of relying on these methods' default definitions.

While types aren't required to subtype `Tables.AbstractColumns`, benefits of doing so include:
  * Indexing interface defined (using `getcolumn`); i.e. `tbl[i]` will retrieve the column at index `i`
  * Property access interface defined (using `columnnames` and `getcolumn`); i.e. `tbl.col1` will retrieve column named `col1`
  * Iteration interface defined; i.e. `for col in table` will iterate each column in the table
  * `AbstractDict` methods defined (`get`, `haskey`, etc.) for checking and retrieving columns
  * A default `show` method
This allows a custom table type to behave as close as possible to a builtin `NamedTuple` of vectors object.
"""
abstract type AbstractColumns end

"""
    Tables.AbstractRow

Abstract interface type representing the expected `eltype` of the iterator returned from `Tables.rows(table)`.
`Tables.rows` must return an iterator of elements that satisfy the `Tables.AbstractRow` interface.
While `Tables.AbstractRow` is an abstract type that custom "row" types may subtype for
useful default behavior (indexing, iteration, property-access, etc.), users should not use it
for dispatch, as Tables.jl interface objects **are not required** to subtype, but only
implement the required interface methods.

Interface definition:

| Required Methods                                       | Default Definition        | Brief Description                                                                                                                                                |
|--------------------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Tables.getcolumn(row, i::Int)`                        | getfield(row, i)          | Retrieve a column value by index                                                                                                                                 |
| `Tables.getcolumn(row, nm::Symbol)`                    | getproperty(row, nm)      | Retrieve a column value by name                                                                                                                                  |
| `Tables.columnnames(row)`                              | propertynames(row)        | Return column names for a row as a 1-based indexable collection                                                                                                         |
| **Optional methods**                                   |                           |                                                                                                                                                                  |
| `Tables.getcolumn(row, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(row, nm) | Given a column element type `T`, index `i`, and column name `nm`, retrieve the column value. Provides a type-stable or even constant-prop-able mechanism for efficiency. |

Note that subtypes of `Tables.AbstractRow` **must** overload all required methods listed above
instead of relying on these methods' default definitions.

While custom row types aren't required to subtype `Tables.AbstractRow`, benefits of doing so include:
  * Indexing interface defined (using `getcolumn`); i.e. `row[i]` will return the column value at index `i`
  * Property access interface defined (using `columnnames` and `getcolumn`); i.e. `row.col1` will retrieve the value for the column named `col1`
  * Iteration interface defined; i.e. `for x in row` will iterate each column value in the row
  * `AbstractDict` methods defined (`get`, `haskey`, etc.) for checking and retrieving column values
  * A default `show` method
This allows the custom row type to behave as close as possible to a builtin `NamedTuple` object.
"""
abstract type AbstractRow end

"""
    Tables.getcolumn(::AbstractColumns, nm::Symbol) => Indexable collection with known length
    Tables.getcolumn(::AbstractColumns, i::Int) => Indexable collection with known length
    Tables.getcolumn(::AbstractColumns, T, i::Int, nm::Symbol) => Indexable collection with known length

    Tables.getcolumn(::AbstractRow, nm::Symbol) => Column value
    Tables.getcolumn(::AbstractRow, i::Int) => Column value
    Tables.getcolumn(::AbstractRow, T, i::Int, nm::Symbol) => Column value

Retrieve an entire column (from `AbstractColumns`) or single row column value (from an `AbstractRow`) by column name (`nm`), index (`i`),
or if desired, by column element type (`T`), index (`i`), and name (`nm`). When called on a `AbstractColumns` interface object,
the returned object should be a 1-based indexable collection with known length. When called on a `AbstractRow` interface
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
    Tables.columnnames(::Union{AbstractColumns, AbstractRow}) => Indexable collection

Retrieves the list of column names as a 1-based indexable collection (like a `Tuple` or `Vector`)
for a `AbstractColumns` or `AbstractRow` interface object.
The default definition calls `propertynames(x)`.
The returned column names must be unique.
"""
function columnnames end

columnnames(x) = propertynames(x)

"""
    Tables.isrowtable(x) => Bool

For convenience, some table objects that are naturally "row oriented" can
define `Tables.isrowtable(::Type{TableType}) = true` to simplify satisfying the
Tables.jl interface. Requirements for defining `isrowtable` include:
  * `Tables.rows(x) === x`, i.e. the table object itself is a `Row` iterator
  * If the table object is mutable, it should support:
    * `push!(x, row)`: allow pushing a single row onto table
    * `append!(x, rows)`: allow appending set of rows onto table
  * If table object is mutable and indexable, it should support:
    * `x[i] = row`: allow replacing of a row with another row by index

A table object that defines `Tables.isrowtable` will have definitions for
`Tables.istable`, `Tables.rowaccess`, and `Tables.rows` automatically defined.
"""
function isrowtable end

isrowtable(::T) where {T} = isrowtable(T)
isrowtable(::Type{T}) where {T} = false
# to avoid ambiguities
isrowtable(::Type{T}) where {T <: AbstractVector{Union{}}} = false

# default definitions for AbstractDict to act as an AbstractColumns or AbstractRow
getcolumn(x::AbstractDict{Symbol}, i::Int) = x[columnnames(x)[i]]
getcolumn(x::AbstractDict{Symbol}, nm::Symbol) = x[nm]
getcolumn(x::AbstractDict{Symbol}, ::Type{T}, i::Int, nm::Symbol) where {T} = x[nm]
columnnames(x::AbstractDict{Symbol}) = collect(keys(x))

getcolumn(x::AbstractDict{<:AbstractString}, i::Int) = x[String(columnnames(x)[i])]
getcolumn(x::AbstractDict{<:AbstractString}, nm::Symbol) = x[String(nm)]
getcolumn(x::AbstractDict{<:AbstractString}, ::Type{T}, i::Int, nm::Symbol) where {T} = x[String(nm)]
columnnames(x::AbstractDict{<:AbstractString}) = collect(Symbol(k) for k in keys(x))

# AbstractVector of Dicts for Tables.rows
const DictRows = AbstractVector{T} where {T <: Union{AbstractDict{<:AbstractString}, AbstractDict{Symbol}}}
isrowtable(::Type{<:DictRows}) = true
# DictRows doesn't naturally lend itself to the `Tables.schema` requirement
# we can't just look at the first row, because the types might change,
# row-to-row (e.g. `missing`, then `1.1`, etc.). Therefore, the safest option
# is to just return `nothing`
schema(x::DictRows) = nothing

# Dict of AbstractVectors for Tables.columns
const DictColumns = Union{<:AbstractDict{<:AbstractString, <:AbstractVector}, <:AbstractDict{Symbol, <:AbstractVector}}
istable(::Type{<:DictColumns}) = true
columnaccess(::Type{<:DictColumns}) = true
columns(x::DictColumns) = x
schema(x::DictColumns) = Schema(collect(keys(x)), eltype.(values(x)))

# for other AbstractDict, let's throw an informative error
columns(x::T) where {T <: AbstractDict} = error("to treat $T as a table, it must have a key type of `Symbol`, and a value type `<: AbstractVector`")

# default definitions for AbstractRow, AbstractColumns
const RorC = Union{AbstractRow, AbstractColumns}

# avoids mutual recursion with default definitions (issue #221)
getcolumn(::T, ::Int) where {T <: RorC} = error("`Tables.getcolumn` must be specifically overloaded for $T <: Union{AbstractRow, AbstractColumns}`")
getcolumn(::T, ::Symbol) where {T <: RorC} = error("`Tables.getcolumn` must be specifically overloaded for $T <: Union{AbstractRow, AbstractColumns}`")
columnnames(::T) where {T <: RorC} = error("`Tables.columnnames` must be specifically overloaded for $T <: Union{AbstractRow, AbstractColumns}`")

Base.IteratorSize(::Type{R}) where {R <: RorC} = Base.HasLength()
Base.length(r::RorC) = length(columnnames(r))
Base.IndexStyle(::Type{<:RorC}) = Base.IndexLinear()
Base.firstindex(r::RorC) = 1
Base.lastindex(r::RorC) = length(r)
Base.getindex(r::RorC, i::Int) = getcolumn(r, i)
Base.getindex(r::RorC, nm::Symbol) = getcolumn(r, nm)
Base.getproperty(r::RorC, nm::Symbol) = getcolumn(r, nm)
Base.getproperty(r::RorC, i::Int) = getcolumn(r, i)
Base.propertynames(r::RorC) = columnnames(r)
Base.keys(r::RorC) = columnnames(r)
Base.values(r::RorC) = collect(r)
Base.haskey(r::RorC, key::Symbol) = key in columnnames(r)
Base.haskey(r::RorC, i::Int) = 0 < i <= length(columnnames(r))
Base.get(r::RorC, key::Union{Integer, Symbol}, default) = haskey(r, key) ? getcolumn(r, key) : default
Base.get(f::Base.Callable, r::RorC, key::Union{Integer, Symbol}) = haskey(r, key) ? getcolumn(r, key) : f()
Base.iterate(r::RorC, i=1) = i > length(r) ? nothing : (getcolumn(r, i), i + 1)
Base.isempty(r::RorC) = length(r) == 0

function Base.NamedTuple(r::RorC)
    names = columnnames(r)
    return NamedTuple{Tuple(map(Symbol, names))}(Tuple(getcolumn(r, nm) for nm in names))
end

function Base.show(io::IO, x::T) where {T <: AbstractRow}
    if get(io, :compact, false) || get(io, :limit, false)
        print(io, "$T: ")
        show(io, NamedTuple(x))
    else
        println(io, "$T:")
        names = collect(columnnames(x))
        values = [getcolumn(x, nm) for nm in names]
        Base.print_matrix(io, hcat(names, values))
    end
end

function Base.show(io::IO, table::AbstractColumns; max_cols = 20)
    ncols = length(columnnames(table))
    print(io, "$(typeof(table)) with $(rowcount(table)) rows, $(ncols) columns, and ")
    sch = schema(table)
    if sch !== nothing
        print(io, "schema:\n")
        show(IOContext(io, :print_schema_header => false), sch)
    else
        print(io, "an unknown schema.")
    end
end

# AbstractRow AbstractVector as Rows
const AbstractRowTable = AbstractVector{T} where {T <: AbstractRow}
isrowtable(::Type{<:AbstractRowTable}) = true
schema(x::AbstractRowTable) = nothing

# AbstractColumns as Columns
istable(::Type{<:AbstractColumns}) = true
columnaccess(::Type{<:AbstractColumns}) = true
columns(x::AbstractColumns) = x
schema(x::AbstractColumns) = nothing

"""
    Tables.Row(row)

Convenience type to wrap any `AbstractRow` interface object in a dedicated struct
to provide useful default behaviors (allows any `AbstractRow` to be used like a `NamedTuple`):
  * Indexing interface defined; i.e. `row[i]` will return the column value at index `i`, `row[nm]` will return column value for column name `nm`
  * Property access interface defined; i.e. `row.col1` will retrieve the value for the column named `col1`
  * Iteration interface defined; i.e. `for x in row` will iterate each column value in the row
  * `AbstractDict` methods defined (`get`, `haskey`, etc.) for checking and retrieving column values
"""
struct Row{T} <: AbstractRow
    x::T
end

Row(x::Row) = x

"""
    Tables.Columns(tbl)

Convenience type that calls `Tables.columns` on an input `tbl` and wraps the resulting `AbstractColumns` interface object in a dedicated struct
to provide useful default behaviors (allows any `AbstractColumns` to be used like a `NamedTuple` of `Vectors`):
  * Indexing interface defined; i.e. `row[i]` will return the column at index `i`, `row[nm]` will return column for column name `nm`
  * Property access interface defined; i.e. `row.col1` will retrieve the value for the column named `col1`
  * Iteration interface defined; i.e. `for x in row` will iterate each column in the row
  * `AbstractDict` methods defined (`get`, `haskey`, etc.) for checking and retrieving columns

Note that `Tables.Columns` calls `Tables.columns` internally on the provided table argument.
`Tables.Columns` can be used for dispatch if needed.
"""
struct Columns{T} <: AbstractColumns
    x::T

    function Columns(x)
        cols = columns(x)
        return new{typeof(cols)}(cols)
    end
end

Columns(x::Columns) = x

# Columns can only wrap something that is a table, so we pass the schema through
schema(x::Columns) = schema(getx(x))

const RorC2 = Union{Row, Columns}

getx(x::RorC2) = getfield(x, :x)

getcolumn(x::RorC2, i::Int) = getcolumn(getx(x), i)
getcolumn(x::RorC2, nm::Symbol) = getcolumn(getx(x), nm)
getcolumn(x::RorC2, ::Type{T}, i::Int, nm::Symbol) where {T} = getcolumn(getx(x), T, i, nm)
columnnames(x::RorC2) = columnnames(getx(x))

"""
    Tables.istable(x) => Bool

Check if an object has specifically defined that it is a table. Note that
not all valid tables will return true, since it's possible to satisfy the
Tables.jl interface at "run-time", e.g. a `Generator` of `NamedTuple`s iterates
`NamedTuple`s, which satisfies the `AbstractRow` interface, but there's no static way
of knowing that the generator is a table.

It is recommended that for users implementing `MyType`, they define only
`istable(::Type{MyType})`. `istable(::MyType)` will then automatically delegate to this
method.

`istable` calls `TableTraits.isiterabletable` as a fallback. This can have a considerable
runtime overhead in some contexts. To avoid these and use `istable` as a compile-time trait,
it can be called on a type as `istable(typeof(obj))`.
"""
function istable end

istable(x::T) where {T} = istable(T) || TableTraits.isiterabletable(x) === true
istable(::Type{T}) where {T} = isrowtable(T)
# to avoid ambiguities
istable(::Type{T}) where {T <: AbstractVector{Union{}}} = false
istable(::AbstractVector{Union{}}) = false

"""
    Tables.rowaccess(x) => Bool

Check whether an object has specifically defined that it implements the `Tables.rows`
function that does _not_ copy table data.  That is to say, `Tables.rows(x)` must be done
with O(1) time and space complexity when `Tables.rowaccess(x) == true`.  Note that
`Tables.rows` will work on any object that iterates `AbstractRow`-compatible objects, even if
they don't define `rowaccess`, e.g. a `Generator` of `NamedTuple`s.  However, this
generic fallback may copy the data from input table `x`.  Also note that just because
an object defines `rowaccess` doesn't mean a user should call `Tables.rows` on it;
`Tables.columns` will also work, providing a valid `AbstractColumns` object from the rows.
Hence, users should call `Tables.rows` or `Tables.columns` depending on what is most
natural for them to *consume* instead of worrying about what and how the input is oriented.

It is recommended that for users implementing `MyType`, they define only
`rowaccess(::Type{MyType})`. `rowaccess(::MyType)` will then automatically delegate to this
method.
"""
function rowaccess end

rowaccess(x::T) where {T} = rowaccess(T)
rowaccess(::Type{T}) where {T} = isrowtable(T)

"""
    Tables.columnaccess(x) => Bool

Check whether an object has specifically defined that it implements the `Tables.columns`
function that does _not_ copy table data.  That is to say, `Tables.columns(x)` must be done
with O(1) time and space complexity when `Tables.columnaccess(x) == true`.  Note that
`Tables.columns` has generic fallbacks allowing it to produces `AbstractColumns` objects, even if
the input doesn't define `columnaccess`.  However, this generic fallback may copy the data
from input table `x`.  Also note that just because an object defines `columnaccess` doesn't
mean a user should call `Tables.columns` on it; `Tables.rows` will also work, providing a
valid `AbstractRow` iterator. Hence, users should call `Tables.rows` or `Tables.columns` depending
on what is most natural for them to *consume* instead of worrying about what and how the
input is oriented.

It is recommended that for users implementing `MyType`, they define only
`columnaccess(::Type{MyType})`. `columnaccess(::MyType)` will then automatically delegate to
this method.
"""
function columnaccess end

columnaccess(x::T) where {T} = columnaccess(T)
columnaccess(::Type{T}) where {T} = false

"""
    Tables.schema(x) => Union{Nothing, Tables.Schema}

Attempt to retrieve the schema of the object returned by `Tables.rows` or `Tables.columns`.
If the `AbstractRow` iterator or `AbstractColumns` object can't determine its schema, `nothing` will be returned.
Otherwise, a `Tables.Schema` object is returned, with the column names and types available for use.
"""
function schema end

schema(x) = nothing

"""
    Tables.materializer(x) => Callable

For a table input, return the "sink" function or "materializing" function that can take a
Tables.jl-compatible table input and make an instance of the table type. This enables "transform"
workflows that take table inputs, apply transformations, potentially converting the table to
a different form, and end with producing a table of the same type as the original input. The
default materializer is `Tables.columntable`, which converts any table input into a `NamedTuple`
of `Vector`s.

It is recommended that for users implementing `MyType`, they define only
`materializer(::Type{<:MyType})`. `materializer(::MyType)` will then automatically delegate to
this method.
"""
function materializer end

materializer(x::T) where {T} = materializer(T)
materializer(::Type{T}) where {T} = columntable

"""
    Tables.columns(x) => AbstractColumns-compatible object

Accesses data of input table source `x` by returning an [`AbstractColumns`](@ref)-compatible
object, which allows retrieving entire columns by name or index. A retrieved column
is a 1-based indexable object that has a known length, i.e. supports
`length(col)` and `col[i]` for any `i = 1:length(col)`. Note that
even if the input table source is row-oriented by nature, an efficient generic
definition of `Tables.columns` is defined in Tables.jl to build a `AbstractColumns`-
compatible object object from the input rows.

The [`Tables.Schema`](@ref) of a `AbstractColumns` object can be queried via `Tables.schema(columns)`,
which may return `nothing` if the schema is unknown.
Column names can always be queried by calling `Tables.columnnames(columns)`, and individual columns
can be accessed by calling `Tables.getcolumn(columns, i::Int )` or `Tables.getcolumn(columns, nm::Symbol)`
with a column index or name, respectively.

Note that if `x` is an object in which columns are stored as vectors, the check that
these vectors use 1-based indexing is not performed (it should be ensured when `x` is constructed).
"""
function columns end

"""
    Tables.rows(x) => Row iterator

Accesses data of input table source `x` row-by-row by returning an [`AbstractRow`](@ref)-compatible iterator.
Note that even if the input table source is column-oriented by nature, an efficient generic
definition of `Tables.rows` is defined in Tables.jl to return an iterator of row views into
the columns of the input.

The [`Tables.Schema`](@ref) of an `AbstractRow` iterator can be queried via `Tables.schema(rows)`,
which may return `nothing` if the schema is unknown.
Column names can always be queried by calling `Tables.columnnames(row)` on an individual row,
and row values can be accessed by calling `Tables.getcolumn(row, i::Int )` or
`Tables.getcolumn(row, nm::Symbol)` with a column index or name, respectively.

See also [`rowtable`](@ref) and [`namedtupleiterator`](@ref).
"""
function rows end

# Schema implementation
"""
    Tables.Schema(names, types)

Create a `Tables.Schema` object that holds the column names and types for an `AbstractRow` iterator
returned from `Tables.rows` or an `AbstractColumns` object returned from `Tables.columns`.
`Tables.Schema` is dual-purposed: provide an easy interface for users to query these properties,
as well as provide a convenient "structural" type for code generation.

To get a table's schema, one can call `Tables.schema` on the result of `Tables.rows` or `Tables.columns`,
but also note that a table may return `nothing`, indicating that its column names and/or column element types
are unknown (usually not inferable). This is similar to the `Base.EltypeUnknown()` trait for iterators
when `Base.IteratorEltype` is called. Users should account for the `Tables.schema(tbl) => nothing` case
by using the properties of the results of `Tables.rows(x)` and `Tables.columns(x)` directly.

To access the names, one can simply call `sch.names` to return a collection of Symbols (`Tuple` or `Vector`).
To access column element types, one can similarly call `sch.types`, which will return a collection of types (like `(Int64, Float64, String)`).

The actual type definition is
```julia
struct Schema{names, types}
    storednames::Union{Nothing, Vector{Symbol}}
    storedtypes::Union{Nothing, Vector{Type}}
end
```
Where `names` is a tuple of `Symbol`s or `nothing`, and `types` is a tuple _type_ of types (like `Tuple{Int64, Float64, String}`) or `nothing`.
Encoding the names & types as type parameters allows convenient use of the type in generated functions
and other optimization use-cases, but users should note that when `names` and/or `types` are the `nothing` value, the names and/or types
are stored in the `storednames` and `storedtypes` fields. This is to account for extremely wide tables with columns in the 10s of thousands
where encoding the names/types as type parameters becomes prohibitive to the compiler. So while optimizations can be written on the typed
`names`/`types` type parameters, users should also consider handling the extremely wide tables by specializing on `Tables.Schema{nothing, nothing}`.
"""
struct Schema{names, types}
    storednames::Union{Nothing, Vector{Symbol}}
    storedtypes::Union{Nothing, Vector{Type}}
end

Schema{names, types}() where {names, types} = Schema{names, types}(nothing, nothing)
Schema(names::Tuple{Vararg{Symbol}}, ::Type{T}) where {T <: Tuple} = Schema{names, T}()
Schema(::Type{NamedTuple{names, types}}) where {names, types} = Schema{names, types}()

# whether names/types are stored or not
stored(::Schema{names, types}) where {names, types} = names === nothing && types === nothing
stored(::Nothing) = false

# pass through Ints to allow Tuples to act as rows
sym(x) = Symbol(x)
sym(x::Int) = x

Schema(names, ::Nothing) = Schema{Tuple(map(sym, names)), nothing}()

const SCHEMA_SPECIALIZATION_THRESHOLD = (2^16) - 1

function Schema(names, types; stored::Bool=false)
    if stored || length(names) > SCHEMA_SPECIALIZATION_THRESHOLD
        return Schema{nothing, nothing}([sym(x) for x in names], Type[T for T in types])
    else
        return Schema{Tuple(map(sym, names)), Tuple{types...}}()
    end
end

function Base.show(io::IO, sch::Schema)
    get(io, :print_schema_header, true) && println(io, "Tables.Schema:")
    nms = sch.names
    Base.print_matrix(io, hcat(nms isa Vector ? nms : collect(nms), sch.types === nothing ? fill(nothing, length(nms)) : collect(sch.types)))
end

function Base.getproperty(sch::Schema{names, types}, field::Symbol) where {names, types}
    if field === :names
        return names === nothing ? getfield(sch, :storednames) : names
    elseif field === :types
        if types === nothing
            return getfield(sch, :storedtypes)
        else
            ncol = fieldcount(types)
            if ncol <= 512
                # Type stable, but slower to compile
                return ntuple(i -> fieldtype(types, i), Val(ncol))
            else
                return Tuple(fieldtype(types, i) for i=1:ncol)
            end
        end
    else
        throw(ArgumentError("unsupported property for Tables.Schema"))
    end
end

Base.propertynames(::Schema) = (:names, :types)
==(a::Schema, b::Schema) = a.names == b.names && a.types == b.types

# partitions

"""
    Tables.partitions(x)

Request a "table" iterator from `x`. Each iterated element must be a "table" in the sense
that one may call `Tables.rows` or `Tables.columns` to get a row-iterator or collection
of columns. All iterated elements _must_ have identical schema, so that users may call
`Tables.schema(first_element)` on the first iterated element and know that each
subsequent iteration will match the same schema. The default definition is:
```julia
Tables.partitions(x) = (x,)
```
So that any input is assumed to be a single "table". This means users should feel free
to call `Tables.partitions` anywhere they're currently calling `Tables.columns` or
`Tables.rows`, and get back an iterator of those instead. In other words, "sink" functions
can use `Tables.partitions` whether or not the user passes a partionable table, since the
default is to treat a single input as a single, non-partitioned table.

[`Tables.partitioner(itr)`](@ref) is a convenience wrapper to provide table partitions
from any table iterator; this allows for easy wrapping of a `Vector` or iterator of tables
as valid partitions, since by default, they'd be treated as a single table.

A 2nd convenience method is provided with the definition:
```julia
Tables.partitions(x...) = x
```
That allows passing vararg tables and they'll be treated as separate partitions. Sink
functions may allow vararg table inputs and can "splat them through" to `partitions`.

For convenience, `Tables.partitions(x::Iterators.PartitionIterator) = x` and
`Tables.partitions(x::Tables.Partitioner) = x` are defined to handle cases
where user created partitioning with the `Iterators.partition` or
[`Tables.partitioner`](@ref) functions.
"""
partitions(x) = (x,)
partitions(x...) = x
partitions(x::Iterators.PartitionIterator) = x

"""
    Tables.LazyTable(f, arg)

A "table" type that delays materialization until `Tables.columns` or `Tables.rows` is called.
This allows, for example, sending a `LazyTable` to a remote process or thread which can
then call `Tables.columns` or `Tables.rows` to "materialize" the table. Is used by default
in `Tables.partitioner(f, itr)` where a materializer function `f` is passed to each element
of an iterable `itr`, allowing distributed/concurrent patterns like:

```julia
for tbl in Tables.partitions(Tables.partitioner(CSV.File, list_of_csv_files))
    Threads.@spawn begin
        cols = Tables.columns(tbl)
        # do stuff with cols
    end
end
```
In this example, `CSV.File` will be called like `CSV.File(x)` for each element of the
`list_of_csv_files` iterable, but _not until_ `Tables.columns(tbl)` is called, which
in this case happens in a thread-spawned task, allowing files to be parsed and processed
in parallel.
"""
struct LazyTable{F, T}
    f::F
    x::T
end

columns(x::LazyTable) = columns(x.f(x.x))
rows(x::LazyTable) = rows(x.f(x.x))

struct Partitioner{T}
    x::T
end

"""
    Tables.subset(x, inds; viewhint=nothing)

Return one or more rows from table `x` according to the position(s) specified by `inds`:

- If `inds` is a single non-boolean integer return a row object.
- If `inds` is a vector of non-boolean integers, a vector of booleans, or a `:`, return a subset of the original table according to the indices.
  In this case, the returned type is not necessarily the same as the original table type.

If other types of `inds` are passed than specified above the behavior is undefined.

The `viewhint` argument tries to influence whether the returned object is a view of the original table
or an independent copy:

- If `viewhint=nothing` (the default) then the implementation for a specific table type
  is free to decide  whether to return a copy or a view.
- If `viewhint=true` then a view is returned and if `viewhint=false` a copy is returned.
  This applies both to returning a row or a table.

Any specialized implementation of `subset` must support the `viewhint=nothing` argument.
Support for `viewhint=true` or `viewhint=false` is optional
(i.e. implementations may ignore the keyword argument and return a view or a copy regardless of `viewhint` value).
"""
function subset(x::T, inds; viewhint::Union{Bool, Nothing}=nothing, view::Union{Bool, Nothing}=nothing) where {T}
    if view !== nothing
        @warn "`view` keyword argument is deprecated for `Tables.subset`, use `viewhint` instead"
        viewhint = view
    end
    # because this method is being called, we know `x` didn't define it's own Tables.subset
    # first check if it supports column access, and if so, apply inds and wrap columns in a DictColumnTable
    if columnaccess(x)
        cols = columns(x)
        if inds isa Integer
            return ColumnsRow(cols, inds)
        else
            ret = viewhint === true ? _map(c -> Base.view(c, inds), cols) : _map(c -> c[inds], cols)
            return DictColumnTable(schema(cols), ret)
        end
    end
    # otherwise, let's get the rows and see if we can apply inds to them
    r = rows(x)
    if r isa AbstractVector
        inds isa Integer && return r[inds]
        ret = viewhint === true ? Base.view(x, inds) : x[inds]
        (ret isa AbstractVector) || throw(ArgumentError("`Tables.subset`: invalid `inds` argument, expected `AbstractVector` output, got $(typeof(ret))"))
        return ret
    end
    throw(ArgumentError("no default `Tables.subset` implementation for type: $T"))
end

vectorcheck(x::AbstractVector) = x
vectorcheck(x) = throw(ArgumentError("`Tables.subset`: invalid `inds` argument, expected `AbstractVector` output, got $(typeof(x))"))
_map(f, cols) = OrderedDict(nm => vectorcheck(f(getcolumn(cols, nm))) for nm in columnnames(cols))


"""
    Tables.partitioner(f, itr)
    Tables.partitioner(x)

Convenience methods to generate table iterators. The first method takes a "materializer"
function `f` and an iterator `itr`, and will call `Tables.LazyTable(f, x) for x in itr`
for each iteration. This allows delaying table materialization until `Tables.columns`
or `Tables.rows` are called on the `LazyTable` object (which will call `f(x)`). This
allows a common desired pattern of materializing and processing a table on a remote
process or thread, like:

```julia
for tbl in Tables.partitions(Tables.partitioner(CSV.File, list_of_csv_files))
    Threads.@spawn begin
        cols = Tables.columns(tbl)
        # do stuff with cols
    end
end
```

The second method is provided because the default behavior of `Tables.partition(x)`
is to treat `x` as a single, non-partitioned table. This method allows users to easily
wrap a `Vector` or generator of tables as table partitions to pass to sink functions
able to utilize `Tables.partitions`.
"""
partitioner(x) = Partitioner(x)
partitioner(f, itr) = partitioner((LazyTable(f, x) for x in itr))

partitions(x::Partitioner) = x

Base.IteratorEltype(::Type{P}) where {S,P<:Partitioner{S}} = Base.IteratorEltype(S)
Base.eltype(::Type{P}) where {S,P<:Partitioner{S}} = eltype(S)
Base.IteratorSize(::Type{P}) where {S,P<:Partitioner{S}} = Base.IteratorSize(S)
Base.length(x::Partitioner) = length(x.x)
Base.size(x::Partitioner) = size(x.x)
Base.iterate(x::Partitioner, st...) = iterate(x.x, st...)

const SPECIALIZATION_THRESHOLD = 100

# reference implementations: Vector of NamedTuples and NamedTuple of Vectors
include("namedtuples.jl")

# helper functions
include("utils.jl")

# generic fallback definitions
include("fallbacks.jl")

# allow any valid iterator to be a table
include("tofromdatavalues.jl")

# matrix integration
include("matrix.jl")

# dict tables
include("dicts.jl")

"""
    Tables.columnindex(table, name::Symbol)

Return the column index (1-based) of a column by `name` in a table with a known schema; returns 0 if `name` doesn't exist in table
"""
columnindex(table, colname::Symbol) = columnindex(schema(table), colname)

"""
    Tables.columntype(table, name::Symbol)

Return the column element type of a column by `name` in a table with a known schema; returns Union{} if `name` doesn't exist in table
"""
columntype(table, colname::Symbol) = columntype(schema(table), colname)

Base.@pure columnindex(::Schema{names, types}, name::Symbol) where {names, types} = columnindex(names, name)

"given names and a Symbol `name`, compute the index (1-based) of the name in names"
Base.@pure function columnindex(names::Tuple{Vararg{Symbol}}, name::Symbol)
    i = 1
    for nm in names
        nm === name && return i
        i += 1
    end
    return 0
end

Base.@pure columntype(::Schema{names, types}, name::Symbol) where {names, types} = columntype(names, types, name)

"given tuple type and a Symbol `name`, compute the type of the name in the tuples types"
Base.@pure function columntype(names::Tuple{Vararg{Symbol}}, ::Type{types}, name::Symbol) where {types <: Tuple}
    i = 1
    for nm in names
        nm === name && return fieldtype(types, i)
        i += 1
    end
    return Union{}
end

end # module
