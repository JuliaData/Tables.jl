### Tables.jl

[![Build Status](https://travis-ci.org/JuliaData/Tables.jl.svg?branch=master)](https://travis-ci.org/JuliaData/Tables.jl)
[![Codecov](https://codecov.io/gh/JuliaData/Tables.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Tables.jl)

The Tables.jl package provides simple, yet powerful interface functions for working with all kinds tabular data through predictable access patterns. At its core, it provides two simple functions for accessing a source table's data, regardless of its storage format or orientation:

```julia
    Tables.rows(table) => Rows
    Tables.columns(table) => Columns
```
These two functions return objects that satisfy the `Rows` or `Columns` interfaces:
* `Rows` is an iterator (i.e. implements `Base.iterate(x)`) of property-accessible objects (any type that supports `propertynames(row)` and `getproperty(row, nm::Symbol`)
* `Columns` is a property-accessible object of iterators (i.e. each column can be retrieved via `getproperty` and is an iterator)

So `Rows` is any object that can be used like:
```julia
for rows in table
    for columnname in propertynames(row)
        value = getproperty(row, columnname)
    end
end
```
And `Columns` is any object that can be used like:
```julia
for columnname in propertynames(table)
    column = getproperty(table, columnname)
end
```

In addition to these `Rows` and `Columns` objects, it's useful to be able to query properties of these objects:
* `Tables.schema(x::Union{Rows, Columns}) => Union{Tables.Schema, Nothing}`: returns a `Tables.Schema` object, or `nothing` if the table's schema is unknown
* For the `Tables.Schema` object:
  * column names can be accessed as an indexable collection of Symbols like `sch.names`
  * column types can be accessed as an indexable collection of types like `sch.types`
  * See `?Tables.Schema` for more details on this type
Because many table types are able to provide a well-defined schema, it can enable optimizations for consumers when this schema can be queried upfront before data access.

A big part of the power in these simple interface functions is that each, `Tables.rows` ***and*** `Tables.columns`, is defined for any table type, even if the table type only explicitly implements one interface function or the other.
This is accomplished by providing performant, generic fallback definitions in Tables.jl itself (though obviously nothing prevents a table type from implementing each interface function directly).

This means that table *authors* only need to worry about providing a single, most natural access pattern to their table type, whereas table *consumers* don't need to worry about the storage format or orientation of a table source, but can instead focus on the most natural *consumption* pattern for data access (row-by-row or on entire columns).

With these simple definitions, powerful workflows are enabled:
* A package providing data cleansing, manipulation, visualization, or analysis can automatically handle any number of decoupled input table types
* A tabular file format can have automatic integration with in-memory structures and translation to other file formats
* table-like database objects can be queried, streaming the results direclty to various file formats or in-memory table structures

# Tables Interface

So how does one go about satisfying the Tables.jl interface functions? It mainly depends on what you've already defined and the natural access patterns of your table:

## `Tables.istable`:

* `Tables.istable(::Type{<:MyTable}) = true`: this provides an explicit affirmation that your type implements the Tables interface
* `Tables.istable(x::MyTable) = x.istable`: alternatively, it may be the case that `MyTable` can only implement that Tables interface in some cases, known only at runtime; in this case, we can define `Tables.istable` on an ***instance*** of `MyTable` instead of the type. For consumers, this function should always be called on ***instances*** (like `Tables.istable(x)`), to ensure input tables are appropriately supported

## To support `Rows`:

* Define `Tables.rowaccess(::Type{<:MyTable}) = true`: this signals that `MyTable` supports iterating objects that satisfy the `Row` interface; note this function isn't meant for public use, but is instead used by Tables.jl itself to provide a generic fallback definition for `Tables.columns` on row-oriented sources
* Define `Tables.rows(x::MyTable)`: return a `Row`-iterator object (perhaps the table itself if it already defines a `Base.iterate` method that returns `Row` interface objects)
* Define `Tables.schema(Tables.rows(x::MyTable))` to either return a `Tables.Schema` object, or `nothing` if the schema is unknown or non-inferrable for some reason

## To support `Columns`:

* Define `Tables.columnaccess(::Type{<:MyTable}) = true`: this signals that `MyTable` supports returning an object satisfying the `Columns` interface; note this function isn't meant for public use, but is instead used by Tables.jl itself to provide a generic fallback definition for `Tables.rows` on column-oriented sources
* Define `Tables.columns(x::MyTable)`: return an object satisfying the `Columns` interface, perhaps the table itself if it naturally supports property-access to columns
* Define `Tables.schema(Tables.columns(x::MyTable))` to either return a `Tables.Schema` object, or `nothing` if the schema is unknown or non-inferrable for some reason

## Consuming table inputs (i.e. ***using*** the Tables.jl interface)

As the author of `MyTable`, I'm ecstatic that `MyTable` can now automatically be used by a number of other "table" packages, but another question is how `MyTable` can be a "sink" for any other table type. In other words, how do I actually ***use*** the Tables.jl interface?

The answer is mostly straightforward: just use the interface functions. A note does need to be made with regards to how interfaces currently operate in Julia; there's no support for "dispatching" on objects satisfying interfaces, which means I can't just define `MyTable(table::Tables.Table)`. What most packages do is define a constructor (or "sink function") that takes a single, un-typed argument like:

```julia
function MyTable(x)
    # Tables.istable(x) || throw(ArgumentError("input is not a table))
    rows = Tables.rows(x)
    sch = Tables.schema(rows)
    names = sch.names
    types = sch.types
    # custom constructor that creates an "empty" MyTable according to given column names & types
    # note that the "unknown" schema case should be considered, i.e. when `Tables.schema(x) === nothing`
    mytbl = MyTable(names, types)
    for row in rows
        # a convenience function provided in Tables.jl for "unrolling" access to each column/property of a `Row`
        # it works by applying a provided function to each value; see `?Tables.eachcolumn` for more details
        Tables.eachcolumn(sch, row) do val, columnindex::Int, columnname::Symbol
            push!(mytbl[columnindex], val)
        end
    end
    return mytbl
end
```
In this example, `MyTable` defines a constructor that takes any tables input source, initializes an empty `MyTable`, and proceeds to iterate over the input rows, appending values to each column. Note that the function didn't do any validation on the input to check if it was a valid table: `Tables.rows(x)` will throw an error if `x` doesn't actually satisfy the Tables.jl interface. Alternatively, we could call `Tables.istable(x)` (as shown in the commented line at the start of the function) on the input before calling `Tables.rows(x)` if we needed to restrict things to known, valid Tables.jl. Note that doing this will prevent certain, valid table inputs from being consumed, due to their inability to confidently return `true` for `Tables.istable`, even at runtime (cases like `Generator`s, or `Vector{Any}`). In short, most package just call `Tables.rows`, allowing invalid source errors to be thrown while also accepting the maximum number of possible valid inputs.

Alternatively, it may be more natural for `MyTable` to consume input data column-by-column, so my definition would be more like:
```julia
function MyTable(x)
    cols = Tables.columns(x)
    # here we use Tables.eachcolumn to iterate over each column in `cols`, which satisfies the `Columns` interface
    return MyTable(collect(propertynames(cols)), [collect(col) for col in Tables.eachcolumn(cols)])
end
```

Note that in neither case did we need to call `Tables.rowaccess` or `Tables.columnaccess`; those interface functions are only used internally by Tables.jl itself to provide the `Tables.rows` and `Tables.columns` fallback definitions. As a consumer, I only need to consider which of `Tables.rows` or `Tables.columns` better fits my use-case, knowing that if the input table isn't oriented naturally, the fallback definition will provide the access pattern I desire. Also note that in the column-oriented definition, we didn't even call `Tables.schema` since we just do a single iteration over each column. Also note that in the row-oriented case, we didn't account for the case when `Tables.schema(x) === nothing`; one way to support the unknown schema case is to do something like:
```julia
function MyTable(x)
    rows = Tables.rows(x)
    state = iterate(rows)
    if state === nothing
        # the input table was empty, so return an empty MyTable
        return MyTable()
    end
    row, st = state
    columnnames = propertynames(row)
    # create a Tables.Schema manually w/ just the column names from the first row
    sch = Tables.Schema(columnnames, nothing)
    cols = length(columnnames)
    # create an emtpy MyTable with just the expected column names
    mytbl = MyTable(columnnames)
    while state !== nothing
        row, st = state
        Tables.eachcolumn(sch, row) do val, columnindex::Int, columnname::Symbol
            push!(mytbl[columnindex], val)
        end
        state = iterate(rows, st)
    end
    return mytbl
end
```

## Functions that input and output tables:

For functions that input a table, perform some calculation, and output a new table, we need a way of constructing the preferred output table given the input.  For this purpose, `Tables.materializer(table)` returns the preferred sink function for a table (`Tables.columntable`, which creates a named tuple of AbstractVectors, is the default).  

Note that an in-memory table with a properly defined "sink" function can reconstruct itself with the following:

```julia
materializer(table)(Tables.columns(table)) 

materializer(table)(Tables.rows(table))
```

For example, we may want to select a subset of columns from a column-access table.  One way we could implement it is with the following:

```julia
function select(table, cols::Symbol...)
    nt = Tables.columntable(table)  # columntable(t) creates a NamedTuple of AbstractVectors
    newcols = NamedTuple{cols}(nt)
    Tables.materializer(table)(newcols)
end

# Example of selecting columns from a columntable
tbl = (x=1:100, y=rand(100), z=randn(100))
select(tbl, :x)
select(tbl, :x, :z)

tbl = [(x=1, y="a", z=1.0), (x=2, y="b", z=2.0)]
select(tbl, :z, :x)
```

## Utilities
A number of "helper" utility functions are provided to aid in working with the Tables.jl collection of interfaces:

* `rowtable(x)`: takes any input that satisfies the Tables.jl interface and converts it to a `Vector` of `NamedTuple`s, which itself satisfies the Tables.jl interface
* `rowtable(rt, x)`: take a "row table" (`Vector` of `NamedTuples`) and any table input `x` and appends `x` to `rt`
* `columntable(x)`: takes any input that satisfies the Tables.jl interface and converts it to a `NamedTuple` of `AbstractVector`s, which itself satisfies the Tables.jl interface
* `columntable(ct, x)`: takes a "column table (`NamedTuple` of `AbstractVector`s) and a table input `x` and appends `x` to `ct`
* `Tables.datavaluerows(x)`: takes any table input `x` and returns an iterator that will replace `missing` values with `DataValue`-wrapped values; this allows any table type to satisfy the `TableTraits.jl` Queryverse integration interface by defining: `IteratorInterfaceExtensions.getiterator(x::MyTable) = Tables.datavaluerows(x)`
* `Tables.nondatavaluerows(x)`: takes any iterator and replaces any `DataValue` values that are actually missing with `missing`
* `Tables.transform(x, transformfunctions...)`: create a lazy wrapper that satisfies the Tables.jl interface and applies `transformfunctions` to values when accessed; the tranform functions can be a NamedTuple or Dict mapping column name (`String` or `Symbol` or `Integer` index) to `Function`
* `Tables.select(x, columns...)`: create a lazy wrapper that satisfies the Tables.jl interface and keeps only the columns given by the `columns` arguments, which can be `String`s, `Symbol`s, or `Integer`s
* `Tables.table(x::AbstractMatrix)`: because any `AbstractMatrix` isn't a table by default, a convenience function is provided to treat an `AbstractMatrix` as a table; see `?Tables.table` for more details
* `Tables.matrix(x; transpose::Bool=false)`: a matrix "sink" function; takes any table input and converts to a dense `Matrix`; see `?Tables.matrix` for more details
* `Tables.eachcolumn`: convenience function for objects satisfying the `Row` or `Columns` interfaces which allows iterating or applying a function over each column; see `?Tables.eachcolumn` for more details
