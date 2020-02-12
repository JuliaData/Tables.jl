# Tables.jl Documentation

This guide provides documentation around the powerful tables interfaces in the Tables.jl package.
Note that the package, and hence, documentation, are geared towards package and library developers
who intend to implement and consume the interfaces. Users, on the other hand, benefit from these
other packages that provide useful access to table data in various formats or workflows.

With that said, don't hesitate to [open a new issue](https://github.com/JuliaData/Tables.jl/issues/new), even
just for a question, or come chat with us on the [#data](https://julialang.slack.com/messages/data/) slack
channel with question, concerns, or clarifications.

```@contents
Depth = 3
```

## Using the Interface (i.e. consuming Tables.jl sources)

We start by discussing _usage_ of the Tables.jl interfaces, since that can help contextualize _implementing_ them.

At a high level, Tables.jl provides two powerful APIs for predictably accessing data from any table-like source:
```julia
# access data of input table `x` row-by-row
rows = Tables.rows(x)

for row in rows
    # example of getting all values in the row
    # don't worry, there are other ways to more efficiently process rows
    rowvalues = [Tables.getcolumn(row, col) for col in Tables.columnnames(row)]
end

# access data of input table `x` column-by-column
columns = Tables.columns(x)

# iterate through each column name in table
for col in Tables.columnnames(columns)
    # retrieve entire column by column name
    # a column is an indexable collection
    # with known length (i.e. supports
    # `length(column)` and `column[i]`)
    column = Tables.getcolumn(columns, col)
end
```

So we see two high-level functions here, `Tables.rows`, and `Tables.columns`.

```@docs
Tables.rows
Tables.columns
```

Given these two powerful data access methods, let's walk through real, albeit somewhat simplified versions of how packages actually use these methods.

### `Tables.rows` usage

First up, let's take a look at the [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl) package and how it uses the Tables.jl interface to allow loading of generic table-like data into a sqlite relational table. Here's the code:
```julia
function load!(table, db::SQLite.DB, tablename)
    # get input table rows
    rows = Tables.rows(table)
    # query for schema of data
    sch = Tables.schema(rows)
    # create table using tablename and data schema
    createtable!(db, tablename, sch)
    # build insert statement
    params = chop(repeat("?,", length(sch.names)))
    stmt = Stmt(db, "INSERT INTO $tablename VALUES ($params)")
    # start a transaction for inserting rows
    transaction(db) do
        # iterate over rows in the input table
        for row in rows
            # Tables.jl provides a utility function
            # Tables.eachcolumn, which allows efficiently
            # applying a function to each column value in a row
            # it's called with a schema and row, and applies
            # a user-provided function to the column `val`, index `i`
            # and column name `nm`. Here, we bind the row values
            # to our parameterized SQL INSERT statement and then
            # call `sqlite3_step` to execute the INSERT statement.
            Tables.eachcolumn(sch, row) do val, i, nm
                bind!(stmt, i, val)
            end
            sqlite3_step(stmt.handle)
            sqlite3_reset(stmt.handle)
        end
    end
    return
end
```

This is pretty straightforward usage: it calls `Tables.rows` on the input table source,
and since we need the schema to setup the database table, we query it via `Tables.schema`.
We then iterate the rows in our table via `for row in rows`, and use the convenient
`Tables.eachcolumn` to efficiently apply a function to each value in the row. Note that
we didn't call `Tables.columnnames` or `Tables.getcolumn` at all, since they're utilized
by `Tables.eachcolumn` itself.

One wrinkle to consider is the "unknown schema" case; i.e. what if our [`Tables.schema`](@ref)
call had returned `nothing`.
```julia
function load!(sch::Nothing, rows, db::SQLite.DB, tablename)
    # sch is nothing === unknown schema
    # start iteration on input table rows
    state = iterate(rows)
    state === nothing && return
    row, st = state
    # query column names of first row
    names = Tables.columnnames(row)
    # partially construct Tables.Schema by at least passing
    # the column names to it
    sch = Tables.Schema(names, nothing)
    # create table if needed
    createtable!(db, tablename, sch)
    # build insert statement
    params = chop(repeat("?,", length(names)))
    stmt = Stmt(db, "INSERT INTO $nm VALUES ($params)")
    # start a transaction for inserting rows
    transaction(db) do
        while true
            # just like before, we can still use `Tables.eachcolumn`
            # even with our partially constructed Tables.Schema
            # to apply a function to each value in the row
            Tables.eachcolumn(sch, row) do val, i, nm
                bind!(stmt, i, val)
            end
            sqlite3_step(stmt.handle)
            sqlite3_reset(stmt.handle)
            # keep iterating rows until we finish
            state = iterate(rows, st)
            state === nothing && break
            row, st = state
        end
    end
    return name
end
```

The strategy taken here is to start iterating the input source, and using the first row
as a guide, we make a `Tables.Schema` object with just the column names, which we can
then still pass to `Tables.eachcolumn` to apply our `bind!` function to each row value.

### `Tables.columns` usage

Ok, now let's take a look at a case utlizing `Tables.columns`.
The following code is taken from the [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl/blob/master/src/other/tables.jl)
Tables.jl implementation:
```julia
getvector(x::AbstractVector) = x
getvector(x) = collect(x)

# note that copycols is ignored in this definition (Tables.CopiedColumns implies copies have already been made)
fromcolumns(x::Tables.CopiedColumns, names; copycols::Bool=true) =
    DataFrame(AbstractVector[getvector(Tables.getcolumn(x, nm) for nm in names],
              Index(names),
              copycols=false)
fromcolumns(x; copycols::Bool=true) =
    DataFrame(AbstractVector[getvector(Tables.getcolumn(x, nm) for nm in names],
              Index(names),
              copycols=copycols)

function DataFrame(x; copycols::Bool=true)
    # get columns from input table source
    cols = Tables.columns(x)
    # get column names as Vector{Symbol}, which is required
    # by core DataFrame constructor
    names = collect(Symbol, Tables.columnnames(cols))
    return fromcolumns(cols, names; copycols=copycols)
end
```

So here we have a generic `DataFrame` constructor that takes a single, untyped argument,
calls `Tables.columns` on it, then `Tables.columnnames` to get the column names.
It then passes the [`AbstractColumns`](@ref)-compatible object to an internal function `fromcolumns`,
which dispatches on a special kind of `Columns` object called a [`Tables.CopiedColumns`](@ref),
which wraps any `AbstractColumns` object that has already had copies of its columns made, and are thus
safe for the columns-consumer to assume ownership of (this is because DataFrames.jl, by default
makes copies of all columns upon construction). In both cases, individual columns are collected
in `Vector{AbstractVector}`s by calling `Tables.getcolumn(x, nm)` for each column name.
A final note is the call to `getvector` on each column, which ensures each column is materialized
as an `AbstractVector`, as is required by the DataFrame constructor.

Note in both the rows and columns usages, we didn't need to worry about the natural orientation
of the input data; we just called `Tables.rows` or `Tables.columns` as was most natural for
the table-specific use-case, knowing that it will Just Work™️.

### Tables.jl Utilities

Before moving on to _implementing_ the Tables.jl interfaces, we take a quick
break to highlight some useful utility functions provided by Tables.jl:
```@docs
Tables.rowtable
Tables.columntable
Tables.namedtupleiterator
Tables.datavaluerows
Tables.nondatavaluerows
Tables.table
Tables.matrix
Tables.eachcolumn
Tables.materializer
Tables.columnindex
Tables.columntype
Tables.Row
Tables.Columns
```

## Implementing the Interface (i.e. becoming a Tables.jl source)

Now that we've seen how one _uses_ the Tables.jl interface, let's walk-through how to implement it; i.e. how can I
make my custom type valid for Tables.jl consumers?

The interface to becoming a proper table is straightforward:

| Required Methods             | Default Definition           | Brief Description                                                                                                               |
|------------------------------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `Tables.istable(table)`      |                              | Declare that your table type implements the interface                                                                           |
|  **One of:**                 |                              |                                                                                                                                 |
| `Tables.rowaccess(table)`    |                              | Declare that your table type defines a `Tables.rows(table)` method                                                              |
| `Tables.rows(table)`         |                              | Return an `AbstractRow`-compatible iterator from your table                                                                     |
| **Or:**                      |                              |                                                                                                                                 |
| `Tables.columnaccess(table)` |                              | Declare that your table type defines a `Tables.columns(table)` method                                                           |
| `Tables.columns(table)`      |                              | Return an `AbstractColumns`-compatible object from your table                                                                   |
| **Optional methods**         |                              |                                                                                                                                 |
| `Tables.schema(x)`           | `Tables.schema(x) = nothing` | Return a `Tables.Schema` object from your `AbstractRow` iterator or `AbstractColumns` object; or `nothing` for unknown schema   |
| `Tables.materializer(table)` | `Tables.columntable`         | Declare a "materializer" sink function for your table type that can construct an instance of your type from any Tables.jl input |

Based on whether your table type has defined `Tables.rows` or `Tables.columns`, you then ensure that the `AbstractRow` iterator
or `AbstractColumns` object satisfies the respective interface:

### `AbstractRow`

```@docs
Tables.AbstractRow
```

### `AbstractColumns`

```@docs
Tables.AbstractColumns
```

### Implementation Example
As an extended example, let's take a look at some code defined in Tables.jl for treating `AbstractMatrix`s as tables.

First, we define a special `MatrixTable` type that will wrap an `AbstractMatrix`, and allow easy overloading for the 
Tables.jl interface.

```julia
struct MatrixTable{T <: AbstractMatrix} <: Tables.AbstractColumns
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::T
end
# declare that MatrixTable is a table
Tables.istable(::Type{<:MatrixTable}) = true
# getter methods to avoid getproperty clash
names(m::MatrixTable) = getfield(m, :names)
mat(m::MatrixTable) = getfield(m, :matrix)
lookup(m::MatrixTable) = getfield(m, :lookup)
# schema is column names and types
Tables.schema(m::MatrixTable{T}) where {T} = Tables.Schema(names(m), fill(eltype(T), size(mat(m), 2)))
```

Here we defined `Tables.istable` for all `MatrixTable` types, signaling that they implement the Tables.jl interfaces.
We also defined `Tables.schema` by pulling the column names out that we stored, and since `AbstractMatrix` have a single
`eltype`, we repeat it for each column. Note that defining `Tables.schema` is optional on tables; by default, `nothing`
is returned and Tables.jl consumers should account for both known and unknown schema cases. It tends to allow consumers
to have certain optimizations when they can know the types of all columns upfront (and if the # of columns isn't too large)
to generate more efficient code.

Now, in this example, we're actually going to have `MatrixTable` implement _both_ `Tables.rows` and `Tables.columns`
methods itself, i.e. it's going to return itself from those functions, so here's first how we make our `MatrixTable` a
valid `AbstractColumns` object:

```julia
# column interface
Tables.columnaccess(::Type{<:MatrixTable}) = true
Tables.columns(m::MatrixTable) = m
# required AbstractColumns object methods
Tables.getcolumn(m::MatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = mat(m)[:, col]
Tables.getcolumn(m::MatrixTable, nm::Symbol) = mat(m)[:, lookup(m)[nm]]
Tables.getcolumn(m::MatrixTable, i::Int) = mat(m)[:, i]
Tables.columnnames(m::MatrixTable) = names(m)
```

We define `columnaccess` for our type, then `columns` just returns the `MatrixTable` itself, and then we define
the three `getcolumn` methods and `columnnames`. Note the use of a `lookup` `Dict` that maps column name to column index
so we can figure out which column to return from the matrix. We're also storing the column names in our `names` field
so the `columnnames` implementation is trivial. And that's it! Literally! It can now be written out to a csv file,
stored in a sqlite or other database, converted to DataFrame or JuliaDB table, etc. Pretty fun.

And now for the `Tables.rows` implementation:
```julia
# declare that any MatrixTable defines its own `Tables.rows` method
rowaccess(::Type{<:MatrixTable}) = true
# just return itself, which means MatrixTable must iterate `AbstractRow`-compatible objects
rows(m::MatrixTable) = m
# the iteration interface, at a minimum, requires `eltype`, `length`, and `iterate`
# for `MatrixTable` `eltype`, we're going to provide a custom row type
Base.eltype(m::MatrixTable{T}) where {T} = MatrixRow{T}
Base.length(m::MatrixTable) = size(mat(m), 1)

Base.iterate(m::MatrixTable, st=1) = st > length(m) ? nothing : (MatrixRow(st, m), st + 1)

# a custom row type; acts as a "view" into a row of an AbstractMatrix
struct MatrixRow{T} <: Tables.AbstractRow
    row::Int
    source::MatrixTable{T}
end
# required `AbstractRow` interface methods (same as for `AbstractColumns` object before)
getcolumn(m::MatrixRow, ::Type, col::Int, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), col]
getcolumn(m::MatrixRow, i::Int) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), i]
getcolumn(m::MatrixRow, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), lookup(getfield(m, :source)), :lookup)[nm]]
columnnames(m::MatrixRow) = names(getfield(m, :source))
```
Here we start by defining `Tables.rowaccess` and `Tables.rows`, and then the iteration interface methods,
since we declared that a `MatrixTable` itself is an iterator of `AbstractRow`-compatible objects. For `eltype`,
we say that a `MatrixTable` iterates our own custom row type, `MatrixRow`. `MatrixRow` subtypes
`Tables.AbstractRow`, which provides interface implementations for several useful behaviors (indexing,
iteration, property-access, etc.); essentially it makes our custom `MatrixRow` type more convenient to work with.

Implementing the `Tables.AbstractRow` interface is straightfoward, and very similar to our implementation
of `AbstractColumns` previously (i.e. the same methods for `getcolumn` and `columnnames`).

And that's it. Our `MatrixTable` type is now a fully fledged, valid Tables.jl source and can be used throughout
the ecosystem. Now, this is obviously not a lot of code; but then again, the actual Tables.jl interface
implementations tend to be fairly simple, given the other behaviors that are already defined for table types
(i.e. table types tend to already have a `getcolumn` like function defined).

### `Tables.isrowtable`

One option for certain table types is to define `Tables.isrowtable` to automatically satisfy the Tables.jl interface.
This can be convenient for "natural" table types that have row elements, along with other required properties.
```@docs
Tables.isrowtable
```