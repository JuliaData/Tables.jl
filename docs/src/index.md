# Tables.jl Documentation

This guide provides documentation around the powerful tables interfaces in the Tables.jl package.
Note that the package, and hence, documentation, are geared towards package and library developers
who intend to implement and consume the interfaces. Users, on the other hand, benefit from these
other packages that provide useful access to table data in various formats or workflows. While everyone
is encouraged to understand the interfaces and the functionality they allow, just note that most users
don't need to use Tables.jl directly.

With that said, don't hesitate to [open a new issue](https://github.com/JuliaData/Tables.jl/issues/new), even
just for a question, or come chat with us on the [#data](https://julialang.slack.com/messages/data/) slack
channel with questions, concerns, or clarifications. Also one can find list of packages that supports
Tables.jl interface in [INTEGRATIONS.md](https://github.com/JuliaData/Tables.jl/blob/master/INTEGRATIONS.md).

Please refer to [TableOperations.jl](https://github.com/JuliaData/TableOperations.jl) for common table operations such as  `select`, `transform`, `filter` and  `map`.

```@contents
Depth = 3
```

## Using the Interface (i.e. consuming Tables.jl-compatible sources)

We start by discussing _usage_ of the Tables.jl interface functions, since that can help contextualize _implementing_ them for custom table types.

At a high level, Tables.jl provides two powerful APIs for predictably accessing data from any table-like source:
```julia
# access data of input table `x` row-by-row
# Tables.rows must return a row iterator
rows = Tables.rows(x)

# we can iterate through each row
for row in rows
    # example of getting all values in the row
    # don't worry, there are other ways to more efficiently process rows
    rowvalues = [Tables.getcolumn(row, col) for col in Tables.columnnames(row)]
end

# access data of input table `x` column-by-column
# Tables.columns returns an object where individual, entire columns can be accessed
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

So we see two high-level functions here, [`Tables.rows`](@ref), and [`Tables.columns`](@ref).

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
    # create table using tablename and schema from input table
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
            # a user-provided function to the column value `val`, index `i`
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

This is pretty straightforward usage: it calls [`Tables.rows`](@ref) on the input table source,
and since we need the schema to setup the database table, we query it via [`Tables.schema`](@ref).
We then iterate the rows in our table via `for row in rows`, and use the convenient
[`Tables.eachcolumn`](@ref) to efficiently apply a function to each value in the row. Note that
we didn't call [`Tables.columnnames`](@ref) or [`Tables.getcolumn`](@ref) at all, since they're utilized
by [`Tables.eachcolumn`](@ref) itself. [`Tables.eachcolumn`](@ref) is optimized to provide type-stable, and even
constant-propagation of column index, name, and type in some cases to allow for efficient
consumption of row values.

One wrinkle to consider is the "unknown schema" case; i.e. what if our [`Tables.schema`](@ref)
call had returned `nothing` (this can be the case for exotic table sources like lazily mapped
transformations over rows in a table):
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
as a guide, we make a [`Tables.Schema`](@ref) object with just the column names, which we can
then still pass to [`Tables.eachcolumn`](@ref) to apply our `bind!` function to each row value.

### `Tables.columns` usage

Ok, now let's take a look at a case utilizing [`Tables.columns`](@ref).
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
calls [`Tables.columns`](@ref) on it, then [`Tables.columnnames`](@ref) to get the column names.
It then passes the [`Tables.AbstractColumns`](@ref)-compatible object to an internal function `fromcolumns`,
which dispatches on a special kind of `Tables.AbstractColumns` object called a [`Tables.CopiedColumns`](@ref),
which wraps any `Tables.AbstractColumns`-compatible object that has already had copies of its
columns made, and are thus safe for the columns-consumer to assume ownership of (this is because
DataFrames.jl, by default makes copies of all columns upon construction). In both cases, individual
columns are collected in `Vector{AbstractVector}`s by calling `Tables.getcolumn(x, nm)` for each column name.
A final note is the call to `getvector` on each column, which ensures each column is materialized
as an `AbstractVector`, as is required by the DataFrame constructor.

Note in both the rows and columns usages, we didn't need to worry about the natural orientation
of the input data; we just called [`Tables.rows`](@ref) or [`Tables.columns`](@ref) as was most natural for
the table-specific use-case, knowing that it will Just Work™️.

### Tables.jl Utilities

Before moving on to _implementing_ the Tables.jl interfaces, we take a quick
break to highlight some useful utility functions provided by Tables.jl:

```@docs
Tables.Schema
Tables.schema
Tables.subset
Tables.partitions
Tables.partitioner
Tables.rowtable
Tables.columntable
Tables.dictrowtable
Tables.dictcolumntable
Tables.namedtupleiterator
Tables.datavaluerows
Tables.nondatavaluerows
Tables.table
Tables.matrix
Tables.eachcolumn
Tables.materializer
Tables.columnindex
Tables.columntype
Tables.rowmerge
Tables.Row
Tables.Columns
```

## Implementing the Interface (i.e. becoming a Tables.jl source)

Now that we've seen how one _uses_ the Tables.jl interface, let's walk-through how to implement it; i.e. how can I
make my custom type valid for Tables.jl consumers?

For a type `MyTable`, the interface to becoming a proper table is straightforward:

| Required Methods                             | Default Definition           | Brief Description                                                                                                                                   |
|----------------------------------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `Tables.istable(::Type{MyTable})`            |                              | Declare that your table type implements the interface                                                                                               |
|  **One of:**                                 |                              |                                                                                                                                                     |
| `Tables.rowaccess(::Type{MyTable})`          |                              | Declare that your table type defines a `Tables.rows(::MyTable)` method                                                                              |
| `Tables.rows(x::MyTable)`                    |                              | Return an `Tables.AbstractRow`-compatible iterator from your table                                                                                  |
| **Or:**                                      |                              |                                                                                                                                                     |
| `Tables.columnaccess(::Type{MyTable})`       |                              | Declare that your table type defines a `Tables.columns(::MyTable)` method                                                                           |
| `Tables.columns(x::MyTable)`                 |                              | Return an `Tables.AbstractColumns`-compatible object from your table                                                                                |
| **Optional methods**                         |                              |                                                                                                                                                     |
| `Tables.schema(x::MyTable)`                  | `Tables.schema(x) = nothing` | Return a [`Tables.Schema`](@ref) object from your `Tables.AbstractRow` iterator or `Tables.AbstractColumns` object; or `nothing` for unknown schema |
| `Tables.materializer(::Type{MyTable})`       | `Tables.columntable`         | Declare a "materializer" sink function for your table type that can construct an instance of your type from any Tables.jl input                     |
| `Tables.subset(x::MyTable, inds; viewhint)`  |                              | Return a row or a sub-table of the original table                                                                                                   |
| `DataAPI.nrow(x::MyTable)`                   |                              | Return number of rows of table `x`                                                                                                                  |
| `DataAPI.ncol(x::MyTable)`                   |                              | Return number of columns of table `x`                                                                                                               |

Based on whether your table type has defined `Tables.rows` or `Tables.columns`, you then ensure that the `Tables.AbstractRow` iterator
or `Tables.AbstractColumns` object satisfies the respective interface.

As an additional source of documentation, see [this discourse post](https://discourse.julialang.org/t/struggling-to-implement-tables-jl-interface-for-vector-mystruct/42318/7?u=quinnj) outlining in detail a walk-through of making a row-oriented table.

### `Tables.AbstractRow`

```@docs
Tables.AbstractRow
```

### `Tables.AbstractColumns`

```@docs
Tables.AbstractColumns
```

### Implementation Example
As an extended example, let's take a look at some code defined in Tables.jl for treating `AbstractVecOrMat`s as tables.

First, we define a special `MatrixTable` type that will wrap an `AbstractVecOrMat`, and allow easy overloading for the
Tables.jl interface.

```julia
struct MatrixTable{T <: AbstractVecOrMat} <: Tables.AbstractColumns
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::T
end
# declare that MatrixTable is a table
Tables.istable(::Type{<:MatrixTable}) = true
# getter methods to avoid getproperty clash
names(m::MatrixTable) = getfield(m, :names)
matrix(m::MatrixTable) = getfield(m, :matrix)
lookup(m::MatrixTable) = getfield(m, :lookup)
# schema is column names and types
Tables.schema(m::MatrixTable{T}) where {T} = Tables.Schema(names(m), fill(eltype(T), size(matrix(m), 2)))
```

Here we defined `Tables.istable` for all `MatrixTable` types, signaling that they implement the Tables.jl interfaces.
We also defined [`Tables.schema`](@ref) by pulling the column names out that we stored, and since `AbstractVecOrMat` have a single
`eltype`, we repeat it for each column (the call to `fill`). Note that defining [`Tables.schema`](@ref) is optional on tables; by default, `nothing`
is returned and Tables.jl consumers should account for both known and unknown schema cases. Returning a schema when possible allows consumers
to have certain optimizations when they can know the types of all columns upfront (and if the # of columns isn't too large)
to generate more efficient code.

Now, in this example, we're actually going to have `MatrixTable` implement _both_ `Tables.rows` and `Tables.columns`
methods itself, i.e. it's going to return itself from those functions, so here's first how we make our `MatrixTable` a
valid `Tables.AbstractColumns` object:

```julia
# column interface
Tables.columnaccess(::Type{<:MatrixTable}) = true
Tables.columns(m::MatrixTable) = m
# required Tables.AbstractColumns object methods
Tables.getcolumn(m::MatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = matrix(m)[:, col]
Tables.getcolumn(m::MatrixTable, nm::Symbol) = matrix(m)[:, lookup(m)[nm]]
Tables.getcolumn(m::MatrixTable, i::Int) = matrix(m)[:, i]
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
# just return itself, which means MatrixTable must iterate `Tables.AbstractRow`-compatible objects
rows(m::MatrixTable) = m
# the iteration interface, at a minimum, requires `eltype`, `length`, and `iterate`
# for `MatrixTable` `eltype`, we're going to provide a custom row type
Base.eltype(m::MatrixTable{T}) where {T} = MatrixRow{T}
Base.length(m::MatrixTable) = size(matrix(m), 1)

Base.iterate(m::MatrixTable, st=1) = st > length(m) ? nothing : (MatrixRow(st, m), st + 1)

# a custom row type; acts as a "view" into a row of an AbstractVecOrMat
struct MatrixRow{T} <: Tables.AbstractRow
    row::Int
    source::MatrixTable{T}
end
# required `Tables.AbstractRow` interface methods (same as for `Tables.AbstractColumns` object before)
# but this time, on our custom row type
getcolumn(m::MatrixRow, ::Type, col::Int, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), col]
getcolumn(m::MatrixRow, i::Int) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), i]
getcolumn(m::MatrixRow, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), getfield(getfield(m, :source), :lookup)[nm]]
columnnames(m::MatrixRow) = names(getfield(m, :source))
```
Here we start by defining `Tables.rowaccess` and `Tables.rows`, and then the iteration interface methods,
since we declared that a `MatrixTable` itself is an iterator of `Tables.AbstractRow`-compatible objects. For `eltype`,
we say that a `MatrixTable` iterates our own custom row type, `MatrixRow`. `MatrixRow` subtypes
`Tables.AbstractRow`, which provides interface implementations for several useful behaviors (indexing,
iteration, property-access, etc.); essentially it makes our custom `MatrixRow` type more convenient to work with.

Implementing the `Tables.AbstractRow` interface is straightforward, and very similar to our implementation
of `Tables.AbstractColumns` previously (i.e. the same methods for `getcolumn` and `columnnames`).

And that's it. Our `MatrixTable` type is now a fully fledged, valid Tables.jl source and can be used throughout
the ecosystem. Now, this is obviously not a lot of code; but then again, the actual Tables.jl interface
implementations tend to be fairly simple, given the other behaviors that are already defined for table types
(i.e. table types tend to already have a `getcolumn` like function defined).

### `Tables.isrowtable`

One option for certain table types is to define `Tables.isrowtable` to automatically satisfy the Tables.jl interface.
This can be convenient for "natural" table types that already iterate rows.
```@docs
Tables.isrowtable
```

### Testing Tables.jl Implementations

One question that comes up is what the best strategies are for testing a Tables.jl implementation. Continuing with
our `MatrixTable` example, let's see some useful ways to test that things are working as expected.

```julia
mat = [1 4.0 "7"; 2 5.0 "8"; 3 6.0 "9"]
```

First, we define a matrix literal with three columns of various differently typed values.

```julia
# first, create a MatrixTable from our matrix input
mattbl = Tables.table(mat)
# test that the MatrixTable `istable`
@test Tables.istable(typeof(mattbl))
# test that it defines row access
@test Tables.rowaccess(typeof(mattbl))
@test Tables.rows(mattbl) === mattbl
# test that it defines column access
@test Tables.columnaccess(typeof(mattbl))
@test Tables.columns(mattbl) === mattbl
# test that we can access the first "column" of our matrix table by column name
@test mattbl.Column1 == [1,2,3]
# test our `Tables.AbstractColumns` interface methods
@test Tables.getcolumn(mattbl, :Column1) == [1,2,3]
@test Tables.getcolumn(mattbl, 1) == [1,2,3]
@test Tables.columnnames(mattbl) == [:Column1, :Column2, :Column3]
# now let's iterate our MatrixTable to get our first MatrixRow
matrow = first(mattbl)
@test eltype(mattbl) == typeof(matrow)
# now we can test our `Tables.AbstractRow` interface methods on our MatrixRow
@test matrow.Column1 == 1
@test Tables.getcolumn(matrow, :Column1) == 1
@test Tables.getcolumn(matrow, 1) == 1
@test propertynames(mattbl) == propertynames(matrow) == [:Column1, :Column2, :Column3]
```

So, it looks like our `MatrixTable` type is looking good. It's doing everything we'd expect with regards to accessing
its rows or columns via the Tables.jl API methods. Testing a table source like this is fairly straightforward since
we're really just testing that our interface methods are doing what we expect them to do.

Now, while we didn't go over a "sink" function for matrices in our walkthrough, there does indeed exist a `Tables.matrix` function that allows converting any table input source into a plain Julia `Matrix` object.

Having both Tables.jl "source" and "sink" implementations (i.e. a type that is a Tables.jl-compatible source,
as well as a way to _consume_ other tables), allows us to do some additional "round trip" testing:

```julia
rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
ct = (a=[1,2,3], b=[4.0, 5.0, 6.0])
```

In addition to our `mat` object earlier, we can define a couple simple "tables"; in this case `rt` is a kind of default "row table" as a `Vector` of `NamedTuple`s, while `ct` is a default "column table" as a `NamedTuple` of `Vector`s. Notice that they contain mostly the same data as our matrix literal earlier, yet in slightly different storage formats. These default "row" and "column" tables are supported by default in Tables.jl due do their natural table representations, and hence can be excellent tools in testing table integrations.

```julia
# let's turn our row table into a plain Julia Matrix object
mat = Tables.matrix(rt)
# test that our matrix came out like we expected
@test mat[:, 1] == [1, 2, 3]
@test size(mat) == (3, 3)
@test eltype(mat) == Any
# so we successfully consumed a row-oriented table,
# now let's try with a column-oriented table
mat2 = Tables.matrix(ct)
@test eltype(mat2) == Float64
@test mat2[:, 1] == ct.a

# now let's take our matrix input, and make a column table out of it
tbl = Tables.table(mat) |> columntable
@test keys(tbl) == (:Column1, :Column2, :Column3)
@test tbl.Column1 == [1, 2, 3]
# and same for a row table
tbl2 = Tables.table(mat2) |> rowtable
@test length(tbl2) == 3
@test map(x->x.Column1, tbl2) == [1.0, 2.0, 3.0]
```
