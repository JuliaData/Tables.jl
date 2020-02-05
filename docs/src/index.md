# Tables.jl Documentation

This guide provides documentation around the powerful tables interfaces in the Tables.jl package.
Note that the package, and hence, documentation, are geared towards package and library developers
who intend to implement and consume the interfaces. Users, on the other hand, benefit from these
other packages that provide useful access to table data in various formats or workflows.

With that said, don't hesitate to [open a new issue](https://github.com/JuliaData/Tables.jl/issues/new), even
just for a question, or come chat with us on the [#data](https://julialang.slack.com/messages/data/) slack
channel with question, concerns, or clarifications.

```@contents
```

## Using the Interface (i.e. consuming Tables.jl sources)

We start by discussing _usage_ of the Tables.jl interfaces, since that can help contextualize _implementing_ them.

At a high level, Tables.jl provides two powerful APIs for predictably accessing data from any table-like source:
```julia
# access data of input table `x` row-by-row
rows = Tables.rows(x)

for row in rows
    # example of getting all values in the row
    # there are other ways to more efficiently process rows
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

### Tables.rows usage

First up, let's take a look at the [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl) package and how it uses the Tables.jl interface to allow loading of generic table-like data into a sqlite relational table. Here's the code:
```julia
function load!(table, db::DB, tablename)
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
            # to our parameterized SQl INSERT statement and then
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
function load!(sch::Nothing, rows, db::DB, tablename)
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

### Tables.columns usage

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
It then passes the `Columns`-compatible object to an internal function `fromcolumns`,
which dispatches on a special kind of `Columns` object called a [`Tables.CopiedColumns`](@ref),
which wraps any `Columns` object that has already had copies of its columns made, and are thus
safe for the columns-consumer to assume ownership of (this is because DataFrames.jl, by default
makes copies of all columns upon construction). In both cases, individual columns are collected
in `Vector{AbstractVector}`s by calling `Tables.getcolumn(x, nm)` for each column name.
A final note is the call to `getvector` on each column, which ensures each column is materialized
as an `AbstractVector`, as is required by the DataFrame constructor.

Note in the both the rows and columns usages, we didn't need to worry about the natural orientation
of the input data; we just called `Tables.rows` or `Tables.columns` as was most natural for
the table-specific use-case, knowing that it will Just Work™️.

### Tables.jl Utilities

Before moving on to _implementing_ the Tables.jl interfaces, we take a quick
break to highlight some useful utility functions provided by Tables.jl:
```@docs
Tables.rowtable
Tables.columntable
Tables.datavaluerows
Tables.nondatavaluerows
Tables.table
Tables.matrix
Tables.eachcolumn
Tables.materializer
Tables.columnindex
Tables.columntype
```

## Implementing the Interface (i.e. becoming a Tables.jl source)

