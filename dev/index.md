
<a id='Tables.jl-Documentation-1'></a>

# Tables.jl Documentation


This guide provides documentation around the powerful tables interfaces in the Tables.jl package. Note that the package, and hence, documentation, are geared towards package and library developers who intend to implement and consume the interfaces. Users, on the other hand, benefit from these other packages that provide useful access to table data in various formats or workflows.


With that said, don't hesitate to [open a new issue](https://github.com/JuliaData/Tables.jl/issues/new), even just for a question, or come chat with us on the [#data](https://julialang.slack.com/messages/data/) slack channel with question, concerns, or clarifications.

- [Tables.jl Documentation](index.md#Tables.jl-Documentation-1)
    - [Using the Interface (i.e. consuming Tables.jl sources)](index.md#Using-the-Interface-(i.e.-consuming-Tables.jl-sources)-1)
    - [Implementing the Interface (i.e. becoming a Tables.jl source)](index.md#Implementing-the-Interface-(i.e.-becoming-a-Tables.jl-source)-1)


<a id='Using-the-Interface-(i.e.-consuming-Tables.jl-sources)-1'></a>

## Using the Interface (i.e. consuming Tables.jl sources)


We start by discussing *usage* of the Tables.jl interfaces, since that can help contextualize *implementing* them.


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

<a id='Tables.rows' href='#Tables.rows'>#</a>
**`Tables.rows`** &mdash; *Function*.



```julia
Tables.rows(x) => Row iterator
```

Accesses data of input table source `x` row-by-row by returning a [`Row`](index.md#Tables.Row) iterator. Note that even if the input table source is column-oriented by nature, an efficient generic definition of `Tables.rows` is defined in Tables.jl to return an iterator of row views into the columns of the input.

The [`Tables.Schema`](@ref) of a `Row` iterator can be queried via `Tables.schema(rows)`, which may return `nothing` if the schema is unknown. Column names can be queried by calling `Tables.columnnames(row)` on an individual row. And row values can be accessed by calling `Tables.getcolumn(rows, i::Int )` or `Tables.getcolumn(rows, nm::Symbol)` with a column index or name, respectively.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL308-L321' class='documenter-source'>source</a><br>

<a id='Tables.columns' href='#Tables.columns'>#</a>
**`Tables.columns`** &mdash; *Function*.



```julia
Tables.columns(x) => Columns-compatible object
```

Accesses data of input table source `x` by returning a [`Columns`](index.md#Tables.Columns)-compatible object, which allows retrieving entire columns by name or index. A retrieved column is an object that is indexable and has a known length, i.e. supports  `length(col)` and `col[i]` for any `i = 1:length(col)`. Note that even if the input table source is row-oriented by nature, an efficient generic definition of `Tables.columns` is defined in Tables.jl to build a `Columns`- compatible object object from the input rows.

The [`Tables.Schema`](@ref) of a `Columns` object can be queried via `Tables.schema(columns)`, which may return `nothing` if the schema is unknown. Column names can be queried by calling `Tables.columnnames(columns)`. And individual columns can be accessed by calling `Tables.getcolumn(columns, i::Int )` or `Tables.getcolumn(columns, nm::Symbol)` with a column index or name, respectively.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL289-L305' class='documenter-source'>source</a><br>


Given these two powerful data access methods, let's walk through real, albeit somewhat simplified versions of how packages actually use these methods.


<a id='Tables.rows-usage-1'></a>

### `Tables.rows` usage


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


This is pretty straightforward usage: it calls `Tables.rows` on the input table source, and since we need the schema to setup the database table, we query it via `Tables.schema`. We then iterate the rows in our table via `for row in rows`, and use the convenient `Tables.eachcolumn` to efficiently apply a function to each value in the row. Note that we didn't call `Tables.columnnames` or `Tables.getcolumn` at all, since they're utilized by `Tables.eachcolumn` itself.


One wrinkle to consider is the "unknown schema" case; i.e. what if our [`Tables.schema`](@ref) call had returned `nothing`.


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


The strategy taken here is to start iterating the input source, and using the first row as a guide, we make a `Tables.Schema` object with just the column names, which we can then still pass to `Tables.eachcolumn` to apply our `bind!` function to each row value.


<a id='Tables.columns-usage-1'></a>

### `Tables.columns` usage


Ok, now let's take a look at a case utlizing `Tables.columns`. The following code is taken from the [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl/blob/master/src/other/tables.jl) Tables.jl implementation:


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


So here we have a generic `DataFrame` constructor that takes a single, untyped argument, calls `Tables.columns` on it, then `Tables.columnnames` to get the column names. It then passes the `Columns`-compatible object to an internal function `fromcolumns`, which dispatches on a special kind of `Columns` object called a [`Tables.CopiedColumns`](@ref), which wraps any `Columns` object that has already had copies of its columns made, and are thus safe for the columns-consumer to assume ownership of (this is because DataFrames.jl, by default makes copies of all columns upon construction). In both cases, individual columns are collected in `Vector{AbstractVector}`s by calling `Tables.getcolumn(x, nm)` for each column name. A final note is the call to `getvector` on each column, which ensures each column is materialized as an `AbstractVector`, as is required by the DataFrame constructor.


Note in both the rows and columns usages, we didn't need to worry about the natural orientation of the input data; we just called `Tables.rows` or `Tables.columns` as was most natural for the table-specific use-case, knowing that it will Just Work™️.


<a id='Tables.jl-Utilities-1'></a>

### Tables.jl Utilities


Before moving on to *implementing* the Tables.jl interfaces, we take a quick break to highlight some useful utility functions provided by Tables.jl:

<a id='Tables.rowtable' href='#Tables.rowtable'>#</a>
**`Tables.rowtable`** &mdash; *Function*.



```julia
Tables.rowtable(x) => Vector{NamedTuple}
```

Take any input table source, and produce a `Vector` of `NamedTuple`s, also known as a "row table". A "row table" is a kind of default table type of sorts, since it satisfies the Tables.jl row interface naturally, i.e. a `Vector` naturally iterates its elements, and `NamedTuple` satisifes the `Row` interface by default (allows indexing value by index, name, and getting all names).


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/namedtuples.jl#LL72-L81' class='documenter-source'>source</a><br>

<a id='Tables.columntable' href='#Tables.columntable'>#</a>
**`Tables.columntable`** &mdash; *Function*.



```julia
Tables.columntable(x) => NamedTuple of Vectors
```

Takes any input table source `x` and returns a `NamedTuple` of `Vector`s, also known as a "column table". A "column table" is a kind of default table type of sorts, since it satisfies the Tables.jl column interface naturally.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/namedtuples.jl#LL110-L117' class='documenter-source'>source</a><br>

<a id='Tables.namedtupleiterator' href='#Tables.namedtupleiterator'>#</a>
**`Tables.namedtupleiterator`** &mdash; *Function*.



```julia
Tables.namedtupleiterator(x)
```

Pass any table input source and return a `NamedTuple` iterator


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/namedtuples.jl#LL17-L21' class='documenter-source'>source</a><br>

<a id='Tables.datavaluerows' href='#Tables.datavaluerows'>#</a>
**`Tables.datavaluerows`** &mdash; *Function*.



```julia
Tables.datavaluerows(x) => NamedTuple iterator
```

Takes any table input `x` and returns a `NamedTuple` iterator that will replace missing values with `DataValue`-wrapped values; this allows any table type to satisfy the TableTraits.jl  Queryverse integration interface by defining: 

```
IteratorInterfaceExtensions.getiterator(x::MyTable) = Tables.datavaluerows(x)
```


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/tofromdatavalues.jl#LL81-L92' class='documenter-source'>source</a><br>

<a id='Tables.nondatavaluerows' href='#Tables.nondatavaluerows'>#</a>
**`Tables.nondatavaluerows`** &mdash; *Function*.



```julia
Tables.nondatavaluerows(x)
```

Takes any Queryverse-compatible `NamedTuple` iterator source and  converts to a Tables.jl-compatible `Row` iterator. Will automatically unwrap any `DataValue`s, replacing `NA` with `missing`. Useful for translating Query.jl results back to non-`DataValue`-based tables.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/tofromdatavalues.jl#LL16-L23' class='documenter-source'>source</a><br>

<a id='Tables.table' href='#Tables.table'>#</a>
**`Tables.table`** &mdash; *Function*.



```julia
Tables.table(m::AbstractMatrix; [header::Vector{Symbol}])
```

Wrap an `AbstractMatrix` (`Matrix`, `Adjoint`, etc.) in a `MatrixTable`, which satisfies the Tables.jl interface. This allows accesing the matrix via `Tables.rows` and `Tables.columns`. An optional keyword argument `header` can be passed as a `Vector{Symbol}` to be used as the column names. Note that no copy of the `AbstractMatrix` is made.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/matrix.jl#LL45-L52' class='documenter-source'>source</a><br>

<a id='Tables.matrix' href='#Tables.matrix'>#</a>
**`Tables.matrix`** &mdash; *Function*.



```julia
Tables.matrix(table; transpose::Bool=false)
```

Materialize any table source input as a `Matrix`. If the table column types are not homogenous, they will be promoted to a common type in the materialized `Matrix`. Note that column names are ignored in the conversion. By default, input table columns will be materialized as corresponding matrix columns; passing `transpose=true` will transpose the input with input columns as matrix rows.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/matrix.jl#LL59-L66' class='documenter-source'>source</a><br>

<a id='Tables.eachcolumn' href='#Tables.eachcolumn'>#</a>
**`Tables.eachcolumn`** &mdash; *Function*.



```julia
Tables.eachcolumn(f, sch, row, args...)
Tables.eachcolumn(Tables.columns(x))
```

The first definition takes a function `f`, table schema `sch`, a `row` object (that satisfies the `Row` interface), and any other `args...`; it generates calls to get the value for each column in the row (`Tables.getcolumn(row, nm)`) and then calls `f(val, col, name, args...)`, where `f` is the user-provided function, `val` is a row's column value, `col` is the column index as an `Int`, and `name` is the row's column name as a `Symbol`.

While the first definition applies to a `Row` object, the 2nd definition applies to a `Columns` object, which simply iterates each column. For example, one could get every column of a `Columns` object by doing:

```julia
vectors = [col for col in Tables.eachcolumn(Tables.columns(x))]
```


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/utils.jl#LL25-L38' class='documenter-source'>source</a><br>

<a id='Tables.materializer' href='#Tables.materializer'>#</a>
**`Tables.materializer`** &mdash; *Function*.



```julia
Tables.materializer(x) => Callable
```

For a table input, return the "sink" function or "materializing" function that can take a Tables.jl-compatible table input and make an instance of the table type. This enables "transform" workflows that take table inputs, apply transformations, potentially converting the table to a different form, and end with producing a table of the same type as the original input. The default materializer is `Tables.columntable`, which converts any table input into a `NamedTuple` of `Vector`s.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL274-L283' class='documenter-source'>source</a><br>

<a id='Tables.columnindex' href='#Tables.columnindex'>#</a>
**`Tables.columnindex`** &mdash; *Function*.



given names and a Symbol `name`, compute the index (1-based) of the name in names


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/utils.jl#LL205' class='documenter-source'>source</a><br>


```
Tables.columnindex(table, name)
```

Return the column index (1-based) of a column by `name` in a table with a known schema; returns 0 if `name` doesn't exist in table


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL395-L399' class='documenter-source'>source</a><br>

<a id='Tables.columntype' href='#Tables.columntype'>#</a>
**`Tables.columntype`** &mdash; *Function*.



given tuple type and a Symbol `name`, compute the type of the name in the tuples types


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/utils.jl#LL217' class='documenter-source'>source</a><br>


```
Tables.columntype(table, name)
```

Return the column type of a column by `name` in a table with a known schema; returns Union{} if `name` doesn't exist in table


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL402-L406' class='documenter-source'>source</a><br>


<a id='Implementing-the-Interface-(i.e.-becoming-a-Tables.jl-source)-1'></a>

## Implementing the Interface (i.e. becoming a Tables.jl source)


Now that we've seen how one *uses* the Tables.jl interface, let's walk-through how to implement it; i.e. how can I make my custom type valid for Tables.jl consumers?


The interface to becoming a proper table is straightforward: | Required Methods             | Default Definition           | Brief Description                                                                                                               | |–––––––––––––––|–––––––––––––––|––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––-| | `Tables.istable(table)`      |                              | Declare that your table type implements the interface                                                                           | |  **One of:**                 |                              |                                                                                                                                 | | `Tables.rowaccess(table)`    |                              | Declare that your table type defines a `Tables.rows(table)` method                                                              | | `Tables.rows(table)`         |                              | Return a `Row` iterator from your table                                                                                         | | **Or:**                      |                              |                                                                                                                                 | | `Tables.columnaccess(table)` |                              | Declare that your table type defines a `Tables.columns(table)` method                                                           | | `Tables.columns(table)`      |                              | Return a `Columns`-compatible object from your table                                                                            | | **Optional methods**         |                              |                                                                                                                                 | | `Tables.schema(x)`           | `Tables.schema(x) = nothing` | Return a `Tables.Schema` object from your `Row` iterator or `Columns` object; or `nothing` for unknown schema                   | | `Tables.materializer(table)` | `Tables.columntable`         | Declare a "materializer" sink function for your table type that can construct an instance of your type from any Tables.jl input |


Based on whether your table type has defined `Tables.rows` or `Tables.columns`, you then ensure that the `Row` iterator or `Columns` object satisfies the respective interface:

<a id='Tables.Row' href='#Tables.Row'>#</a>
**`Tables.Row`** &mdash; *Type*.



```julia
Tables.Row
```

An interface type that represents a single row of a table, with column values retrievable by name or index. The high-level [`Tables.rows`](index.md#Tables.rows) function returns a `Row`-compatible iterator from any input table source.

Any object implements the `Row` interface, by satisfying the following: | Required Methods                                       | Default Definition        | Brief Description                                                                                                                                                | |––––––––––––––––––––––––––––|–––––––––––––-|–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––| | `Tables.getcolumn(row, i::Int)`                        | getfield(row, i)          | Retrieve a column value by index                                                                                                                                 | | `Tables.getcolumn(row, nm::Symbol)`                    | getproperty(row, nm)      | Retrieve a column value by name                                                                                                                                  | | `Tables.columnnames(row)`                              | propertynames(row)        | Return column names for a row as an indexable collection                                                                                                         | | **Optional methods**                                   |                           |                                                                                                                                                                  | | `Tables.getcolumn(row, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(row, nm) | Given a column type `T`, index `i`, and column name `nm`, retrieve the column value. Provides a type-stable or even constant-prop-able mechanism for efficiency. |

Note that custom row types shouldn't subtype `Row`, as it is purely an interface type to help document the Tables.jl API. See the [`Tables.AbstractRow`](index.md#Tables.AbstractRow) type for a type to potentially subtype to gain useful default behaviors.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL62-L81' class='documenter-source'>source</a><br>

<a id='Tables.Columns' href='#Tables.Columns'>#</a>
**`Tables.Columns`** &mdash; *Type*.



```julia
Tables.Columns
```

An interface type defined as an ordered set of columns that support retrieval of individual columns by name or index. A retrieved column must be an indexable collection with known length, i.e. an object that supports `length(col)` and `col[i]` for any `i = 1:length(col)`. The high-level [`Tables.columns`](index.md#Tables.columns) function returns a `Columns`-compatible object from any input table source.

Any object implements the `Columns` interface, by satisfying the following: | Required Methods                                         | Default Definition          | Brief Description                                                                                                                                            | |–––––––––––––––––––––––––––––|––––––––––––––-|–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––| | `Tables.getcolumn(table, i::Int)`                        | getfield(table, i)          | Retrieve a column by index                                                                                                                                   | | `Tables.getcolumn(table, nm::Symbol)`                    | getproperty(table, nm)      | Retrieve a column by name                                                                                                                                    | | `Tables.columnnames(table)`                              | propertynames(table)        | Return column names for a table as an indexable collection                                                                                                   | | **Optional methods**                                     |                             |                                                                                                                                                              | | `Tables.getcolumn(table, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(table, nm) | Given a column eltype `T`, index `i`, and column name `nm`, retrieve the column. Provides a type-stable or even constant-prop-able mechanism for efficiency. |

Note that table sources shouldn't subtype `Columns`, as it is purely an interface type to help document the Tables.jl API. See the [`Tables.AbstractColumns`](index.md#Tables.AbstractColumns) type for a type to potentially subtype to gain useful default behaviors.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL11-L33' class='documenter-source'>source</a><br>


Though the strict requirements for `Row` and `Columns` are minimal (just `getcolumn` and `columnnames`), you may desire additional behavior for your row or columns types (and you're implementing them yourself). For convenience, Tables.jl defines the `Tables.AbstractRow` and `Tables.AbstractColumns` abstract types, to allow subtyped custom types to inherit convenient behavior, such as indexing, iteration, and property access, all defined in terms of `getcolumn` and `columnnames`.

<a id='Tables.AbstractRow' href='#Tables.AbstractRow'>#</a>
**`Tables.AbstractRow`** &mdash; *Type*.



```julia
Tables.AbstractRow
```

Abstract type provided to allow custom row types to inherit useful and required behavior. Note that this type is for convenience for table *source* authors to provide useful default behavior to their `Row` object, and not to be used or relied upon by sink authors to dispatch on; i.e. not all `Row` objects will inherit from `Tables.AbstractRow`.

Interface definition: | Required Methods                                       | Default Definition        | Brief Description                                                                                                                                                | |––––––––––––––––––––––––––––|–––––––––––––-|–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––| | `Tables.getcolumn(row, i::Int)`                        | getfield(row, i)          | Retrieve a column value by index                                                                                                                                 | | `Tables.getcolumn(row, nm::Symbol)`                    | getproperty(row, nm)      | Retrieve a column value by name                                                                                                                                  | | `Tables.columnnames(row)`                              | propertynames(row)        | Return column names for a row as an indexable collection                                                                                                         | | **Optional methods**                                   |                           |                                                                                                                                                                  | | `Tables.getcolumn(row, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(row, nm) | Given a column type `T`, index `i`, and column name `nm`, retrieve the column value. Provides a type-stable or even constant-prop-able mechanism for efficiency. |

While custom row types aren't required to subtype `Tables.AbstractRow`, benefits of doing so include:

  * Indexing interface defined (using `getcolumn`); i.e. `row[i]` will return the column value at index `i`
  * Property access interface defined (using `columnnames` and `getcolumn`); i.e. `row.col1` will retrieve the value for the column named `col1`
  * Iteration interface defined; i.e. `for x in row` will iterate each column value in the row
  * A default `show` method

This allows the custom row type to behave as close as possible to a builtin `NamedTuple` object.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL84-L107' class='documenter-source'>source</a><br>

<a id='Tables.AbstractColumns' href='#Tables.AbstractColumns'>#</a>
**`Tables.AbstractColumns`** &mdash; *Type*.



```julia
Tables.AbstractColumns
```

Abstract type provided to allow custom table types to inherit useful and required behavior. Note that this type is for convenience for table *source* authors to provide useful default behavior to their `Columns` object, and not to be used or relied upon by sink authors to dispatch on; i.e. not all `Columns` objects will inherit from `Tables.AbstractColumns`.

Interface definition: | Required Methods                                         | Default Definition          | Brief Description                                                                                                                                            | |–––––––––––––––––––––––––––––|––––––––––––––-|–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––| | `Tables.getcolumn(table, i::Int)`                        | getfield(table, i)          | Retrieve a column by index                                                                                                                                   | | `Tables.getcolumn(table, nm::Symbol)`                    | getproperty(table, nm)      | Retrieve a column by name                                                                                                                                    | | `Tables.columnnames(table)`                              | propertynames(table)        | Return column names for a table as an indexable collection                                                                                                   | | **Optional methods**                                     |                             |                                                                                                                                                              | | `Tables.getcolumn(table, ::Type{T}, i::Int, nm::Symbol)` | Tables.getcolumn(table, nm) | Given a column eltype `T`, index `i`, and column name `nm`, retrieve the column. Provides a type-stable or even constant-prop-able mechanism for efficiency. |

While custom table types aren't required to subtype `Tables.AbstractColumns`, benefits of doing so include:

  * Indexing interface defined (using `getcolumn`); i.e. `tbl[i]` will retrieve the column at index `i`
  * Property access interface defined (using `columnnames` and `getcolumn`); i.e. `tbl.col1` will retrieve column named `col1`
  * Iteration interface defined; i.e. `for col in table` will iterate each column in the table
  * A default `show` method

This allows a custom table type to behave as close as possible to a builtin `NamedTuple` of vectors object.


<a target='_blank' href='https://github.com/JuliaData/Tables.jl/blob/e57eb74fdddedff0ab778720345d1d903d2fc79f/src/Tables.jl#LL36-L59' class='documenter-source'>source</a><br>


As an extended example, let's take a look at some code defined in Tables.jl for treating `AbstractMatrix`s as tables.


First, we define a special `MatrixTable` type that will wrap an `AbstractMatrix`, and allow easy overloading for the  Tables.jl interface.


```julia
struct MatrixTable{T <: AbstractMatrix} <: Tables.AbstractColumns
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    matrix::T
end
# declare that MatrixTable is a table
Tables.istable(::Type{<:MatrixTable}) = true
# getter method on stored column names
names(m::MatrixTable) = getfield(m, :names)
# schema is column names and types
Tables.schema(m::MatrixTable{T}) where {T} = Tables.Schema(names(m), fill(eltype(T), size(getfield(m, :matrix), 2)))
```


Here we defined `Tables.istable` for all `MatrixTable` types, signaling that my type implements the Tables.jl interfaces. We also defined `Tables.schema` by pulling the column names out that we stored, and since `AbstractMatrix` have a single `eltype`, we repeat it for each column. Note that defining `Tables.schema` is optional on tables; by default, `nothing` is returned and Tables.jl consumers should account for both known and unknown schema cases. It tends to allow consumers to have certain optimizations when they can know the types of all columns upfront (and if the # of columns isn't too large) to generate more efficient code.


Now, in this example, we're actually going to have `MatrixTable` implement *both* `Tables.rows` and `Tables.columns` methods itself, i.e. it's going to return itself from those functions, so here's first how we make our `MatrixTable` a valid `Columns` object:


```julia
# column interface
Tables.columnaccess(::Type{<:MatrixTable}) = true
Tables.columns(m::MatrixTable) = m
# required Columns object methods
Tables.getcolumn(m::MatrixTable, ::Type{T}, col::Int, nm::Symbol) where {T} = getfield(m, :matrix)[:, col]
Tables.getcolumn(m::MatrixTable, nm::Symbol) = getfield(m, :matrix)[:, getfield(m, :lookup)[nm]]
Tables.getcolumn(m::MatrixTable, i::Int) = getfield(m, :matrix)[:, i]
Tables.columnnames(m::MatrixTable) = names(m)
```


We define `columnaccess` for our type, then `columns` just returns the `MatrixTable` itself, and then we define the three `getcolumn` methods and `columnnames`. Note the use of a `lookup` Dict that maps column name to column index so we can figure out which column to return from the matrix. We're also storing the column names in our `names` field so the `columnnames` implementation is trivial. And that's it! Literally! It can now be written out to a csv file, stored in a sqlite or other database, converted to DataFrame or JuliaDB table, etc. Pretty fun.


And now for the `Tables.rows` implementation:


```julia
# declare that any MatrixTable defines its own `Tables.rows` method
rowaccess(::Type{<:MatrixTable}) = true
# just return itself, which means MatrixTable must iterate `Row`-compatible objects
rows(m::MatrixTable) = m
# the iteration interface, at a minimum, requires `eltype`, `length`, and `iterate`
# for `MatrixTable` `eltype`, we're going to provide a custom row type
Base.eltype(m::MatrixTable{T}) where {T} = MatrixRow{T}
Base.length(m::MatrixTable) = size(getfield(m, :matrix), 1)

Base.iterate(m::MatrixTable, st=1) = st > length(m) ? nothing : (MatrixRow(st, m), st + 1)

# a custom Row type; acts as a "view" into a row of an AbstractMatrix
struct MatrixRow{T} <: Tables.AbstractRow
    row::Int
    source::MatrixTable{T}
end
# required `Row` interface methods (same as for `Columns` object before)
getcolumn(m::MatrixRow, ::Type, col::Int, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), col]
getcolumn(m::MatrixRow, i::Int) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), i]
getcolumn(m::MatrixRow, nm::Symbol) =
    getfield(getfield(m, :source), :matrix)[getfield(m, :row), getfield(getfield(m, :source), :lookup)[nm]]
columnnames(m::MatrixRow) = names(getfield(m, :source))
```


Here we start by defining `Tables.rowaccess` and `Tables.rows`, and then the iteration interface methods, since we declared that a `MatrixTable` itself is an iterator of `Row`-compatible objects. For `eltype`, we say that a `MatrixTable` iterates our own custom row type, `MatrixRow`. `MatrixRow` subtypes `Tables.AbstractRow`, which has the same required interface as a `Row` object, but also provides interface implementations for several useful behaviors (indexing, iteration, property-access, etc.); essentially it makes our custom `MatrixRow` type more convenient to work with.


Implementing the `Row`/`Tables.AbstractRow` interface is straightfoward, and very similar to our implementation of `Columns` previously (i.e. the same methods for `getcolumn` and `columnnames`).


And that's it. Our `MatrixTable` type is now a fully fledged, valid Tables.jl source and can be used throughout the ecosystem. Now, this is obviously not a lot of code; but then again, the actual Tables.jl interface implementations tend to be fairly simple, given the other behaviors that are already defined for table types (i.e. table types tend to already have a `getcolumn` like function defined).

