### Tables.jl

The Tables.jl package provides four useful interface functions for working with tabular data in a variety of formats.

```julia
    Tables.schema(table) => NamedTuple{names, types}
    Tables.AccessStyle(table_type) => Tables.RowAccess() | Tables.ColumnAccess()
    Tables.rows(table) => iterator with values accessible vai getproperty(row, columnname)
    Tables.columns(table) => Collection of iterators, with each column accessible via getproperty(x, columnname)
```

Essentially, for any table type that implements the interface requirements, one can get access to:
    1. the schema of a table, returned as a NamedTuple _type_, with parameters of `names` which are the column names, and `types` which is a `Tuple{...}` type of types
    2. rows of the table via a Row-iterator
    3. the columns of the table via a collection of iterators, where the individual column iterators are accessible via getproperty(table, columnname)

So how does one go about satisfying these interface functions? It mainly depends on the `Tables.AccessStyle(T)` of your table:

* `Tables.schema(x)`: given an _instance_ of a table type, generate a `NamedTuple` type, with a tuple of symbols for column names (e.g. `(:a, :b, :c)`), and a tuple type of types as the 2nd parameter (e.g. `Tuple{Int, Float64, String}`); like `NamedTuple{(:a, :b, :c), Tuple{Int, Float64, String}}`

* `Tables.RowAccess()`:
  * overload `Tables.rows` for your table type (e.g. `Tables.rows(t::MyTableType)`), and return an iterator of `Row`s. Where a `Row` type is any object with keys accessible via `getproperty(obj, key)` (like a NamedTuple type)

* `Tables.ColumnAccess()`:
  * overload `Tables.columns` for your table type (e.g. `Tables.columns(t::MyTableType)`), returning a collection of iterators, with individual column iterators accessible via column names by `getproperty(x, columnname)` (like a NamedTuple of Vectors, for examples)

The final question is how `MyTableType` can be a "sink" for any other table type:

* Define a function or constructor that takes, at a minimum, a single, untyped argument and then calls `Tables.rows` or `Tables.columns` on that argument to construct an instance of `MyTableType`

For example, if `MyTableType` is a row-oriented format, I might define my "sink" function like:
```julia
function MyTableType(x)
    mytbl = MyTableType(Tables.schema(x)) # custom constructor that creates an "empty" MyTableType w/ the right schema
    for row in Tables.rows(x)
        append!(mytbl, row)
    end
    return mytbl
end
```

Alternatively, if `MyTableType` is column-oriented, perhaps my definition would be more like:

```julia
function MyTableType(x)
    cols = Tables.columns(x)
    return MyTableType(collect(map(String, keys(cols))), [col for col in cols])
end
```

Obviously every table type is different, but via a combination of `Tables.schema`, `Tables.rows`, and `Tables.columns`, each table type should be able to construct an instance of itself.