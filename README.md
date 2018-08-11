### Tables.jl

    The Tables.jl package provides three useful interface functions for working with tabular data in a variety of formats.

    `Tables.schema(table) => NamedTuple{names, types}`
    `Tables.rows(table) => Row-iterator`
    `Tables.columns(table) => NamedTuple of AbstractVectors`

Essentially, for any table type that implements the interface requirements, one can get access to:
    1) the schema of a table, returned as a NamedTuple _type_, with parameters of `names` which are the column names, and `types` which is a `Tuple{...}` type of types
    2) rows of the table via a Row-iterator
    3) the columns of the table via a NamedTuple of AbstractVectors, where the NamedTuple keys are the column names.

So how does one go about satisfying the Tables.Table interface? We must ensure the three methods get satisfied for a given table type.

    `Tables.schema`: given an _instance_ of a table type, generate a `NamedTuple` type, with a tuple of symbols for column names (e.g. `(:a, :b, :c)`), and a tuple type of types as the 2nd parameter (e.g. `Tuple{Int, Float64, String}`); like `NamedTuple{(:a, :b, :c), Tuple{Int, Float64, String}}`

    `Tables.rows`:
        - overload `Tables.rows` directly for your table type (e.g. `Tables.rows(t::MyTableType)`), and return an iterator of `Row`s. Where a `Row` type is any object with keys accessible via `getproperty(obj, key)`
        - define `Tables.producescells(::Type{<:MyTableType}) = true`, as well as `Tables.getcell(t::MyTableType, ::Type{T}, row::Int, col::Int)` and `Tables.isdonefunction(::Type{<:MyTableType})::Function`; a generic `Tables.rows(x)` implementation will use these definitions to generate a valid `Row` iterator for `MyTableType`

    `Tables.columns`:
        - overload `Tables.columns` directly for your table type (e.g. `Tables.columns(t::MyTableType)`), returning a NamedTuple of AbstractVectors, with keys being the column names
        - define `Tables.producescolumns(::Type{<:MyTableType}) = true`, as well as `Tables.getcolumn(t::MyTableType, ::Type{T}, col::Int)`; a generic `Tables.columns(x)` implementation will use these definitions to generate a valid NamedTuple of AbstractVectors for `MyTableType`
        - define `Tables.producescells(::Type{<:MyTableType}) = true`, as well as `Tables.getcell(t::MyTableType, ::Type{T}, row::Int, col::Int)` and `Tables.isdonefunction(::Type{<:MyTableType})::Function`; again, the generic `Tables.columns(x)` implementation can use these definitions to generate a valid NamedTuple of AbstractVectors for `MyTableType`

The final question is how `MyTableType` can be a "sink" for any other table type:

    - Define a function or constructor that takes, at a minimum, a single, untyped argument and then calls `Tables.rows` or `Tables.columns` on that argument to construct an instance of `MyTableType`

For example, if `MyTableType` is a row-oriented format, I might define my "sink" function like:
```julia
function MyTableType(x)
    mytbl = MyTableType(Tables.schema(x))
    for row in Tables.rows(x)
        append!(mytbl, row)
    end
    return mytbl
end
```
Alternatively, if `MyTableType` is column-oriented, perhaps my definition would be more like:
```
function MyTableType(x)
    cols = Tables.columns(x)
    return MyTableType(collect(map(String, keys(cols))), [col for col in cols])
end
```
Obviously every table type is different, but via a combination of `Tables.schema`, `Tables.rows`, and `Tables.columns`, each table type should be able to construct an instance of itself.