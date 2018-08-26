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
end

include("utils.jl")

"Abstract row type with a simple required interface: row values are accessible via `getproperty(row, field)`; for example, a NamedTuple like `nt = (a=1, b=2, c=3)` can access its value for `a` like `nt.a` which turns into a call to the function `getproperty(nt, :a)`"
abstract type Row end

"""
The Tables.jl package provides four useful interface functions for working with tabular data in a variety of formats.

```julia
    Tables.schema(table) => NamedTuple{names, types}
    Tables.AccessStyle(table_type) => Tables.RowAccess() || Tables.ColumnAccess()
    Tables.rows(table) => iterator with values accessible via getproperty(row, columnname)
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
  * overload `Tables.columns` for your table type (e.g. `Tables.columns(t::MyTableType)`), returning a collection of iterators, with individual column iterators accessible via column names by `getproperty(x, columnname)` (a NamedTuple of Vectors, for example, would satisfy the interface)

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
"""
abstract type Table end

abstract type AccessStyle end
struct RowAccess <: AccessStyle end
struct ColumnAccess <: AccessStyle end
AccessStyle(x) = RowAccess()

"Tables.schema(s) => NamedTuple{names, types}"
function schema end
schema(x) = eltype(x)

include("namedtuples.jl")

## generic fallbacks; if a table provides Tables.rows or Tables.columns,
## provide the inverse interface function

# generic row iteration of columns
struct ColumnsRow{T}
    columns::T # a ColumnTable; NamedTuple of Vectors
    row::Int
end

Base.getproperty(c::ColumnsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getproperty(getfield(c, 1), T, col, nm)[getfield(c, 2)]
Base.getproperty(c::ColumnsRow, nm::Symbol) = getproperty(getfield(c, 1), nm)[getfield(c, 2)]
Base.propertynames(c::ColumnsRow) = propertynames(c.columns)

struct RowIterator{NT, S}
    source::S # assumes we can call length(col) & getindex(col, row) on columns
end
RowIterator(::Type{NT}, x::S) where {NT <: NamedTuple, S} = RowIterator{NT, S}(x)
Base.eltype(x::RowIterator{NT, S}) where {NT, S} = ColumnsRow{S}
Base.length(x::RowIterator{NamedTuple{names, types}}) where {names, types} = length(getproperty(x.source, names[1]))

function Base.iterate(rows::RowIterator, st=1)
    st > length(rows) && return nothing
    return ColumnsRow(rows.source, st), st + 1
end

function rows(x::T) where {T}
    if AccessStyle(T) === ColumnAccess()
        return RowIterator(schema(x), columns(x))
    else
        return x # assume x implicitly implements row interface
    end
end

# build columns from rows
"""
    Tables.allocatecolumn(::Type{T}, len) => returns a column type (usually AbstractVector) w/ size to hold `len` elements
    
    Custom column types can override with an appropriate "scalar" element type that should dispatch to their column allocator.
"""
allocatecolumn(T, len) = Vector{T}(undef, len)

@inline function allocatecolumns(::Type{NT}, len) where {NT <: NamedTuple{names}} where {names}
    if @generated
        vals = Tuple(:(allocatecolumn($(fieldtype(NT, i)), len)) for i = 1:fieldcount(NT))
        return :(NamedTuple{names}(($(vals...),)))
    else
        return NamedTuple{names}(Tuple(allocatecolumn(fieldtype(NT, i), len) for i = 1:fieldcount(NT)))
    end
end

# add! will push! or setindex! a value depending on if the row-iterator HasLength or not
@inline add!(val, col::Int, nm::Symbol, ::Base.HasLength, nt, row) = setindex!(nt[col], val, row)
@inline add!(val, col::Int, nm::Symbol, T, nt, row) = push!(nt[col], val)

@inline function columns(x::T) where {T}
    @assert AccessStyle(T) === RowAccess()
    sch = schema(x)
    rowitr = rows(x)
    L = Base.IteratorSize(typeof(rowitr))
    len = L == Base.HasLength() ? length(rowitr) : 0
    nt = allocatecolumns(sch, len)
    for (i, row) in enumerate(rowitr)
        unroll(add!, sch, row, L, nt, i)
    end
    return nt
end

end # module
