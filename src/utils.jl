"helper function to calculate a run-length encoding of a tuple type"
Base.@pure function runlength(::Type{T}) where {T <: Tuple}
    rle = Tuple{Type, Int}[]
    fieldcount(T) == 0 && return rle
    curT = fieldtype(T, 1)
    prevT = curT
    len = 1
    for i = 2:fieldcount(T)
        @inbounds curT = fieldtype(T, i)
        if curT === prevT
            len += 1
        else
            push!(rle, (prevT, len))
            prevT = curT
            len = 1
        end
    end
    push!(rle, (curT, len))
    return rle
end

"""
    Tables.eachcolumn(f, sch::Tables.Schema{names, types}, x::Union{Tables.AbstractRow, Tables.AbstractColumns})
    Tables.eachcolumn(f, sch::Tables.Schema{names, nothing}, x::Union{Tables.AbstractRow, Tables.AbstractColumns})

Takes a function `f`, table schema `sch`, `x`, which is an object that satisfies the `AbstractRow` or `AbstractColumns` interfaces;
it generates calls to get the value for each column (`Tables.getcolumn(x, nm)`) and then calls `f(val, index, name)`, where `f` is the
user-provided function, `val` is the column value (`AbstractRow`) or entire column (`AbstractColumns`), `index` is the column index as an `Int`,
and `name` is the column name as a `Symbol`.

An example using `Tables.eachcolumn` is:
```julia
rows = Tables.rows(tbl)
sch = Tables.schema(rows)
if sch === nothing
    state = iterate(rows)
    state === nothing && return
    row, st = state
    sch = Tables.schema(Tables.columnnames(row), nothing)
    while state !== nothing
        Tables.eachcolumn(sch, row) do val, i, nm
            bind!(stmt, i, val)
        end
        state = iterate(rows, st)
        state === nothing && return
        row, st = state
    end
else
    for row in rows
        Tables.eachcolumn(sch, row) do val, i, nm
            bind!(stmt, i, val)
        end
    end
end
```
Note in this example we account for the input table potentially returning `nothing` from `Tables.schema(rows)`; in that case, we start
iterating the rows, and build a partial schema using the column names from the first row `sch = Tables.schema(Tables.columnnames(row), nothing)`,
which is valid to pass to `Tables.eachcolumn`.
"""
function eachcolumn end

quot(s::Symbol) = Meta.QuoteNode(s)
quot(x::Int) = x

@inline function eachcolumn(f::F, sch::Schema{names, types}, row::T) where {F, names, types, T}
    N = fieldcount(types)
    if N <= SPECIALIZATION_THRESHOLD
        Base.@nexprs 100 i -> begin
            if i <= N
                f(getcolumn(row, fieldtype(types, i), i, names[i]), i, names[i])
            end
        end
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, fieldtype(types, i), i, nm), i, nm)
        end
    end
    return
end

@inline function eachcolumn(f::F, sch::Schema{names, nothing}, row::T) where {F, names, T}
    N = length(names)
    if N <= SPECIALIZATION_THRESHOLD
        Base.@nexprs 100 i -> begin
            if i <= N
                f(getcolumn(row, names[i]), i, names[i])
            end
        end
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, nm), i, nm)
        end
    end
    return
end

@inline function eachcolumn(f::F, sch::Schema{nothing, nothing}, row::T) where {F, T}
    for (i, nm) in enumerate(sch.names)
        f(getcolumn(row, nm), i, nm)
    end
    return
end

# these are specialized `eachcolumn`s where we also want
# the indexing of `columns` to be constant propagated, so it needs to be returned from the generated function
@inline function eachcolumns(f::F, sch::Schema{names, types}, row::T, columns::S, args...) where {F, names, types, T, S}
    N = fieldcount(types)
    if N <= SPECIALIZATION_THRESHOLD
        Base.@nexprs 100 i -> begin
            if i <= N
                f(getcolumn(row, fieldtype(types, i), i, names[i]), i, names[i], columns[i], args...)
            end
        end
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, fieldtype(types, i), i, nm), i, nm, columns[i], args...)
        end
    end
    return
end

@inline function eachcolumns(f::F, sch::Schema{names, nothing}, row::T, columns::S, args...) where {F, names, T, S}
    N = length(names)
    if N <= SPECIALIZATION_THRESHOLD
        Base.@nexprs 100 i -> begin
            if i <= N
                f(getcolumn(row, names[i]), i, names[i], columns[i], args...)
            end
        end
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, nm), i, nm, columns[i], args...)
        end
    end
    return
end

@inline function eachcolumns(f::F, sch::Schema{nothing, nothing}, row::T, columns::S, args...) where {F, T, S}
    for (i, nm) in enumerate(sch.names)
        f(getcolumn(row, nm), i, nm, columns[i], args...)
    end
    return
end

"""
    rowmerge(row, other_rows...)
    rowmerge(row; fields_to_merge...)

Return a `NamedTuple` by merging `row` (an `AbstractRow`-compliant value) with `other_rows`
(one or more `AbstractRow`-compliant values) via `Base.merge`. This function is similar to
`Base.merge(::NamedTuple, ::NamedTuple...)`, but accepts `AbstractRow`-compliant values
instead of `NamedTuple`s.

A convenience method `rowmerge(row; fields_to_merge...) = rowmerge(row, fields_to_merge)`
is defined that enables the `fields_to_merge` to be specified as keyword arguments.
"""
rowmerge(row, other) = merge(_row_to_named_tuple(row), _row_to_named_tuple(other))
rowmerge(row, other, more...) = merge(_row_to_named_tuple(row), rowmerge(other, more...))
rowmerge(row; fields_to_merge...) = rowmerge(row, values(fields_to_merge))

_row_to_named_tuple(row::NamedTuple) = row
_row_to_named_tuple(row) = NamedTuple(Row(row))

"""
    ByRow <: Function

`ByRow(f)` returns a function which applies function `f` to each element in a vector.

`ByRow(f)` can be passed two types of arguments:
- One or more 1-based `AbstractVector`s of equal length: In this case the returned value
  is a vector resulting from applying `f` to elements of passed vectors element-wise.
  Function `f` is called exactly once for each element of passed vectors (as opposed to `map`
  which assumes for some types of source vectors (e.g. `SparseVector`) that the
  wrapped function is pure, and may call the function `f` only once for multiple
  equal values.
- A `Tables.ColumnTable` holding 1-based columns of equal length: In this case the function
  `f` is passed a `NamedTuple` created for each row of passed table.

The return value of `ByRow(f)` is always a vector.

`ByRow` expects that at least one argument is passed to it and in the case of
`Tables.ColumnTable` passed that the table has at least one column. In some
contexts of operations on tables (for example `DataFrame`) the user might want
to pass no arguments (or an empty `Tables.ColumnTable`) to `ByRow`. This case
must be separately handled by the code implementing the logic of processing the
`ByRow` operation on this specific parent table (the reason is that passing such
arguments to `ByRow` does not allow it to determine the number of rows of the
source table).

# Examples
```
julia> Tables.ByRow(x -> x^2)(1:3)
3-element Vector{Int64}:
 1
 4
 9

julia> Tables.ByRow((x, y) -> x*y)(1:3, 2:4)
3-element Vector{Int64}:
  2
  6
 12

julia> Tables.ByRow(x -> x.a)((a=1:2, b=3:4))
2-element Vector{Int64}:
 1
 2

 julia> Tables.ByRow(x -> (a=x.a*2, b=sin(x.b), c=x.c))((a=[1, 2, 3],
                                                         b=[1.2, 3.4, 5.6],
                                                         c=["a", "b", "c"]))
3-element Vector{NamedTuple{(:a, :b, :c), Tuple{Int64, Float64, String}}}:
 (a = 2, b = 0.9320390859672263, c = "a")
 (a = 4, b = -0.2555411020268312, c = "b")
 (a = 6, b = -0.6312666378723216, c = "c")

```
"""
struct ByRow{T} <: Function
    fun::T
end

# invoke the generic AbstractVector function to ensure function is called
# exactly once for each element
function (f::ByRow)(cols::AbstractVector...)
    if !(all(col -> ==(length(first(cols)))(length(col)) && firstindex(col) == 1, cols))
        throw(ArgumentError("All passed vectors must have the same length and use 1-based indexing"))
    end
    return invoke(map,
                  Tuple{typeof(f.fun), ntuple(i -> AbstractVector, length(cols))...},
                  f.fun, cols...)
end

function (f::ByRow)(table::ColumnTable)
    if !(all(col -> ==(length(first(table)))(length(col)) && firstindex(col) == 1, table))
        throw(ArgumentError("All passed vectors must have the same length and use 1-based indexing"))
    end
    return [f.fun(nt) for nt in Tables.namedtupleiterator(table)]
end

(f::ByRow)() = throw(ArgumentError("no arguments passed"))
(f::ByRow)(::NamedTuple{(), Tuple{}}) =
    throw(ArgumentError("no columns passed in Tables.ColumnTable"))
