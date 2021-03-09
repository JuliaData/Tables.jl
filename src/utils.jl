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
rowmerge(row; fields_to_merge...) = rowmerge(row, fields_to_merge.data)

_row_to_named_tuple(row::NamedTuple) = row
_row_to_named_tuple(row) = NamedTuple(Row(row))
