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

@inline function eachcolumn(f::Base.Callable, sch::Schema{names, types}, row) where {names, types}
    if @generated
        if length(names) < 101
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getcolumn(row, $(fieldtype(types, i)), $i, $(quot(names[i]))), $i, $(quot(names[i])))
                end)
            end
            return block
        end
        rle = runlength(types)
        if length(rle) < 100
            block = Expr(:block, Expr(:meta, :inline))
            i = 1
            for (T, len) in rle
                push!(block.args, quote
                    for j = 0:$(len-1)
                        @inbounds f(getcolumn(row, $T, $i + j, names[$i + j]), $i + j, names[$i + j])
                    end
                end)
                i += len
            end
            b = block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getcolumn(row, fieldtype(types, i), i, nm), i, nm)
                end
                return
            end
        end
        # println(b)
        return b
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, fieldtype(types, i), i, nm), i, nm)
        end
        return
    end
end

@inline function eachcolumn(f::Base.Callable, sch::Schema{names, nothing}, row) where {names}
    if @generated
        if length(names) < 100
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getcolumn(row, $(quot(names[i]))), $i, $(quot(names[i])))
                end)
            end
            return block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getcolumn(row, nm), i, nm)
                end
                return
            end
            return b
        end
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, nm), i, nm)
        end
        return
    end
end

# these are specialized `eachcolumn`s where we also want
# the indexing of `columns` to be constant propagated, so it needs to be returned from the generated function
@inline function eachcolumns(f::Base.Callable, sch::Schema{names, types}, row, columns, args...) where {names, types}
    if @generated
        if length(names) < 101
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getcolumn(row, $(fieldtype(types, i)), $i, $(quot(names[i]))), $i, $(quot(names[i])), columns[$i], args...)
                end)
            end
            return block
        end
        rle = runlength(types)
        if length(rle) < 100
            block = Expr(:block, Expr(:meta, :inline))
            i = 1
            for (T, len) in rle
                push!(block.args, quote
                    for j = 0:$(len-1)
                        @inbounds f(getcolumn(row, $T, $i + j, names[$i + j]), $i + j, names[$i + j], columns[$i + j], args...)
                    end
                end)
                i += len
            end
            b = block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getcolumn(row, fieldtype(types, i), i, nm), i, nm, columns[i], args...)
                end
                return
            end
        end
        # println(b)
        return b
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, fieldtype(types, i), i, nm), i, nm, columns[i], args...)
        end
        return
    end
end

@inline function eachcolumns(f::Base.Callable, sch::Schema{names, nothing}, row, columns, args...) where {names}
    if @generated
        if length(names) < 100
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getcolumn(row, $(quot(names[i]))), $i, $(quot(names[i])), columns[$i], args...)
                end)
            end
            return block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getcolumn(row, nm), i, nm, columns[i], args...)
                end
                return
            end
            return b
        end
    else
        for (i, nm) in enumerate(names)
            f(getcolumn(row, nm), i, nm, columns[i], args...)
        end
        return
    end
end
