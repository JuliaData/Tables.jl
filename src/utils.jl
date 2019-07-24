names(::Type{NamedTuple{nms, T}}) where {nms, T} = nms
types(::Type{NamedTuple{nms, T}}) where {nms, T} = T

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

# generic fallback from getproperty w/ type information to basic symbol lookup
Base.getproperty(x, ::Type{T}, i::Int, nm) where {T} = getproperty(x, nm)
Base.getproperty(x::NamedTuple{names, types}, ::Type{T}, i::Int, nm::Symbol) where {names, types, T} = Core.getfield(x, i)

"""
    Tables.eachcolumn(f, sch, row, args...)
    Tables.eachcolumn(Tables.columns(x))

    The first definition takes a function `f`, table schema `sch`, a `row` type (that satisfies the `Tables.Row` interface), and any other `args...`;
    it generates calls to get the value for each column in the row (`getproperty(row, nm)`) and then calls `f(val, col, name, args...)`, where `f` is the
    user-provided function, `val` is a row's column value, `col` is the column index as an `Int`, and `name` is the row's column name as a `Symbol`.

    While the first definition applies to a `Row` object, the last definition simply returns a property-iterator over a `Columns` object.
    For example, one could "collect" every column of a `Columns` object by doing:
    ```julia
    vectors = [collect(col) for col in Tables.eachcolumn(Tables.columns(x))]
    ```
"""
function eachcolumn end

quot(s::Symbol) = Meta.QuoteNode(s)
quot(x::Int) = x

@inline function eachcolumn(f::Base.Callable, sch::Schema{names, types}, row, args...) where {names, types}
    if @generated
        if length(names) < 101
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getproperty(row, $(fieldtype(types, i)), $i, $(quot(names[i]))), $i, $(quot(names[i])), args...)
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
                        @inbounds f(getproperty(row, $T, $i + j, names[$i + j]), $i + j, names[$i + j], args...)
                    end
                end)
                i += len
            end
            b = block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getproperty(row, fieldtype(types, i), i, nm), i, nm, args...)
                end
                return
            end
        end
        # println(b)
        return b
    else
        for (i, nm) in enumerate(names)
            f(getproperty(row, fieldtype(types, i), i, nm), i, nm, args...)
        end
        return
    end
end

@inline function eachcolumn(f::Base.Callable, sch::Schema{names, nothing}, row, args...) where {names}
    if @generated
        if length(names) < 100
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getproperty(row, $(quot(names[i]))), $i, $(quot(names[i])), args...)
                end)
            end
            return block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getproperty(row, nm), i, nm, args...)
                end
                return
            end
            return b
        end
    else
        for (i, nm) in enumerate(names)
            f(getproperty(row, nm), i, nm, args...)
        end
        return
    end
end

# this are specialized `eachcolumn`s where we also want
# the indexing of `columns` to be constant propagated, so it needs to be returned from the generated function
@inline function eachcolumns(f::Base.Callable, sch::Schema{names, types}, row, columns, args...) where {names, types}
    if @generated
        if length(names) < 101
            block = Expr(:block, Expr(:meta, :inline))
            for i = 1:length(names)
                push!(block.args, quote
                    f(getproperty(row, $(fieldtype(types, i)), $i, $(quot(names[i]))), $i, $(quot(names[i])), columns[$i], args...)
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
                        @inbounds f(getproperty(row, $T, $i + j, names[$i + j]), $i + j, names[$i + j], columns[$i + j], args...)
                    end
                end)
                i += len
            end
            b = block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getproperty(row, fieldtype(types, i), i, nm), i, nm, columns[i], args...)
                end
                return
            end
        end
        # println(b)
        return b
    else
        for (i, nm) in enumerate(names)
            f(getproperty(row, fieldtype(types, i), i, nm), i, nm, columns[i], args...)
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
                    f(getproperty(row, $(quot(names[i]))), $i, $(quot(names[i])), columns[$i], args...)
                end)
            end
            return block
        else
            b = quote
                $(Expr(:meta, :inline))
                for (i, nm) in enumerate(names)
                    f(getproperty(row, nm), i, nm, columns[i], args...)
                end
                return
            end
            return b
        end
    else
        for (i, nm) in enumerate(names)
            f(getproperty(row, nm), i, nm, columns[i], args...)
        end
        return
    end
end

# iterator over a `Columns`' properties
struct EachColumn{T}
    source::T
end

Base.length(e::EachColumn) = length(propertynames(e.source))
Base.IteratorEltype(::Type{<:EachColumn}) = Base.EltypeUnknown()

function Base.iterate(e::EachColumn, (idx, props)=(1, propertynames(e.source)))
    idx > length(props) && return nothing
    return getproperty(e.source, props[idx]), (idx + 1, props)
end

eachcolumn(c) = EachColumn(c)

"given names and a Symbol `name`, compute the index (1-based) of the name in names"
Base.@pure function columnindex(names::Tuple{Vararg{Symbol}}, name::Symbol)
    i = 1
    for nm in names
        nm === name && return i
        i += 1
    end
    return 0
end

Base.@pure columntype(::Schema{names, types}, name::Symbol) where {names, types} = columntype(names, types, name)

"given tuple type and a Symbol `name`, compute the type of the name in the tuples types"
Base.@pure function columntype(names::Tuple{Vararg{Symbol}}, ::Type{T}, name::Symbol) where {T <: Tuple}
    i = 1
    for nm in names
        nm === name && return fieldtype(T, i)
        i += 1
    end
    return Union{}
end
