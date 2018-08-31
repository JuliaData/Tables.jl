"get the names of a NamedTuple type"
names(::Type{NamedTuple{nms, typs}}) where {nms, typs} = nms
"get the types of a NamedTuple type, returned as a Tuple"
Base.@pure types(::Type{NT}) where {NT <: NamedTuple} = Tuple(fieldtype(NT, i) for i = 1:fieldcount(NT))

"helper function to calculate a run-length encoding of the types of a NamedTuple type"
Base.@pure function runlength(::Type{NT}) where {NT <: NamedTuple}
    rle = Tuple{Type, Int}[]
    fieldcount(NT) == 0 && return rle
    T = fieldtype(NT, 1)
    prevT = T
    len = 1
    for i = 2:fieldcount(NT)
        @inbounds T = fieldtype(NT, i)
        if T === prevT
            len += 1
        else
            push!(rle, (prevT, len))
            prevT = T
            len = 1
        end
    end
    push!(rle, (T, len))
    return rle
end

# generic fallback from getproperty w/ type information to basic symbol lookup
Base.getproperty(x, ::Type{T}, i::Int, nm::Symbol) where {T} = getproperty(x, nm)

"""
    Tables.unroll(f, schema, row, args...)

    Takes a function `f`, a NamedTuple type `schema`, a `row` type (that satisfies the Row interface), and any other `args...`;
    it generates calls to get the value for each column in the row and then calls `f(val, T, col, name)`, where `f` is the
    user-provided function, `val` is a row's column value, `T` is the column's element type, `col` is the column index as
    an `Int`, and `name` is the row's column name.

    This is useful for sinks iterating rows who wish to provide a type-stable mechanism for their "inner loops". Typically,
    such inner loops suffer from dynamic dispath due to the varying types of columns.
"""
@inline function unroll(f::Base.Callable, ::Type{NT}, row, args...) where {NT <: NamedTuple{names}} where {names}
    if @generated
        if fieldcount(NT) < 100
            return Expr(:block, Any[:(f(getproperty(row, $(fieldtype(NT, i)), $i, $(Meta.QuoteNode(names[i]))), $i, $(Meta.QuoteNode(names[i])), args...)) for i = 1:fieldcount(NT)]...)
        else
            rle = runlength(NT)
            if length(rle) < 100
                block = Expr(:block)
                i = 1
                for (T, len) in rle
                    push!(block.args, quote
                        for j = 0:$(len-1)
                            f(getproperty(row, $T, $i + j, names[$i + j]), $i + j, names[$i + j], args...)
                        end
                    end)
                    i += len
                end
                # @show block
                return block
            else
                return quote
                    for (i, nm) in enumerate(names)
                        f(getproperty(row, fieldtype(NT, i), i, nm), i, nm, args...)
                    end
                    return
                end
            end
        end
    else
        for (i, nm) in enumerate(names)
            f(getproperty(row, fieldtype(NT, i), i, nm), i, nm, args...)
        end
        return
    end
end

@inline function unroll(f::Base.Callable, names::Tuple{Vararg{Symbol}}, row, args...)
    if @generated
        if fieldcount(names) < 100
            b = Expr(:block, Any[:(@inbounds f(getproperty(row, names[$i]), $i, names[$i], args...)) for i = 1:fieldcount(names)]...)
            @show b
            return b
        else
            return quote
                for (i, nm) in enumerate(names)
                    f(getproperty(row, nm), i, nm, args...)
                end
                return
            end
        end
    else
        for (i, nm) in enumerate(names)
            f(getproperty(row, nm), i, nm, args...)
        end
        return
    end
end


"given a NamedTuple type and a Symbol `name`, compute the index (1-based) of the name in the NamedTuple's names"
Base.@pure function columnindex(::Type{NT}, name::Symbol) where {NT <: NamedTuple{names}} where {names}
    i = 1
    for nm in names
        nm === name && return i
        i += 1
    end
    return 0
end

"given a NamedTuple type and a Symbol `name`, compute the type of the name in the NamedTuple's types"
Base.@pure function columntype(::Type{NT}, name::Symbol) where {NT <: NamedTuple{names}} where {names}
    i = 1
    for nm in names
        nm === name && return fieldtype(NT, i)
        i += 1
    end
    return Union{}
end
