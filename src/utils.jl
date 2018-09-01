"get the names of a NamedTuple type"
names(::Type{NamedTuple{nms, typs}}) where {nms, typs} = nms
"get the types of a NamedTuple type, returned as a Tuple"
Base.@pure types(::Type{NT}) where {NT <: NamedTuple} = Tuple{Any[fieldtype(NT, i) for i = 1:fieldcount(NT)]...}

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
Base.getproperty(x, ::Type{T}, i::Int, nm::Symbol) where {T} = getproperty(x, nm)

"""
    Tables.eachcolumn(f, names, types, row, args...)

    Takes a function `f`, column names `names`, column types `types`, a `row` type (that satisfies the Row interface), and any other `args...`;
    it generates calls to get the value for each column in the row (`getproperty(row, nm)`) and then calls `f(val, col, name)`, where `f` is the
    user-provided function, `val` is a row's column value, `col` is the column index as an `Int`, and `name` is the row's column name.
"""
@inline function eachcolumn(f::Base.Callable, sch::Schema{names, types}, row, args...) where {names, types}
    if @generated
        if length(names) < 100
            if types === nothing
                b = Expr(:block, Any[:(f(getproperty(row, $(Meta.QuoteNode(names[i]))), $i, $(Meta.QuoteNode(names[i])), args...)) for i = 1:length(names)]...)
            else
                b = Expr(:block, Any[:(f(getproperty(row, $(fieldtype(types, i)), $i, $(Meta.QuoteNode(names[i]))), $i, $(Meta.QuoteNode(names[i])), args...)) for i = 1:fieldcount(types)]...)
            end
        elseif types !== nothing
            rle = runlength(types)
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
                b = block
            else
                b = quote
                    for (i, nm) in enumerate(names)
                        f(getproperty(row, fieldtype(types, i), i, nm), i, nm, args...)
                    end
                    return
                end
            end
        else
            b = quote
                for (i, nm) in enumerate(names)
                    f(getproperty(row, nm), i, nm, args...)
                end
                return
            end
        end
        # println(b)
        return b
    else
        if types === nothing
            for (i, nm) in enumerate(names)
                f(getproperty(row, nm), i, nm, args...)
            end
        else
            for (i, nm) in enumerate(names)
                f(getproperty(row, fieldtype(types, i), i, nm), i, nm, args...)
            end
        end
        return
    end
end

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

"given tuple type and a Symbol `name`, compute the type of the name in the tuples types"
Base.@pure function columntype(names, ::Type{T}, name::Symbol) where {T <: Tuple}
    i = 1
    for nm in names
        nm === name && return fieldtype(T, i)
        i += 1
    end
    return Union{}
end
