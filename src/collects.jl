function collectcolumns(sink, sch::Schema{T}, itr) where {T}
    if @generated
        loops = Expr(:block)
        col = 0
        for (i, t) in enumerate(T)
            push!(loops.args, quote
                for i = 1:$(t[2])
                    setcolumn!(sink, getcolumn(itr, $(t[1]), $col + i), $col + i)
                end
            end)
            col += t[2]
        end
        q = quote
            $loops
            return sink
        end
        return q
    else
        col = 0
        for t in T
            setcolumn(sink, itr, t, col)
            col += t[2]
        end
        return sink
    end
end

@noinline function setcolumn(sink, itr, tup, coloffset)
    for i = 1:tup[2]
        setcolumn!(sink, getcolumn(itr, tup[1], coloffset + i), coloffset + i)
    end
end
