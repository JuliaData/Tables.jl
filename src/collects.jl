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

function collectcells(sink, sch::Schema{T}, itr) where {T}
    if @generated
        innerloops = Expr(:block)
        col = 0
        for (i, t) in enumerate(T)
            push!(loops.args, quote
                for i = 1:$(t[2])
                    setcell!(sink, getcell(itr, $(t[1]), $col + i), $col + i)
                end
            end)
            col += t[2]
        end
        q = quote
            for row = 1:size(itr)[1]
                $innerloops
            end
            return sink
        end
        return q
    else
        for row = 1:size(itr)[1]
            col = 0
            for t in T
                setcell(sink, itr, t, row, col)
                col += t[2]
            end
        end
        return sink
    end
end

@noinline function setcell(sink, itr, tup, row, coloffset)
    for i = 1:tup[2]
        setcell!(sink, getcell(itr, tup[1], row, coloffset + i), row, coloffset + i)
    end
end