module Tables

"""
    Tables.rows(x) => NamedTuple-iterator

    most basic function to implement to satisfy the `Table` interface.
    Types should override this function to provide an iterator distince from the original object `x`.
"""
function rows end

# by default, assume `x` iterates namedtuples
rows(x) = x

"Tables.schema(s) => Tables.Schema"
function schema end

"`Tables.producescells(::Type{T}) where {T] = true` to signal your table type can produce individual cells"
function producescells end

"`Tables.getcell(source, ::Type{T}, row, col)::T` gets an individual cell value from source"
function getcell end

"`Tables.skipcell!(source, ::Type{T}, row, col)` skips a single cell"
function skipcell! end

"`Tables.producescolumns(::Type{T}) where {T] = true` to signal your table type can produce individual columns"
function producescolumns end

"`Tables.getcolumn(source, ::Type{T}, col)` gets an individual column from source"
function getcolumn end

"`Tables.acceptscells(::Type{T}) where {T} = true` to signal your table type can accept individual cell values"
function acceptscells end
acceptscells(x) = false

"`Tables.setcell!(sink, val::T, row, col` used to set the value of an individual cell in a table type"
function setcell! end

"`Tables.acceptscolumns(::Type{T}) where {T} = true` to signal your table type can accept column values"
function accceptscolumns end
accceptscolumns(x) = false

"`Tables.setcolumn!(sink, column, col)` used to set a column from a source to the sink"
function setcolumn! end

function collect(::Type{T}, itr::S) where {T, S}
    if accceptscolumns(T) && producescolumns(S)
        #TODO: @generated method
        sch = schema(itr)
        sink = T(sch)
        typs = types(sch)
        for (col, T) in enumerate(typs)
            setcolumn!(sink, T, getcolumn(itr, T, col))
        end
        return sink
    elseif acceptscells(T) && producescells(S)
        sch = schema(itr)
        sink = T(sch)
        typs = types(sch)
        # account for rows
        for (col, T) in enumerate(typs)
            setcell!(sink, T, getcell(itr, T, col))
        end
        return sink
    else
        # default assumes `x` can take care of a NamedTuple-iterator by itself
        return x(itr)
    end
end

end # module
