module Tables

include("rle.jl")

"""
    Tables.rows(x) => NamedTuple-iterator

    most basic function to implement to satisfy the `Table` interface.
    Types should override this function to provide an iterator distince from the original object `x`.
"""
function rows end

# by default, assume `x` iterates namedtuples
rows(x) = x

struct Schema{T}
    header::Vector{String}
end

function Schema(types, header::Vector{String})
    return Schema{RLE(types)}(header)
end

function Schema(::Type{T}) where {T <: NamedTuple}
    return Schema(T.parameters[2].parameters, Base.collect(map(string, T.parameters[1])))
end

types(sch::Schema{T}) where {T} = types(T)

"Tables.schema(s) => Tables.Schema"
function schema end

"`Tables.producescells(::Type{T}) where {T] = true` to signal your table type can produce individual cells"
function producescells end

"`Tables.getcell(source, ::Type{T}, row, col)::T` gets an individual cell value from source"
function getcell end

"`Tables.skipcell!(source, ::Type{T}, row, col)` skips a single cell"
function skipcell! end
skipcell!(x, T, row, col) = nothing

"`Tables.producescolumns(::Type{T}) where {T] = true` to signal your table type can produce individual columns"
function producescolumns end

"`Tables.getcolumn(source, ::Type{T}, col)` gets an individual column from source"
function getcolumn end

"`Tables.acceptscolumns(::Type{T}) where {T} = true` to signal your table type can accept column values"
function accceptscolumns end
accceptscolumns(x) = false

"`Tables.setcolumn!(sink, column, col)` used to set a column from a source to the sink"
function setcolumn! end

include("collects.jl")

function collect(::Type{T}, itr::S) where {T, S}
    if accceptscolumns(T) && producescolumns(S)
        sch = schema(itr)
        sink = T(sch)
        return collectcolumns(sink, sch, itr)
    end
    # default assumes `x` can take care of a NamedTuple-iterator by itself
    return T(itr)
end

include("namedtuples.jl")

end # module
