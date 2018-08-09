module Tables

using IteratorInterfaceExtensions

export rowtable, columntable

"Abstract row type with a simple required interface: row values are accessible via `getproperty(row, field)`"
abstract type Row end

"Abstract table type that table types may subtype to hook into useful default interface definitions"
abstract type Table end

"""
    Tables.rows(x) => Row-iterator

    most basic function to implement to satisfy the `Table` interface.
    Types should override this function, if needed, to provide an iterator distinct from the original object `x`.
"""
function rows end

IteratorInterfaceExtensions.getiterator(x::Table) = rows(x)

# by default, assume `x` iterates `Row`s
rows(x) = x

struct Schema{T}
    header::Vector{String}
end

Schema(types, header::Vector{String}) = Schema{Tuple{types...}}(header)
Schema(::Type{NamedTuple{names, T}}) where {names, T} = Schema{T}(Base.collect(map(string, names)))

types(sch::Schema{T}) where {T} = Tuple(T.parameters)

"Tables.schema(s) => Tables.Schema"
function schema end

"`Tables.producescells(::Type{<:MyTable}) = true` to signal your table type can produce individual cells"
function producescells end

"`Tables.getcell(source, ::Type{T}, row, col)::T` gets an individual cell value from source"
function getcell end

"`Tables.skipcell!(source, ::Type{T}, row, col)` skips a single cell"
function skipcell! end
skipcell!(x, T, row, col) = nothing

"`Tables.producescolumns(::Type{<:MyTable}) = true` to signal your table type can produce individual columns"
function producescolumns end

"`Tables.getcolumn(source, ::Type{T}, col::Int)` gets an individual column from source"
function getcolumn end

"`Tables.acceptscolumns(::Type{<:MyTable}) = true` to signal your table type can accept column values"
function accceptscolumns end
accceptscolumns(x) = false

"`Tables.setcolumn!(sink, column, col::Int)` used to set a column from a source to the sink"
function setcolumn! end

include("rle.jl")
include("collects.jl")

function collect(f::Base.Callable, itr::S) where {S}
    if acceptscolumns(f) && producescolumns(S)
        sch = schema(itr)
        sink = f(sch)
        return collectcolumns(sink, sch, itr)
    end
    # default assumes `x` can take care of a Row-iterator by itself
    return f(itr)
end

include("namedtuples.jl")

end # module
