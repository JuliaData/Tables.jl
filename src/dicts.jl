# Dict of Vectors as table
struct DictColumnTable <: AbstractColumns
    schema::Schema
    values::OrderedDict{Symbol, AbstractVector}
end

"""
    Tables.dictcolumntable(x) => Tables.DictColumnTable

Take any Tables.jl-compatible source `x` and return a `DictColumnTable`, which
can be thought of as a `OrderedDict` mapping column names as `Symbol`s to `AbstractVector`s.
The order of the input table columns is preserved via the `Tables.schema(::DictColumnTable)`.

For "schema-less" input tables, `dictcolumntable` employs a "column unioning" behavior,
as opposed to taking the column names from the first row like `Tables.columns`. This
means that as rows are iterated, each value from the row is joined into an aggregate
final set of columns. This is especially useful when input table rows may not include
columns if the value is missing, instead of including an actual value `missing`, which
is common in json, for example. This results in a performance cost tracking all seen
values and inferring the final unioned schemas, so it's recommended to use only when
needed.

For unknown schemas, the source is read once and the original cell values are buffered
in `Any` vectors while column types are inferred with `promote_type`. Values are then
converted directly to their final column types. Absent cells become `missing`. Peak
memory can include both the buffered values and output columns, plus buffering overhead.
Mutable cell contents are not copied. Final numeric promotion can still round values.
Rows with a known schema do not need this intermediate buffer.
"""
function dictcolumntable(x)
    if columnaccess(x)
        cols = columns(x)
        names = columnnames(cols)
        sch = schema(cols)
        out = OrderedDict(nm => getcolumn(cols, nm) for nm in names)
    else
        r = rows(x)
        L = Base.IteratorSize(typeof(r))
        len = Base.haslength(r) ? length(r) : 0
        sch = schema(r)
        if sch !== nothing
            names, types = sch.names, sch.types
            out = OrderedDict{Int, AbstractVector}(i => allocatecolumn(types[i], len) for i = 1:length(types))
            for (i, row) in enumerate(r)
                eachcolumns(add!, sch, row, out, L, i)
            end
            out = OrderedDict(names[k] => v for (k, v) in out)
        else
            names, cols = buffercolumns(r, len)
            out = OrderedDict{Symbol, AbstractVector}(zip(names, cols))
            sch = Schema(collect(keys(out)), eltype.(values(out)))
        end
    end
    return DictColumnTable(sch, out)
end

# Column names and types are only known once every row is seen, so buffer the original
# values and convert each one once into its final column type. Kept in its own function
# so the hot loop's locals stay type-stable.
function buffercolumns(r, len)
    names = Symbol[]
    index = Dict{Symbol, Int}()
    types = Type[]
    buffers = Vector{Any}[]
    n = 0
    for row in r
        n += 1
        for nm in columnnames(row)
            i = get(index, nm, 0)
            if i == 0
                push!(names, nm)
                push!(types, Union{})
                push!(buffers, sizehint!(Any[], len))
                i = index[nm] = length(names)
            end
            vals = buffers[i]
            T = types[i]
            if length(vals) < n - 1 # absent from earlier rows
                append!(vals, Iterators.repeated(missing, n - 1 - length(vals)))
                T = Union{Missing, T}
            end
            val = getcolumn(row, nm)
            push!(vals, val)
            types[i] = val isa T ? T : promote_type(T, typeof(val))
        end
    end
    cols = AbstractVector[]
    for (vals, T) in zip(buffers, types)
        if length(vals) < n # absent from the last rows
            append!(vals, Iterators.repeated(missing, n - length(vals)))
            T = Union{Missing, T}
        end
        push!(cols, finishcolumn(vals, T))
    end
    return names, cols
end

istable(::Type{DictColumnTable}) = true
columnaccess(::Type{DictColumnTable}) = true
columns(x::DictColumnTable) = x
schema(x::DictColumnTable) = getfield(x, :schema)
columnnames(x::DictColumnTable) = getfield(x, :schema).names
getcolumn(x::DictColumnTable, i::Int) = getfield(x, :values)[columnnames(x)[i]]
getcolumn(x::DictColumnTable, nm::Symbol) = getfield(x, :values)[nm]

# Vector of Dicts as table
struct DictRowTable
    names::Vector{Symbol}
    types::Dict{Symbol, Type}
    values::Vector{Dict{Symbol, Any}}
end

isrowtable(::Type{DictRowTable}) = true
schema(x::DictRowTable) = Schema(getfield(x, :names), [getfield(x, :types)[nm] for nm in getfield(x, :names)])

struct DictRow <: AbstractRow
    names::Vector{Symbol}
    row::Dict{Symbol, Any}
end

columnnames(x::DictRow) = getfield(x, :names)
getcolumn(x::DictRow, i::Int) = get(getfield(x, :row), columnnames(x)[i], missing)
getcolumn(x::DictRow, nm::Symbol) = get(getfield(x, :row), nm, missing)

Base.IteratorSize(::Type{DictRowTable}) = Base.HasLength()
Base.length(x::DictRowTable) = length(getfield(x, :values))
Base.IteratorEltype(::Type{DictRowTable}) = Base.HasEltype()
Base.eltype(::Type{DictRowTable}) = DictRow

function Base.iterate(x::DictRowTable, st=1)
    st > length(x) && return nothing
    return DictRow(x.names, x.values[st]), st + 1
end

function subset(x::DictRowTable, inds; viewhint::Union{Bool,Nothing}=nothing, view::Union{Bool,Nothing}=nothing)
    if view !== nothing
        @warn "`view` keyword argument is deprecated for `Tables.subset`, use `viewhint` instead" maxlog=10
        viewhint = view
    end
    values = viewhint === true ? Base.view(getfield(x, :values), inds) : getfield(x, :values)[inds]
    if inds isa Integer
        return DictRow(getfield(x, :names), values)
    else
        values isa AbstractVector || throw(ArgumentError("`Tables.subset`: invalid `inds` argument, expected `RowTable` output, got $(typeof(values))"))
        return DictRowTable(getfield(x, :names), getfield(x, :types), values)
    end
end

"""
    Tables.dictrowtable(x) => Tables.DictRowTable

Take any Tables.jl-compatible source `x` and return a `DictRowTable`, which
can be thought of as a `Vector` of `OrderedDict` rows mapping column names as `Symbol`s to values.
The order of the input table columns is preserved via the `Tables.schema(::DictRowTable)`.

For "schema-less" input tables, `dictrowtable` employs a "column unioning" behavior,
as opposed to inferring the schema from the first row like `Tables.columns`. This
means that as rows are iterated, each value from the row is joined into an aggregate
final set of columns. This is especially useful when input table rows may not include
columns if the value is missing, instead of including an actual value `missing`, which
is common in json, for example. This results in a performance cost tracking all seen
values and inferring the final unioned schemas, so it's recommended to use only when
the union behavior is needed.
"""
function dictrowtable(x)
    names = Symbol[]
    seen = Set{Symbol}()
    types = OrderedDict{Symbol, Type}()
    r = rows(x)
    L = Base.IteratorSize(typeof(r))
    out = Vector{OrderedDict{Symbol, Any}}(undef, Base.haslength(r) ? length(r) : 0)
    for (i, drow) in enumerate(r)
        row = OrderedDict{Symbol, Any}(nm => getcolumn(drow, nm) for nm in columnnames(drow))
        add!(row, 0, :_, out, L, i)
        if isempty(names)
            for (k, v) in row
                push!(names, k)
                types[k] = typeof(v)
            end
            seen = Set(names)
        else
            for nm in names
                if haskey(row, nm)
                    T = types[nm]
                    v = row[nm]
                    if !(typeof(v) <: T)
                        types[nm] = Union{T, typeof(v)}
                    end
                else
                    types[nm] = Union{Missing, types[nm]}
                end
            end
            for (k, v) in row
                if !(k in seen)
                    push!(seen, k)
                    push!(names, k)
                    # we mark the type as Union{T, Missing} here because
                    # we're at least on the 2nd row, and we didn't see
                    # this column in the 1st row, so its value will be
                    # `missing` for that row
                    types[k] = Union{typeof(v), Missing}
                end
            end
        end
    end
    return DictRowTable(names, types, out)
end

# implement default nrow and ncol methods for DataAPI.jl

DataAPI.nrow(table::DictRowTable) = length(table)
DataAPI.ncol(table::DictRowTable) = length(getfield(table, :names))
