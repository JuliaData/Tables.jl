struct TransformsRow{T, F}
    row::T
    funcs::F
end

Base.getproperty(row::TransformsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = (getfunc(row, getfield(row, 2), col, nm))(getproperty(getfield(row, 1), T, col, nm))
Base.getproperty(row::TransformsRow, nm::Symbol) = (getfunc(row, getfield(row, 2), nm))(getproperty(getfield(row, 1), nm))
Base.propertynames(row::TransformsRow) = propertynames(getfield(row, 1))

struct Transforms{T, F, C}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end
Base.propertynames(t::Transforms) = propertynames(getfield(t, 1))
Base.getproperty(t::Transforms, nm::Symbol) = map(getfunc(t, getfield(t, 2), nm), getproperty(getfield(t, 1), nm))

transform(funcs) = x->transform(x, funcs)
transform(; kw...) = transform(kw.data)
function transform(src::T, funcs) where {T}
    cols = false
    if columnaccess(T)
        x = columns(src)
        cols = true
    else
        x = rows(src)
    end
    return Transforms{typeof(x), typeof(funcs), cols}(x, funcs)
end

getfunc(row, nt::NamedTuple, i, nm) = get(nt, i, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, i, nm) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, i, nm) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, i, nm) = get(d, i, identity)

getfunc(row, nt::NamedTuple, nm) = get(nt, nm, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, nm) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, nm) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, nm) = get(d, findfirst(isequal(nm), propertynames(row)), identity)

istable(::Type{<:Transforms}) = true
rowaccess(::Type{<:Transforms}) = true
rows(t::Transforms{T, F, false}) where {T, F} = t
columnaccess(::Type{Transforms{T, F, C}}) where {T, F, C} = C
columns(t::Transforms{T, F, true}) where {T, F} = t
# avoid relying on inference here and just let sinks figure things out
schema(t::Transforms) = nothing

Base.IteratorSize(::Type{<:Transforms{T}}) where {T} = Base.IteratorSize(T)
Base.length(t::Transforms) = length(getfield(t, 1))
Base.eltype(t::Transforms{T, F}) where {T, F} = TransformsRow{eltype(getfield(t, 1)), F}

@inline function Base.iterate(t::Transforms, st=())
    state = iterate(getfield(t, 1), st...)
    state === nothing && return nothing
    return TransformsRow(state[1], getfield(t, 2)), (state[2],)
end

# select
struct Select{T, columnaccess, names}
    source::T
end

select(names::Symbol...) = x->select(x, names...)
select(names::String...) = x->select(x, map(Symbol, names)...)
function select(x::T, names::Symbol...) where {T}
    colaccess = columnaccess(T)
    r = colaccess ? columns(x) : rows(x)
    return Select{typeof(r), colaccess, names}(r)
end

istable(::Type{<:Select}) = true

Base.@pure function typesubset(::Schema{names, types}, nms) where {names, types}
    return Tuple{Any[columntype(names, types, nm) for nm in nms]...}
end

function schema(s::Select{T, columnaccess, names}) where {T, columnaccess, names}
    sch = schema(getfield(s, 1))
    sch === nothing && return nothing
    return Schema(names, typesubset(sch, names))
end

# columns: make Select property-accessible
Base.getproperty(s::Select, nm::Symbol) = getproperty(getfield(s, 1), nm)
Base.propertynames(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = names
columnaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = columnaccess
columns(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = columnaccess ? s :
    buildcolumns(schema(s), s)

# rows: implement Iterator interface
Base.IteratorSize(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = Base.IteratorSize(T)
Base.length(s::Select) = length(getfield(s, 1))
Base.IteratorEltype(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = Base.IteratorEltype(T)
Base.eltype(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = SelectRow{eltype(getfield(s, 1)), names}
rowaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = !columnaccess
rows(s::Select{T, columnaccess, names}) where {T, columnaccess, names} = columnaccess ? RowIterator(s, rowcount(getfield(s, 1))) : s

# we need to iterate a "row view" in case the underlying source has unknown schema
# to ensure each iterated row only has `names` propertynames
struct SelectRow{T, names}
    row::T
end

Base.getproperty(row::SelectRow, ::Type{T}, col::Int, nm::Symbol) where {T} = getproperty(getfield(row, 1), T, col, nm)
Base.getproperty(row::SelectRow, nm::Symbol) = getproperty(getfield(row, 1), nm)
Base.propertynames(row::SelectRow{T, names}) where {T, names} = names

function Base.iterate(s::Select{T, names}, st=()) where {T, names}
    state = iterate(getfield(s, 1), st...)
    state === nothing && return nothing
    row, st = state
    return SelectRow{typeof(row), names}(row), (st,)
end
