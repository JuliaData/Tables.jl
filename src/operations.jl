struct TransformsRow{T, F} <: AbstractRow
    row::T
    funcs::F
end

getrow(r::TransformsRow) = getfield(r, :row)
getfuncs(r::TransformsRow) = getfield(r, :funcs)

getcolumn(row::TransformsRow, nm::Symbol) = (getfunc(row, getfuncs(row), nm))(getcolumn(getrow(row), nm))
getcolumn(row::TransformsRow, i::Int) = (getfunc(row, getfuncs(row), i))(getcolumn(getrow(row), i))
columnnames(row::TransformsRow) = columnnames(getrow(row))

struct Transforms{C, T, F}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end

columnnames(t::Transforms{true}) = columnnames(getfield(t, 1))
getcolumn(t::Transforms{true}, nm::Symbol) = Base.map(getfunc(t, getfield(t, 2), nm), getcolumn(getfield(t, 1), nm))
getcolumn(t::Transforms{true}, i::Int) = Base.map(getfunc(t, getfield(t, 2), i), getcolumn(getfield(t, 1), i))
# for backwards compat
Base.propertynames(t::Transforms{true}) = columnnames(t)
Base.getproperty(t::Transforms{true}, nm::Symbol) = getcolumn(t, nm)

"""
    Tables.transform(source, funcs) => Tables.Transforms
    source |> Tables.transform(funcs) => Tables.Transform

***EXPERIMENTAL - May be moved or removed in a future release***
Given any Tables.jl-compatible source, apply a series of transformation functions, for the columns specified in `funcs`.
The tranform functions can be a NamedTuple or Dict mapping column name (`String` or `Symbol` or `Integer` index) to Function.
"""
function transform end

transform(funcs) = x->transform(x, funcs)
transform(; kw...) = transform(kw.data)
function transform(src::T, funcs::F) where {T, F}
    C = columnaccess(T)
    x = C ? columns(src) : rows(src)
    return Transforms{C, typeof(x), F}(x, funcs)
end

getfunc(row, nt::NamedTuple, nm::Symbol) = get(nt, nm, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, nm::Symbol) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, nm::Symbol) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, nm::Symbol) = get(d, findfirst(isequal(nm), columnnames(row)), identity)

getfunc(row, nt::NamedTuple, i::Int) = get(nt, columnnames(row)[i], identity)
getfunc(row, d::Dict{String, <:Base.Callable}, i::Int) = get(d, String(columnnames(row)[i]), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, i::Int) = get(d, columnnames(row)[i], identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, i::Int) = get(d, i, identity)

istable(::Type{<:Transforms}) = true
rowaccess(::Type{Transforms{C, T, F}}) where {C, T, F} = !C
rows(t::Transforms{false}) = t
columnaccess(::Type{Transforms{C, T, F}}) where {C, T, F} = C
columns(t::Transforms{true}) = t
# avoid relying on inference here and just let sinks figure things out
schema(t::Transforms) = nothing
IteratorInterfaceExtensions.isiterable(x::Transforms) = true
IteratorInterfaceExtensions.getiterator(x::Transforms) = datavaluerows(columntable(x))

Base.IteratorSize(::Type{Transforms{false, T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(t::Transforms{false}) = length(getfield(t, 1))
Base.eltype(t::Transforms{false, T, F}) where {T, F} = TransformsRow{eltype(getfield(t, 1)), F}

@inline function Base.iterate(t::Transforms{false}, st=())
    state = iterate(getfield(t, 1), st...)
    state === nothing && return nothing
    return TransformsRow(state[1], getfield(t, 2)), (state[2],)
end

# select
struct Select{T, columnaccess, names}
    source::T
end

"""
    Tables.select(source, columns...) => Tables.Select
    source |> Tables.select(columns...) => Tables.Select

***EXPERIMENTAL - May be moved or removed in a future release***
Create a lazy wrapper that satisfies the Tables.jl interface and keeps only the columns given by the columns arguments, which can be `String`s, `Symbol`s, or `Integer`s
"""
function select end

select(names::Symbol...) = x->select(x, names...)
select(names::String...) = x->select(x, Base.map(Symbol, names)...)
select(inds::Integer...) = x->select(x, Base.map(Int, inds)...)

function select(x::T, names...) where {T}
    colaccess = columnaccess(T)
    r = colaccess ? columns(x) : rows(x)
    return Select{typeof(r), colaccess, names}(r)
end

istable(::Type{<:Select}) = true
IteratorInterfaceExtensions.isiterable(x::Select) = true
IteratorInterfaceExtensions.getiterator(x::Select) = datavaluerows(x)

Base.@pure function typesubset(::Schema{names, types}, nms::NTuple{N, Symbol}) where {names, types, N}
    return Tuple{Any[columntype(names, types, nm) for nm in nms]...}
end

Base.@pure function typesubset(::Schema{names, types}, inds::NTuple{N, Int}) where {names, types, N}
    return Tuple{Any[fieldtype(types, i) for i in inds]...}
end

typesubset(::Schema{names, types}, ::Tuple{}) where {names, types} = Tuple{}

namesubset(::Schema{names, types}, nms::NTuple{N, Symbol}) where {names, types, N} = nms
Base.@pure namesubset(::Schema{names, T}, inds::NTuple{N, Int}) where {names, T, N} = ntuple(i -> names[inds[i]], N)
namesubset(::Schema{names, types}, ::Tuple{}) where {names, types} = ()
namesubset(names, nms::NTuple{N, Symbol}) where {N} = nms
namesubset(names, inds::NTuple{N, Int}) where {N} = ntuple(i -> names[inds[i]], N)
namesubset(names, ::Tuple{}) = ()

function schema(s::Select{T, columnaccess, names}) where {T, columnaccess, names}
    sch = schema(getfield(s, 1))
    sch === nothing && return nothing
    return Schema(namesubset(sch, names), typesubset(sch, names))
end

# columns: make Select property-accessible
getcolumn(s::Select{T, true, names}, nm::Symbol) where {T, names} = getcolumn(getfield(s, 1), nm)
getcolumn(s::Select{T, true, names}, i::Int) where {T, names} = getcolumn(getfield(s, 1), i)
columnnames(s::Select{T, true, names}) where {T, names} = namesubset(columnnames(getfield(s, 1)), names)
columnaccess(::Type{Select{T, C, names}}) where {T, C, names} = C
columns(s::Select{T, true, names}) where {T, names} = s
# for backwards compat
Base.propertynames(s::Select{T, true, names}) where {T, names} = columnnames(s)
Base.getproperty(s::Select{T, true, names}, nm::Symbol) where {T, names} = getcolumn(s, nm)

# rows: implement Iterator interface
Base.IteratorSize(::Type{Select{T, false, names}}) where {T, names} = Base.IteratorSize(T)
Base.length(s::Select{T, false, names}) where {T, names} = length(getfield(s, 1))
Base.IteratorEltype(::Type{Select{T, false, names}}) where {T, names} = Base.IteratorEltype(T)
Base.eltype(s::Select{T, false, names}) where {T, names} = SelectRow{eltype(getfield(s, 1)), names}
rowaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = !columnaccess
rows(s::Select{T, false, names}) where {T, names} = s

# we need to iterate a "row view" in case the underlying source has unknown schema
# to ensure each iterated row only has `names` columnnames
struct SelectRow{T, names} <: AbstractRow
    row::T
end

getcolumn(row::SelectRow, nm::Symbol) = getcolumn(getfield(row, 1), nm)
getcolumn(row::SelectRow, i::Int) = getcolumn(getfield(row, 1), i)
getcolumn(row::SelectRow, ::Type{T}, i::Int, nm::Symbol) where {T} = getcolumn(getfield(row, 1), T, i, nm)

getprops(row, nms::NTuple{N, Symbol}) where {N} = nms
getprops(row, inds::NTuple{N, Int}) where {N} = ntuple(i->columnnames(getfield(row, 1))[inds[i]], N)
getprops(row, ::Tuple{}) = ()

columnnames(row::SelectRow{T, names}) where {T, names} = getprops(row, names)

@inline function Base.iterate(s::Select{T, false, names}) where {T, names}
    state = iterate(getfield(s, 1))
    state === nothing && return nothing
    row, st = state
    return SelectRow{typeof(row), names}(row), st
end

@inline function Base.iterate(s::Select{T, false, names}, st) where {T, names}
    state = iterate(getfield(s, 1), st)
    state === nothing && return nothing
    row, st = state
    return SelectRow{typeof(row), names}(row), st
end

# filter
struct Filter{F, T}
    f::F
    x::T
end

function filter(f::F, x) where {F <: Base.Callable}
    r = rows(x)
    return Filter{F, typeof(r)}(f, r)
end
filter(f::Base.Callable) = x->filter(f, x)

istable(::Type{<:Filter}) = true
rowaccess(::Type{<:Filter}) = true
rows(f::Filter) = f
schema(f::Filter) = schema(f.x)

Base.IteratorSize(::Type{Filter{F, T}}) where {F, T} = Base.SizeUnknown()
Base.IteratorEltype(::Type{Filter{F, T}}) where {F, T} = Base.IteratorEltype(T)
Base.eltype(f::Filter) = eltype(f.x)

 @inline function Base.iterate(f::Filter)
    state = iterate(f.x)
    state === nothing && return nothing
    while !f.f(state[1])
        state = iterate(f.x, state[2])
        state === nothing && return nothing
    end
    return state
end

 @inline function Base.iterate(f::Filter, st)
    state = iterate(f.x, st)
    state === nothing && return nothing
    while !f.f(state[1])
        state = iterate(f.x, state[2])
        state === nothing && return nothing
    end
    return state
end