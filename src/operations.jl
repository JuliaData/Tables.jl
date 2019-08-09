struct TransformsRow{T, F}
    row::T
    funcs::F
end

Base.getproperty(row::TransformsRow, nm::Symbol) = (getfunc(row, getfield(row, 2), nm))(getproperty(getfield(row, 1), nm))
Base.propertynames(row::TransformsRow) = propertynames(getfield(row, 1))

struct Transforms{C, T, F}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end

Base.propertynames(t::Transforms{true}) = propertynames(getfield(t, 1))
Base.getproperty(t::Transforms{true}, nm::Symbol) = Base.map(getfunc(t, getfield(t, 2), nm), getproperty(getfield(t, 1), nm))

transform(funcs) = x->transform(x, funcs)
transform(; kw...) = transform(kw.data)
function transform(src::T, funcs::F) where {T, F}
    C = columnaccess(T)
    x = C ? columns(src) : rows(src)
    return Transforms{C, typeof(x), F}(x, funcs)
end

getfunc(row, nt::NamedTuple, nm) = get(nt, nm, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, nm) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, nm) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, nm) = get(d, findfirst(isequal(nm), propertynames(row)), identity)

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
Base.getproperty(s::Select{T, true, names}, nm::Symbol) where {T, names} = getproperty(getfield(s, 1), nm)
Base.propertynames(s::Select{T, true, names}) where {T, names} = namesubset(propertynames(getfield(s, 1)), names)
columnaccess(::Type{Select{T, C, names}}) where {T, C, names} = C
columns(s::Select{T, true, names}) where {T, names} = s

# rows: implement Iterator interface
Base.IteratorSize(::Type{Select{T, false, names}}) where {T, names} = Base.IteratorSize(T)
Base.length(s::Select{T, false, names}) where {T, names} = length(getfield(s, 1))
Base.IteratorEltype(::Type{Select{T, false, names}}) where {T, names} = Base.IteratorEltype(T)
Base.eltype(s::Select{T, false, names}) where {T, names} = SelectRow{eltype(getfield(s, 1)), names}
rowaccess(::Type{Select{T, columnaccess, names}}) where {T, columnaccess, names} = !columnaccess
rows(s::Select{T, false, names}) where {T, names} = s

# we need to iterate a "row view" in case the underlying source has unknown schema
# to ensure each iterated row only has `names` propertynames
struct SelectRow{T, names}
    row::T
end

Base.getproperty(row::SelectRow, nm::Symbol) = getproperty(getfield(row, 1), nm)

getprops(row, nms::NTuple{N, Symbol}) where {N} = nms
getprops(row, inds::NTuple{N, Int}) where {N} = ntuple(i->propertynames(getfield(row, 1))[inds[i]], N)
getprops(row, ::Tuple{}) = ()

Base.propertynames(row::SelectRow{T, names}) where {T, names} = getprops(row, names)

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
