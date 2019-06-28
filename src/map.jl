struct MissingValue{T}
    x::Union{T, Missing}
end

missingvalue() = MissingValue{Union{}}(missing)
missingvalue(x) = MissingValue(x)
missingvalue(::Missing) = MissingValue{Missing}(missing)
missingvalue(::Type{T}, x) where {T} = MissingValue{T}(x)
missingvalue(::Type{Missing}, x) = MissingValue{Missing}(missing)

Base.getindex(x::MissingValue) = x.x

Base.ismissing(x::MissingValue) = ismissing(x[])
Base.nonmissingtype(::Type{MissingValue{T}}) where {T} = T

Base.promote_rule(::Type{MissingValue{S}}, ::Type{T}) where {S,T} = MissingValue{promote_type(S, T)}
Base.promote_rule(::Type{MissingValue{T}}, ::Type{Any}) where {T} = MissingValue{Any}
Base.promote_rule(::Type{MissingValue{Union{}}}, ::Type{Any}) = MissingValue{Any}
Base.promote_rule( ::Type{Any}, ::Type{MissingValue{Union{}}}) = MissingValue{Any}
Base.promote_rule(::Type{MissingValue{S}}, ::Type{MissingValue{T}}) where {S,T} = MissingValue{promote_type(S, T)}

Base.convert(::Type{Union{Missing, T}}, value::MissingValue{T}) where T = value[]

import Base: ==, isequal, <, isless, isapprox, !, ~, +, -, *, /, ^, &, |, xor

# Unary operators/functions
for f in (:(!), :(~), :(+), :(-), :(zero), :(one), :(oneunit),
          :(isfinite), :(isinf), :(isodd),
          :(isinteger), :(isreal), :(isnan),
          :(iszero), :(transpose), :(adjoint), :(float), :(conj),
          :(abs), :(abs2), :(iseven), :(ispow2),
          :(real), :(imag), :(sign), :(inv))
    @eval ($f)(x::MissingValue{T}) where {T} = MissingValue{T}(($f)(x[]))
end

# Binary comparisons
for f in (:(==), :isequal, :(<), :isless, :isapprox, :min, :max)
    @eval begin
        # Scalar with missing
        ($f)(x::MissingValue, y::MissingValue) = ($f)(x[], y[])
        ($f)(x::MissingValue, y::Number)  = ($f)(x[], y)
        ($f)(y::Number, x::MissingValue) = ($f)(x[], y)
    end
end

# Number binary operators
for f in (:(+), :(-), :(*), :(/), :(^), :(div), :(mod), :(fld), :(rem))
    @eval begin
        # Scalar with missing
        ($f)(x::MissingValue{T}, y::MissingValue{T}) where {T} = MissingValue{T}(($f)(x[], y[]))
        ($f)(x::MissingValue{T}, y::MissingValue{S}) where {T, S} = MissingValue{promote_type(T, S)}(($f)(x[], y[]))
        ($f)(x::MissingValue{T}, y::Number) where {T}  = MissingValue{T}(($f)(x[], y))
        ($f)(y::Number, x::MissingValue{T}) where {T} = MissingValue{T}(($f)(x[], y))
    end
end

# Bit operators
for f in (:(&), :(|), :xor)
    @eval begin
        ($f)(x::MissingValue, y::MissingValue) = ($f)(x[], y[])
        ($f)(x::MissingValue, y::Bool)  = ($f)(x[], y)
        ($f)(y::Bool, x::MissingValue) = ($f)(x[], y)
        ($f)(x::MissingValue, y::Integer)  = ($f)(x[], y)
        ($f)(y::Integer, x::MissingValue) = ($f)(x[], y)
    end
end

# to avoid ambiguity warnings
(^)(x::MissingValue{T}, y::Integer) where {T} = MissingValue{T}(^(x[], y))
==(x::MissingValue, y::WeakRef) = ==(x[], y)
==(y::WeakRef, x::MissingValue) = ==(x[], y)
*(x::MissingValue{T}, y::AbstractString) where {T} = MissingValue{T}(*(x[], y))
*(y::AbstractString, x::MissingValue{T}) where {T} = MissingValue{T}(*(x[], y))

Base.coalesce(x::MissingValue, y...) = coalesce(x[], y...)

# Rounding and related functions
# round(::Missing, ::RoundingMode=RoundNearest; sigdigits::Integer=0, digits::Integer=0, base::Integer=0) = missing
# round(::Type{>:Missing}, ::Missing, ::RoundingMode=RoundNearest) = missing
# round(::Type{T}, ::Missing, ::RoundingMode=RoundNearest) where {T} =
#     throw(MissingException("cannot convert a missing value to type $T: use Union{$T, Missing} instead"))
# round(::Type{T}, x::Any, r::RoundingMode=RoundNearest) where {T>:Missing} = round(nonmissingtype(T), x, r)
# to fix ambiguities
# round(::Type{T}, x::Rational, r::RoundingMode=RoundNearest) where {T>:Missing} = round(nonmissingtype(T), x, r)
# round(::Type{T}, x::Rational{Bool}, r::RoundingMode=RoundNearest) where {T>:Missing} = round(nonmissingtype(T), x, r)

# Handle ceil, floor, and trunc separately as they have no RoundingMode argument
# for f in (:(ceil), :(floor), :(trunc))
#     @eval begin
#         ($f)(::Missing; sigdigits::Integer=0, digits::Integer=0, base::Integer=0) = missing
#         ($f)(::Type{>:Missing}, ::Missing) = missing
#         ($f)(::Type{T}, ::Missing) where {T} =
#             throw(MissingException("cannot convert a missing value to type $T: use Union{$T, Missing} instead"))
#         ($f)(::Type{T}, x::Any) where {T>:Missing} = $f(nonmissingtype(T), x)
#         # to fix ambiguities
#         ($f)(::Type{T}, x::Rational) where {T>:Missing} = $f(nonmissingtype(T), x)
#     end
# end

nonmissingvaluetype(::Type{T}) where {T} = T
nonmissingvaluetype(::Type{MissingValue{T}}) where {T} = Union{T, Missing}

missingvaluetype(::Type{T}) where {T} = T
missingvaluetype(::Type{T}) where {T <: MissingValue} = T
missingvaluetype(::Type{Union{T, Missing}}) where {T} = MissingValue{T}
missingvaluetype(::Type{Missing}) = MissingValue{Union{}}

Base.@pure function nonmissingtypenamedtupletype(::Type{NT}) where {NT <: NamedTuple{names}} where {names}
    TT = Tuple{Any[ nonmissingvaluetype(fieldtype(NT, i)) for i = 1:fieldcount(NT) ]...}
    return NamedTuple{names, TT}
end

Base.@pure function missingtypenamedtupletype(::Schema{names, types}) where {names, types}
    TT = Tuple{Any[ missingvaluetype(fieldtype(types, i)) for i = 1:fieldcount(types) ]...}
    return NamedTuple{names, TT}
end

wrap(::Type{T}, x) where {T} = x
wrap(u::Union, x) = missingvaluetype(u)(x)

unwrap2(x) = x
unwrap2(nt::NT) where {NT <: NamedTuple} = nonmissingtypenamedtupletype(NT)(nt)

# map
struct Map{F, T}
    func::F
    source::T
end

function Map(f::F, x::T) where {F <: Base.Callable, T}
    r = rows(x)
    return Map{F, typeof(r)}(f, r)
end
Map(f::Base.Callable) = x->Map(f, x)

istable(::Type{<:Map}) = true
rowaccess(::Type{<:Map}) = true
rows(m::Map) = m
schema(m::Map) = nothing

Base.IteratorSize(::Type{Map{T, F}}) where {T, F} = Base.IteratorSize(T)
Base.length(m::Map) = length(m.source)
Base.IteratorEltype(::Type{<:Map}) = Base.EltypeUnknown()

struct MissingValueWrapper{T}
    x::T
end

@inline function Base.getproperty(m::MissingValueWrapper, nm::Symbol)
    T = propertytype(getfield(m, :x), nm)
    x = wrap(T, getproperty(getfield(m, :x), nm))
    return x
end

@inline function Base.iterate(m::Map)
    state = iterate(m.source)
    state === nothing && return nothing
    x = m.func(MissingValueWrapper(state[1]))
    # @show x
    return unwrap2(x), state[2]
end

@inline function Base.iterate(m::Map, st)
    state = iterate(m.source, st)
    state === nothing && return nothing
    return unwrap2(m.func(MissingValueWrapper(state[1]))), state[2]
end
