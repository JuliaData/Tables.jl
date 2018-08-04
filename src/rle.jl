"helper struct for encoding the run-length encoding of schema types"
struct RLE{T} end

# make a run-length encoding type for any iterable of types
function RLE(types)
    rle = []
    st = iterate(types)
    st === nothing && return RLE{Tuple{}}
    curT, state = st
    len = curlen = 1
    prevT = curT
    while true
        st = iterate(types, state)
        st === nothing && break
        curT, state = st
        if curT === prevT
            len += 1
        else
            push!(rle, Tuple{prevT, len})
            prevT = curT
            len = 1
        end
    end
    push!(rle, Tuple{curT, curlen})
    return RLE{Tuple{rle...}}
end

function Base.iterate(::Type{RLE{rle}}, st::Int=1) where {rle}
    len = length(rle.parameters)
    st > len && return nothing
    return Tuple(rle.parameters[st].parameters), st+1
end

function types(::Type{T}) where {T <: RLE}
    typs = []
    for t in T
        for i = 1:t[2]
            push!(typs, t[1])
        end
    end
    return typs
end