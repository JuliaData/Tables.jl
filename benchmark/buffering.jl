# Run against the desired Tables checkout in a separate Julia process:
# julia --project=benchmark -e 'using Pkg; Pkg.develop(path="."); Pkg.instantiate()'
# julia --project=benchmark benchmark/buffering.jl [row_count=100000]
# For whole-process peak RSS, warm up on a small input, then construct once:
# /usr/bin/time -l julia --project=benchmark benchmark/buffering.jl 1000000 rss unknown_dense columntable
using Tables, BenchmarkTools

struct UnknownRows{T}
    rows::T
end
Tables.isrowtable(::Type{<:UnknownRows}) = true
Tables.schema(::UnknownRows) = nothing
Base.IteratorSize(::Type{UnknownRows{T}}) where {T} = Base.IteratorSize(T)
Base.IteratorEltype(::Type{UnknownRows{T}}) where {T} = Base.IteratorEltype(T)
Base.eltype(::Type{UnknownRows{T}}) where {T} = eltype(T)
Base.length(x::UnknownRows) = length(x.rows)
Base.iterate(x::UnknownRows, args...) = iterate(x.rows, args...)

function source(case, n)
    if case == "known_dense"
        return [(a=i, b=Float64(i), c="value") for i in 1:n]
    elseif case == "unknown_dense"
        return UnknownRows(source("known_dense", n))
    elseif case == "unknown_stream"
        return UnknownRows(Iterators.filter(_ -> true, source("known_dense", n)))
    elseif case == "late_widening"
        rows = NamedTuple{(:a,)}[(a=i == 1 ? typemax(Int64) : Int64(i),) for i in 1:n]
        rows[end-1] = (a=0.0,)
        rows[end] = (a="value",)
        return rows
    elseif case == "unknown_wide"
        names = ntuple(i -> Symbol(:c, i), 32)
        return UnknownRows([NamedTuple{names}(ntuple(_ -> i, 32)) for i in 1:n])
    elseif case == "sparse"
        return [i % 3 == 0 ? (a=i, c="value") : (a=i, b=Float64(i)) for i in 1:n]
    end
    error("unknown case: $case")
end

function check(case, result, n)
    length(Tables.getcolumn(result, 1)) == n || error("wrong row count")
    if case == "late_widening"
        return Tables.getcolumn(result, :a)[1] === typemax(Int64)
    end
    return true
end

function main(args)
    n = isempty(args) ? 100_000 : parse(Int, args[1])
    println("# Julia=", VERSION, " Tables=", pkgversion(Tables), " threads=", Threads.nthreads(), " CPU=", Sys.CPU_NAME)
    if length(args) >= 2 && args[2] == "rss"
        case, name = args[3:4]
        f = name == "columntable" ? Tables.columntable : Tables.dictcolumntable
        f(source(case, 1000))
        GC.gc()
        input = source(case, n)
        result = f(input)
        println("case=", case, " constructor=", name, " n=", n, " preserved=", check(case, result, n), " output_bytes=", Base.summarysize(result))
        return
    end
    println("case,constructor,rows,samples,median_ns,min_ns,allocated_bytes,allocations,preserved")
    for case in ("known_dense", "unknown_dense", "unknown_stream", "late_widening", "unknown_wide", "sparse")
        input = source(case, n)
        for (name, f) in (("columntable", Tables.columntable), ("dictcolumntable", Tables.dictcolumntable))
            case == "sparse" && name == "columntable" && continue
            preserved = check(case, f(input), n)
            GC.gc()
            trial = @benchmark $f($input) evals=1 samples=200 seconds=3
            med = median(trial)
            low = minimum(trial)
            println(join((case, name, n, length(trial), med.time, low.time, med.memory, med.allocs, preserved), ','))
            flush(stdout)
        end
    end
end
main(ARGS)
