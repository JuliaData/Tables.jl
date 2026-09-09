using Test, Tables

# A single-use source that returns the same mutable row on every iteration.
struct BufferedTestRows{T, S}
    source::T
    row::Dict{Symbol, Any}
    started::Base.RefValue{Bool}
    size::S
end
BufferedTestRows(rows, size=Base.SizeUnknown()) = BufferedTestRows(rows, Dict{Symbol, Any}(), Ref(false), size)
Tables.isrowtable(::Type{<:BufferedTestRows}) = true
Tables.schema(::BufferedTestRows) = nothing
Base.IteratorSize(::Type{BufferedTestRows{T, S}}) where {T, S} = S()
Base.IteratorEltype(::Type{<:BufferedTestRows}) = Base.EltypeUnknown()
Base.length(rows::BufferedTestRows) = length(rows.source)
function Base.iterate(rows::BufferedTestRows)
    rows.started[] && error("source was restarted")
    rows.started[] = true
    return iterate(rows, 1)
end
function Base.iterate(rows::BufferedTestRows, i::Int)
    i > length(rows.source) && return nothing
    empty!(rows.row)
    for nm in Tables.columnnames(rows.source[i])
        rows.row[nm] = Tables.getcolumn(rows.source[i], nm)
    end
    return rows.row, i + 1
end

@testset "Unknown-schema buffering" begin
    rows = [(a=typemax(Int64),), (a=0.0,), (a="",)]
    for constructor in (Tables.columntable, Tables.dictcolumntable)
        for p in ((1,2,3), (1,3,2), (2,1,3), (2,3,1), (3,1,2), (3,2,1))
            input = rows[collect(p)]
            for source in (input, Iterators.filter(_ -> true, input),
                           BufferedTestRows(input), BufferedTestRows(input, Base.HasLength()))
                col = constructor(source).a
                @test eltype(col) === Any
                @test all(col[i] === input[i].a for i in eachindex(input))
            end
        end
        # Intermediate conversion can throw, even when the final type is Any.
        col = constructor([(a=-1,), (a=UInt64(1),), (a="",)]).a
        @test col[1] === -1
        @test col[2] === UInt64(1)

        col = constructor([(a=typemax(Int64),), (a=0.0,), (a=BigFloat(0),)]).a
        @test eltype(col) === BigFloat
        @test col[1] == BigFloat(typemax(Int64))

        col = constructor(BufferedTestRows([(a=1,), (a=2.0,), (a=missing,)])).a
        @test eltype(col) === Union{Missing, Float64}
        @test isequal(col, [1.0, 2.0, missing])
        @test col[1] === 1.0

        # Cell contents retain the usual shallow-reference semantics.
        cell = [1, 2]
        @test constructor(BufferedTestRows([(a=cell,)])).a[1] === cell
        @test isempty(Tables.columnnames(constructor(BufferedTestRows(NamedTuple[]))))
        @test isempty(Tables.columnnames(constructor(BufferedTestRows([NamedTuple(), NamedTuple()]))))
    end

    # Fixed names come from the first row, even if later rows add/reorder names.
    ct = Tables.columntable(BufferedTestRows([(a=1, b=2), (b=3, a=4, c=5)]))
    @test ct.a == [1, 4]
    @test ct.b == [2, 3]
    @test !(:c in keys(ct))

    sparse = [(a=typemax(Int64),), (b=1, a=0.0), (b=2,), (a="", c=true)]
    for input in (sparse, BufferedTestRows(sparse))
        ct = Tables.dictcolumntable(input)
        @test Set(Tables.columnnames(ct)) == Set((:a, :b, :c))
        @test ct.a[1] === typemax(Int64)
        @test isequal(ct.a, Any[typemax(Int64), 0.0, missing, ""])
        @test isequal(ct.b, [missing, 1, 2, missing])
        @test isequal(ct.c, [missing, missing, missing, true])
        @test eltype(ct.b) === Union{Missing, Int}
        @test eltype(ct.c) === Union{Missing, Bool}
    end
    ct = Tables.dictcolumntable(sparse)
    @test Tables.columnnames(ct) == (:a, :b, :c)
    ct = Tables.dictcolumntable(BufferedTestRows([NamedTuple(); rows]))
    @test ismissing(ct.a[1])
    @test ct.a[2] === typemax(Int64)
end
