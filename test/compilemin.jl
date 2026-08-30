using Tables, Test

struct CompileMinColumns <: Tables.AbstractColumns
    a::Vector{Int}
    b::Vector{Float64}
end

Tables.columnnames(::CompileMinColumns) = (:a, :b)
Tables.getcolumn(x::CompileMinColumns, i::Int) = getfield(x, i)
Tables.getcolumn(x::CompileMinColumns, name::Symbol) = getfield(x, name)
Tables.schema(::CompileMinColumns) = Tables.Schema((:a, :b), (Int, Float64))

@testset "non-generated fallbacks" begin
    schema = Tables.Schema((:a, :b), (Int, Float64))
    columns = Tables.allocatecolumns(schema, 2)
    @test columns isa NamedTuple{(:a, :b), Tuple{Vector{Int}, Vector{Float64}}}

    rows = [(a=1, b=2.0), (a=3, b=4.0)]
    @test collect(Tables.namedtupleiterator(rows)) == rows

    source = CompileMinColumns([1, 3], [2.0, 4.0])
    materialized = Tables.columntable(source)
    @test materialized == (a=[1, 3], b=[2.0, 4.0])
    @test materialized.a === source.a
    @test materialized.b === source.b

    @test collect(Tables.datavaluerows(rows)) == rows
end
