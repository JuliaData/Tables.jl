using Test, Tables

@testset "utils.jl" begin

    NT = NamedTuple{(), Tuple{}}
    @test Tables.names(NT) === ()
    @test Tables.types(NT) === ()
    @test isempty(Tables.runlength(NT))
    @test Tables.columnindex(NT, :i) == 0
    @test Tables.columntype(NT, :i) == Union{}

    NT = NamedTuple{(:a, :b, :c), NTuple{3, Int64}}
    @test Tables.names(NT) === (:a, :b, :c)
    @test Tables.types(NT) === (Int64, Int64, Int64)
    @test Tables.runlength(NT) == [(Int64, 3)]
    @test Tables.columnindex(NT, :a) == 1
    @test Tables.columnindex(NT, :i) == 0
    @test Tables.columntype(NT, :a) == Int64
    @test Tables.columntype(NT, :i) == Union{}

    NT = NamedTuple{Tuple(Symbol("a$i") for i = 1:20), Tuple{vcat(fill(Int, 10), fill(String, 10))...}}
    @test Tables.names(NT) === Tuple(Symbol("a$i") for i = 1:20)
    @test Tables.types(NT) === Tuple(vcat(fill(Int, 10), fill(String, 10)))
    @test Tables.runlength(NT) == [(Int64, 10), (String, 10)]
    @test Tables.columnindex(NT, :a20) == 20
    @test Tables.columnindex(NT, :i) == 0
    @test Tables.columntype(NT, :a20) == String
    @test Tables.columntype(NT, :i) == Union{}

    nt = (a=1, b=2, c=3)
    @test getproperty(nt, Int, 1, :a) === 1

    output = [0, 0, 0]
    Tables.unroll(typeof(nt), nt, output) do val, col, nm, out
        out[col] = val
    end
    @test output == [1, 2, 3]

    nt = NamedTuple{Tuple(Symbol("a$i") for i = 1:101)}(Tuple(i for i = 1:101))
    @test Tables.runlength(typeof(nt)) == [(Int, 101)]
    output = zeros(Int, 101)
    Tables.unroll(typeof(nt), nt, output) do val, col, nm, out
        out[col] = val
    end
    @test output == [i for i = 1:101]

    nt = NamedTuple{Tuple(Symbol("a$i") for i = 1:101)}(Tuple(i % 2 == 0 ? i : "$i" for i = 1:101))
    @test Tables.runlength(typeof(nt)) == [i % 2 == 0 ? (Int, 1) : (String, 1) for i = 1:101]
    output = Vector{Any}(undef, 101)
    Tables.unroll(typeof(nt), nt, output) do val, col, nm, out
        out[col] = val
    end
    @test output == [i % 2 == 0 ? i : "$i" for i = 1:101]

end

struct DictRow
    dict::Dict{Symbol, Int}
end
Base.getproperty(d::DictRow, nm::Symbol) = getfield(d, 1)[nm]

@testset "namedtuples.jl" begin

    nt = (a=1, b=2, c=3)
    rt = [nt, nt, nt]
    @test Tables.rows(rt) === rt
    @test Tables.schema(rt) == typeof(nt)
    @test Tables.namedtupleiterator(eltype(rt), Tables.schema(rt), rt) === rt

    dt = [DictRow(Dict(pairs(nt))) for nt in rt]
    ntitr = Tables.namedtupleiterator(eltype(dt), Tables.schema(rt), dt)
    @test eltype(ntitr) == typeof(nt)
    @test length(ntitr) == 3
    @test collect(ntitr) == rt
    
    rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
    nt = (a=[1,2,3], b=[4.0, 5.0, 6.0], c=["7", "8", "9"])
    @test Tables.rowcount(nt) == 3
    @test Tables.schema(nt) == NamedTuple{(:a, :b, :c), Tuple{Int, Float64, String}}
    @test Tables.AccessStyle(typeof(nt)) == Tables.ColumnAccess()
    @test Tables.columns(nt) === nt
    @test rowtable(nt) == rt
    @test columntable(rt) == nt
    @test rt == (rt |> columntable |> rowtable)
    @test nt == (nt |> rowtable |> columntable)
end

import Base: ==
struct GenericRow
    a::Int
    b::Float64
    c::String
end
==(a::GenericRow, b::GenericRow) = a.a == b.a && a.b == b.b && a.c == b.c

struct GenericRowTable
    data::Vector{GenericRow}
end
==(a::GenericRowTable, b::GenericRowTable) = all(a.data .== b.data)

Base.eltype(g::GenericRowTable) = GenericRow
Base.length(g::GenericRowTable) = length(g.data)
Base.size(g::GenericRowTable) = (length(g.data),)
Tables.schema(x::GenericRowTable) = NamedTuple{(:a, :b, :c), Tuple{Int, Float64, String}}

function Base.iterate(g::GenericRowTable, st=1)
    st > length(g.data) && return nothing
    return g.data[st], st + 1
end

genericrowtable(x) = GenericRowTable(collect(map(x->GenericRow(x.a, x.b, x.c), Tables.rows(x))))

struct GenericColumn{T}
    data::Vector{T}
end
Base.eltype(g::GenericColumn{T}) where {T} = T
Base.length(g::GenericColumn) = length(g.data)
==(a::GenericColumn, b::GenericColumn) = a.data == b.data
Base.getindex(g::GenericColumn, i::Int) = g.data[i]

struct GenericColumnTable
    names::Dict{Symbol, Int}
    data::Vector{GenericColumn}
end

Tables.schema(g::GenericColumnTable) = NamedTuple{Tuple(keys(getfield(g, 1))), Tuple{(eltype(x) for x in getfield(g, 2))...}}
Tables.AccessStyle(::Type{GenericColumnTable}) = Tables.ColumnAccess()
Tables.columns(x::GenericColumnTable) = x
Base.getproperty(g::GenericColumnTable, nm::Symbol) = getfield(g, 2)[getfield(g, 1)[nm]]

function genericcolumntable(x)
    sch = Tables.schema(x)
    cols = Tables.columns(x)
    data = [GenericColumn(getproperty(cols, nm)) for nm in Tables.names(sch)]
    return GenericColumnTable(Dict(nm=>i for (i, nm) in enumerate(Tables.names(sch))), data)
end
==(a::GenericColumnTable, b::GenericColumnTable) = getfield(a, 1) == getfield(b, 1) && getfield(a, 2) == getfield(b, 2)

@testset "Tables.jl" begin

    gr = GenericRowTable([GenericRow(1, 4.0, "7"), GenericRow(2, 5.0, "8"), GenericRow(3, 6.0, "9")])
    gc = GenericColumnTable(Dict(:a=>1, :b=>2, :c=>3), [GenericColumn([1,2,3]), GenericColumn([4.0, 5.0, 6.0]), GenericColumn(["7", "8", "9"])])
    @test gc == (gr |> genericcolumntable)
    @test gr == (gc |> genericrowtable)
    @test gr == (gr |> genericrowtable)
    
end
