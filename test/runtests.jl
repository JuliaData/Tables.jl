using Test, Tables

@testset "utils.jl" begin

    NT = NamedTuple{(), Tuple{}}
    @test Tables.names(NT) === ()
    @test Tables.types(NT) === Tuple{}
    @test isempty(Tables.runlength(Tables.types(NT)))
    @test Tables.columnindex(Tables.names(NT), :i) == 0
    @test Tables.columntype(Tables.names(NT), Tables.types(NT), :i) == Union{}

    NT = NamedTuple{(:a, :b, :c), NTuple{3, Int64}}
    @test Tables.names(NT) === (:a, :b, :c)
    @test Tables.types(NT) === Tuple{Int64, Int64, Int64}
    @test Tables.runlength(Tables.types(NT)) == [(Int64, 3)]
    @test Tables.columnindex(Tables.names(NT), :a) == 1
    @test Tables.columnindex(Tables.names(NT), :i) == 0
    @test Tables.columntype(Tables.names(NT), Tables.types(NT), :a) == Int64
    @test Tables.columntype(Tables.names(NT), Tables.types(NT), :i) == Union{}

    NT = NamedTuple{Tuple(Symbol("a$i") for i = 1:20), Tuple{vcat(fill(Int, 10), fill(String, 10))...}}
    @test Tables.names(NT) === Tuple(Symbol("a$i") for i = 1:20)
    @test Tables.types(NT) === Tuple{vcat(fill(Int, 10), fill(String, 10))...}
    @test Tables.runlength(Tables.types(NT)) == [(Int, 10), (String, 10)]
    @test Tables.columnindex(Tables.names(NT), :a20) == 20
    @test Tables.columnindex(Tables.names(NT), :i) == 0
    @test Tables.columntype(Tables.names(NT), Tables.types(NT), :a20) == String
    @test Tables.columntype(Tables.names(NT), Tables.types(NT), :i) == Union{}

    nt = (a=1, b=2, c=3)
    @test getproperty(nt, Int, 1, :a) === 1

    NT = typeof(nt)
    output = [0, 0, 0]
    Tables.eachcolumn(Tables.Schema(Tables.names(NT), Tables.types(NT)), nt, output) do val, col, nm, out
        out[col] = val
    end
    @test output == [1, 2, 3]

    nt = NamedTuple{Tuple(Symbol("a$i") for i = 1:101)}(Tuple(i for i = 1:101))
    NT =typeof(nt)
    @test Tables.runlength(Tables.types(NT)) == [(Int, 101)]
    output = zeros(Int, 101)
    Tables.eachcolumn(Tables.Schema(Tables.names(NT), Tables.types(NT)), nt, output) do val, col, nm, out
        out[col] = val
    end
    @test output == [i for i = 1:101]

    nt = NamedTuple{Tuple(Symbol("a$i") for i = 1:101)}(Tuple(i % 2 == 0 ? i : "$i" for i = 1:101))
    NT = typeof(nt)
    @test Tables.runlength(Tables.types(NT)) == [i % 2 == 0 ? (Int, 1) : (String, 1) for i = 1:101]
    output = Vector{Any}(undef, 101)
    Tables.eachcolumn(Tables.Schema(Tables.names(NT), Tables.types(NT)), nt, output) do val, col, nm, out
        out[col] = val
    end
    @test output == [i % 2 == 0 ? i : "$i" for i = 1:101]

    nt = (a=Ref(0), b=Ref(0))
    Tables.eachcolumn(Tables.Schema((:a, :b), nothing), (a=1, b=2)) do val, col, nm
        nt[nm][] = val
    end
    @test nt.a[] == 1
    @test nt.b[] == 2

    nt = (a=[1,2,3], b=[4,5,6])
    @test collect(Tables.eachcolumn(nt)) == [[1,2,3], [4,5,6]]
end

@testset "namedtuples.jl" begin

    nt = (a=1, b=2, c=3)
    rt = [nt, nt, nt]
    @test Tables.rows(rt) === rt
    @test Tables.schema(rt).names == Tables.names(typeof(nt))
    @test Tables.namedtupleiterator(eltype(rt), rt) === rt

    rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
    nt = (a=[1,2,3], b=[4.0, 5.0, 6.0], c=["7", "8", "9"])
    @test Tables.rowcount(nt) == 3
    @test Tables.schema(nt) == Tables.Schema((:a, :b, :c), Tuple{Int, Float64, String})
    @test Tables.columnaccess(typeof(nt))
    @test Tables.columns(nt) === nt
    @test rowtable(nt) == rt
    @test columntable(rt) == nt
    @test rt == (rt |> columntable |> rowtable)
    @test nt == (nt |> rowtable |> columntable)

    @test Tables.buildcolumns(nothing, rt) == nt
    @test Tables.columntable(nothing, nt) == nt

    # append
    nt2 = columntable(nt, rt)
    @test Tables.rowcount(nt2) == 6
    @test Tables.schema(nt2) == Tables.Schema((:a, :b, :c), Tuple{Int, Float64, String})
    @test nt2 == (a = [1, 2, 3, 1, 2, 3], b = [4.0, 5.0, 6.0, 4.0, 5.0, 6.0], c = ["7", "8", "9", "7", "8", "9"])
    rt2 = rowtable(rt, nt)
    @test length(rt2) == 9

    rt = [(a=1, b=4.0, c="7"), (a=2.0, b=missing, c="8"), (a=3, b=6.0, c="9")]
    tt = Tables.buildcolumns(nothing, rt)
    @test isequal(tt, (a = [1.0, 2.0, 3.0], b = Union{Missing, Float64}[4.0, missing, 6.0], c = ["7", "8", "9"]))
    @test tt.a[1] === 1.0
    @test tt.a[2] === 2.0
    @test tt.a[3] === 3.0

    nti = Tables.NamedTupleIterator{Nothing, typeof(rt)}(rt)
    nti2 = collect(nti)
    @test isequal(rt, nti2)
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
Tables.istable(::Type{GenericRowTable}) = true
Tables.rowaccess(::Type{GenericRowTable}) = true
Tables.rows(x::GenericRowTable) = x
Tables.schema(x::GenericRowTable) = Tables.Schema((:a, :b, :c), Tuple{Int, Float64, String})

function Base.iterate(g::GenericRowTable, st=1)
    st > length(g.data) && return nothing
    return g.data[st], st + 1
end

genericrowtable(x) = GenericRowTable(collect(map(x->GenericRow(x.a, x.b, x.c), Tables.rows(x))))

struct GenericColumn{T} <: AbstractVector{T}
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

Tables.istable(::Type{GenericColumnTable}) = true
Tables.columnaccess(::Type{GenericColumnTable}) = true
Tables.columns(x::GenericColumnTable) = x
Tables.schema(g::GenericColumnTable) = Tables.Schema(Tuple(keys(getfield(g, 1))), Tuple{(eltype(x) for x in getfield(g, 2))...})
Base.getproperty(g::GenericColumnTable, nm::Symbol) = getfield(g, 2)[getfield(g, 1)[nm]]
Base.propertynames(g::GenericColumnTable) = Tuple(keys(getfield(g, 1)))

function genericcolumntable(x)
    cols = Tables.columns(x)
    sch = Tables.schema(x)
    data = [GenericColumn(getproperty(cols, nm)) for nm in sch.names]
    return GenericColumnTable(Dict(nm=>i for (i, nm) in enumerate(sch.names)), data)
end
==(a::GenericColumnTable, b::GenericColumnTable) = getfield(a, 1) == getfield(b, 1) && getfield(a, 2) == getfield(b, 2)

@testset "Tables.jl interface" begin

    @test !Tables.istable(1)
    @test !Tables.istable(Int)
    @test !Tables.rowaccess(1)
    @test !Tables.rowaccess(Int)
    @test !Tables.columnaccess(1)
    @test !Tables.columnaccess(Int)
    @test Tables.schema(1) === nothing

    sch = Tables.Schema{(:a, :b), Tuple{Int64, Float64}}()
    @test Tables.Schema((:a, :b), Tuple{Int64, Float64}) === sch
    @test Tables.Schema(NamedTuple{(:a, :b), Tuple{Int64, Float64}}) === sch
    @test Tables.Schema((:a, :b), nothing) === Tables.Schema{(:a, :b), nothing}()
    @test Tables.Schema([:a, :b], [Int64, Float64]) === sch
    show(sch)
    @test sch.names == (:a, :b)
    @test sch.types == (Int64, Float64)
    @test_throws ArgumentError sch.foobar
    
    gr = GenericRowTable([GenericRow(1, 4.0, "7"), GenericRow(2, 5.0, "8"), GenericRow(3, 6.0, "9")])
    gc = GenericColumnTable(Dict(:a=>1, :b=>2, :c=>3), [GenericColumn([1,2,3]), GenericColumn([4.0, 5.0, 6.0]), GenericColumn(["7", "8", "9"])])
    @test gc == (gr |> genericcolumntable)
    @test gr == (gc |> genericrowtable)
    @test gr == (gr |> genericrowtable)
end

@static if :Query in Symbol.(Base.loaded_modules_array())
    rt = (a = Real[1, 2.0, 3], b = Union{Missing, Float64}[4.0, missing, 6.0], c = ["7", "8", "9"])

    dv = Tables.datavaluerows(rt)
    @test eltype(dv) == NamedTuple{(:a, :b, :c),Tuple{Real,DataValue{Float64},String}}
    rt2 = collect(dv)
    @test rt2[1] == (a = 1, b = DataValue{Float64}(4.0), c = "7")

    ei = Tables.DataValueUnwrapper(QueryOperators.EnumerableIterable{eltype(dv), typeof(dv)}(dv))
    nt = ei |> columntable
    @test isequal(rt, nt)
    rt3 = ei |> rowtable
    @test isequal(rt |> rowtable, rt3)

    # rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
    mt = Tables.DataValueUnwrapper(ei.x |> y->QueryOperators.map(y, x->(a=x.b, c=x.c), Expr(:block)))
    @inferred (mt |> columntable)
    @inferred (mt |> rowtable)

    # uninferrable case
    mt = Tables.DataValueUnwrapper(ei.x |> y->QueryOperators.map(y, x->(a=x.a, c=x.c), Expr(:block)))
    @test (mt |> columntable) == (a = Real[1, 2.0, 3], c = ["7", "8", "9"])
    @test length(mt |> rowtable) == 3
end