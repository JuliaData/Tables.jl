using Test, Tables, TableTraits, DataValues, QueryOperators, IteratorInterfaceExtensions

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
    NT = typeof(nt)
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
    @test Tables.columnindex(nt, :i) == 0
    @test Tables.columnindex(nt, :a) == 1
    @test Tables.columntype(nt, :a) == Int
    @test Tables.columntype(nt, :i) == Union{}

    rows = Tables.rows(nt)
    @test eltype(rows) == Tables.ColumnsRow{typeof(nt)}
    @test Tables.schema(rows) == Tables.Schema((:a, :b), (Int, Int))
    row = first(rows)
    @test row.a == 1

    @test Tables.sym(1) === 1
    @test Tables.sym("hey") == :hey

    @test propertynames(Tables.Schema((:a, :b), nothing)) == (:names, :types)

    v = Tables.EmptyVector(1)
    @test_throws UndefRefError v[1]
    @test Base.IndexStyle(typeof(v)) == Base.IndexLinear()

    @test Tables.istable(Tables.CopiedColumns)
    @test Tables.columnaccess(Tables.CopiedColumns)
    c = Tables.CopiedColumns(nt)
    @test Tables.columns(c) === c
    @test Tables.materializer(c) == Tables.materializer(nt)

    @test_throws ArgumentError Tables.columntable([1,2,3])

    tt = [(1,2,3), (4,5,6)]
    r = Tables.nondatavaluerows(tt)
    row = first(r)
    @test getproperty(row, 1) == 1
    @test Tables.columntable(tt) == NamedTuple{(Symbol("1"), Symbol("2"), Symbol("3"))}(([1, 4], [2, 5], [3, 6]))

    @test Tables.getarray([1,2,3]) == [1,2,3]
    @test Tables.getarray((1,2,3)) == [1,2,3]
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
    @test Tables.istable(typeof(nt))
    @test Tables.columnaccess(typeof(nt))
    @test Tables.columns(nt) === nt
    @test rowtable(nt) == rt
    @test columntable(rt) == nt
    @test rt == (rt |> columntable |> rowtable)
    @test nt == (nt |> rowtable |> columntable)

    @test Tables.buildcolumns(nothing, rt) == nt
    @test Tables.columntable(nothing, nt) == nt

    # test push!
    rtf = Iterators.Filter(x->x.a >= 1, rt)
    @test Tables.columntable(rtf) == nt
    @test Tables.buildcolumns(nothing, rtf) == nt

    # append
    nt2 = columntable(nt, rt)
    @test Tables.rowcount(nt2) == 6
    @test Tables.schema(nt2) == Tables.Schema((:a, :b, :c), Tuple{Int, Float64, String})
    @test nt2 == (a = [1, 2, 3, 1, 2, 3], b = [4.0, 5.0, 6.0, 4.0, 5.0, 6.0], c = ["7", "8", "9", "7", "8", "9"])
    rt2 = rowtable(rt, nt)
    @test length(rt2) == 9

    rt = [(a=1, b=4.0, c="7"), (a=2.0, b=missing, c="8"), (a=3, b=6.0, c="9")]
    @test Tables.istable(typeof(rt))
    @test Tables.rowaccess(typeof(rt))
    tt = Tables.buildcolumns(nothing, rt)
    @test isequal(tt, (a = [1.0, 2.0, 3.0], b = Union{Missing, Float64}[4.0, missing, 6.0], c = ["7", "8", "9"]))
    @test tt.a[1] === 1.0
    @test tt.a[2] === 2.0
    @test tt.a[3] === 3.0

    nti = Tables.NamedTupleIterator{Nothing, typeof(rt)}(rt)
    @test Base.IteratorEltype(typeof(nti)) == Base.EltypeUnknown()
    @test Base.IteratorSize(typeof(nti)) == Base.HasShape{1}()
    @test length(nti) == 3
    nti2 = collect(nti)
    @test isequal(rt, nti2)
    nti = Tables.NamedTupleIterator{typeof(Tables.Schema((:a, :b, :c), (Union{Int, Float64}, Union{Float64, Missing}, String))), typeof(rt)}(rt)
    @test eltype(typeof(nti)) == NamedTuple{(:a, :b, :c),Tuple{Union{Float64, Int},Union{Missing, Float64},String}}

    # test really wide tables
    nms = Tuple(Symbol("i", i) for i = 1:101)
    vals = Tuple(rand(Int, 3) for i = 1:101)
    nt = NamedTuple{nms}(vals)
    rt = Tables.rowtable(nt)
    @test length(rt) == 3
    @test length(rt[1]) == 101
    @test eltype(rt).parameters[1] == nms
    @test Tables.columntable(rt) == nt
    @test Tables.buildcolumns(nothing, rt) == nt
end

@testset "Materializer" begin 
    rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
    nt = (a=[1,2,3], b=[4.0, 5.0, 6.0], c=["7", "8", "9"])

    @test nt == Tables.materializer(nt)(Tables.columns(nt))
    @test nt == Tables.materializer(nt)(Tables.columns(rt))
    @test nt == Tables.materializer(nt)(rt)
    @test rt == Tables.materializer(rt)(nt)

    function select(table, cols::Symbol...)
        Tables.istable(table) || throw(ArgumentError("select requires a table input"))
        nt = Tables.columntable(table)  # columntable(t) creates a NamedTuple of AbstractVectors
        newcols = NamedTuple{cols}(nt)
        Tables.materializer(table)(newcols)
    end

    @test select(nt, :a, :b, :c) == nt
    @test select(nt, :c, :a) == NamedTuple{(:c, :a)}(nt)
    @test select(rt, :a) == [(a=1,), (a=2,), (a=3,)]

    @test Tables.materializer(1) === Tables.columntable
end

@testset "Matrix integration" begin
    rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
    nt = (a=[1,2,3], b=[4.0, 5.0, 6.0])

    mat = Tables.matrix(rt)
    @test nt.a == mat[:, 1]
    @test size(mat) == (3, 3)
    @test eltype(mat) == Any
    @test_throws ArgumentError Tables.rows(mat)
    @test_throws ArgumentError Tables.columns(mat)
    mat2 = Tables.matrix(nt)
    @test eltype(mat2) == Float64
    @test mat2[:, 1] == nt.a
    @test !Tables.istable(mat2)
    @test !Tables.istable(typeof(mat2))
    mat3 = Tables.matrix(nt; transpose=true)
    @test size(mat3) == (2, 3)
    @test mat3[1, :] == nt.a
    @test mat3[2, :] == nt.b

    tbl = Tables.table(mat) |> columntable
    @test keys(tbl) == (:Column1, :Column2, :Column3)
    @test tbl.Column1 == [1, 2, 3]
    tbl2 = Tables.table(mat2) |> rowtable
    @test length(tbl2) == 3
    @test map(x->x.Column1, tbl2) == [1.0, 2.0, 3.0]

    mattbl = Tables.table(mat)
    @test Tables.istable(typeof(mattbl))
    @test Tables.rowaccess(typeof(mattbl))
    @test Tables.rows(mattbl) === mattbl
    @test Tables.columnaccess(typeof(mattbl))
    @test Tables.columns(mattbl) === mattbl
    @test mattbl.Column1 == [1,2,3]
    matrow = first(mattbl)
    @test eltype(mattbl) == typeof(matrow)
    @test matrow.Column1 == 1
    @test propertynames(mattbl) == propertynames(matrow) == [:Column1, :Column2, :Column3]
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

    @test_throws ArgumentError Tables.columns(Int64)
    @test_throws ArgumentError Tables.rows(Int64)
end

@testset "isless" begin
    t = (x = [1, 1, 0, 2], y = [-1, 1, 3, 2])
    a,b,c,d = Tables.rows(t)
    @test isless(a, b)
    @test isless(c, d)
    @test !isless(d, a)
    @test !isequal(a, b)
    @test isequal(a, a)
    @test sortperm([a, b, c, d]) == [3, 1, 2, 4]
end

struct ColumnSource
end

TableTraits.supports_get_columns_copy_using_missing(::ColumnSource) = true

function TableTraits.get_columns_copy_using_missing(x::ColumnSource)
    return (a=[1,2,3], b=[4.,5.,6.], c=["A", "B", "C"])
end

let x=ColumnSource()
    @test Tables.source(Tables.columns(x)) == Tables.source(Tables.CopiedColumns(TableTraits.get_columns_copy_using_missing(x)))
end

struct ColumnSource2
end

IteratorInterfaceExtensions.isiterable(x::ColumnSource2) = true
TableTraits.isiterabletable(::ColumnSource2) = true

IteratorInterfaceExtensions.getiterator(::ColumnSource2) =
    Tables.rows((a=[1,2,3], b=[4.,5.,6.], c=["A", "B", "C"]))

let x=ColumnSource2()
    @test Tables.source(Tables.columns(x)) == (a=[1,2,3], b=[4.,5.,6.], c=["A", "B", "C"])
end

@testset "operations.jl" begin
ctable = (A=[1, missing, 3], B=[1.0, 2.0, 3.0], C=["hey", "there", "sailor"])
rtable = Tables.rowtable(ctable)

## Tables.transform
tran = ctable |> Tables.transform(C=Symbol)
@test Tables.istable(typeof(tran))
@test !Tables.rowaccess(typeof(tran))
@test Tables.columnaccess(typeof(tran))
@test Tables.columns(tran) === tran
@test IteratorInterfaceExtensions.isiterable(tran)
@test typeof(IteratorInterfaceExtensions.getiterator(tran)) <: Tables.DataValueRowIterator

tran2 = rtable |> Tables.transform(C=Symbol)
@test Tables.istable(typeof(tran2))
@test Tables.rowaccess(typeof(tran2))
@test !Tables.columnaccess(typeof(tran2))
@test Tables.rows(tran2) === tran2
@test Base.IteratorSize(typeof(tran2)) == Base.HasShape{1}()
@test length(tran2) == 3
@test eltype(tran2) == Tables.TransformsRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},NamedTuple{(:C,),Tuple{DataType}}}
trow = first(tran2)
@test trow.A === 1
@test trow.B === 1.0
@test trow.C == :hey
ctable2 = Tables.columntable(tran2)
@test isequal(ctable2.A, ctable.A)
@test ctable2.C == map(Symbol, ctable.C)

# test various ways of inputting Tables.transform functions
table = Tables.transform(ctable, Dict{String, Base.Callable}("C" => Symbol)) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

table = ctable |> Tables.transform(C=Symbol) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

table = Tables.transform(ctable, Dict{Symbol, Base.Callable}(:C => Symbol)) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

table = Tables.transform(ctable, Dict{Int, Base.Callable}(3 => Symbol)) |> Tables.columntable
@test table.C == [:hey, :there, :sailor]

# test simple Tables.transforms + return types
table = ctable |> Tables.transform(Dict("A"=>x->x+1)) |> Tables.columntable
@test isequal(table.A, [2, missing, 4])
@test typeof(table.A) == Vector{Union{Missing, Int}}

table = ctable |> Tables.transform(Dict("A"=>x->coalesce(x+1, 0))) |> Tables.columntable
@test table.A == [2, 0, 4]

table = ctable |> Tables.transform(Dict("A"=>x->coalesce(x+1, 0.0))) |> Tables.columntable
@test table.A == [2, 0.0, 4]

table = ctable |> Tables.transform(Dict(2=>x->x==2.0 ? missing : x)) |> Tables.columntable
@test isequal(table.B, [1.0, missing, 3.0])
@test typeof(table.B) == Vector{Union{Float64, Missing}}

# test row sinks
# test various ways of inputting Tables.transform functions
table = Tables.transform(ctable, Dict{String, Base.Callable}("C" => Symbol)) |> Tables.rowtable
@test table[1].C == :hey

table = ctable |> Tables.transform(C=Symbol) |> Tables.rowtable
@test table[1].C == :hey

table = Tables.transform(ctable, Dict{Symbol, Base.Callable}(:C => Symbol)) |> Tables.rowtable
@test table[1].C == :hey

table = Tables.transform(ctable, Dict{Int, Base.Callable}(3 => Symbol)) |> Tables.rowtable
@test table[1].C == :hey

# test simple transforms + return types
table = ctable |> Tables.transform(Dict("A"=>x->x+1)) |> Tables.rowtable
@test isequal(map(x->x.A, table), [2, missing, 4])
@test typeof(map(x->x.A, table)) == Vector{Union{Missing, Int}}

table = ctable |> Tables.transform(Dict("A"=>x->coalesce(x+1, 0))) |> Tables.rowtable
@test map(x->x.A, table) == [2, 0, 4]

table = ctable |> Tables.transform(Dict("A"=>x->coalesce(x+1, 0.0))) |> Tables.rowtable
@test map(x->x.A, table) == [2, 0.0, 4]

table = ctable |> Tables.transform(Dict(2=>x->x==2.0 ? missing : x)) |> Tables.rowtable
@test isequal(map(x->x.B, table), [1.0, missing, 3.0])
@test typeof(map(x->x.B, table)) == Vector{Union{Float64, Missing}}

## Tables.select

# 117
sel = Tables.select(ctable)
@test Tables.istable(typeof(sel))
@test Tables.schema(sel) == Tables.Schema((), ())
@test Tables.columnaccess(typeof(sel))
@test Tables.columns(sel) === sel
@test propertynames(sel) == ()
@test Tables.columntable(sel) == NamedTuple()
@test Tables.rowtable(sel) == NamedTuple{(), Tuple{}}[]

sel = ctable |> Tables.select(:A)
@test Tables.istable(typeof(sel))
@test IteratorInterfaceExtensions.isiterable(sel)
@test IteratorInterfaceExtensions.getiterator(sel) == Tables.datavaluerows(sel)
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Tables.columnaccess(typeof(sel))
@test Tables.columns(sel) === sel
@test propertynames(sel) == (:A,)

sel = ctable |> Tables.select(1)
@test Tables.istable(typeof(sel))
@test IteratorInterfaceExtensions.isiterable(sel)
@test IteratorInterfaceExtensions.getiterator(sel) == Tables.datavaluerows(sel)
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Tables.columnaccess(typeof(sel))
@test Tables.columns(sel) === sel
@test propertynames(sel) == (:A,)

sel = Tables.select(rtable)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((), ())
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == Tables.SelectRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},()}
@test Tables.columntable(sel) == NamedTuple()
@test Tables.rowtable(sel) == [NamedTuple(), NamedTuple(), NamedTuple()]
srow = first(sel)
@test propertynames(srow) == ()

sel = rtable |> Tables.select(:A)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == Tables.SelectRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},(:A,)}
@test isequal(Tables.columntable(sel), (A = [1, missing, 3],))
@test isequal(Tables.rowtable(sel), [(A=1,), (A=missing,), (A=3,)])
srow = first(sel)
@test propertynames(srow) == (:A,)

sel = rtable |> Tables.select(1)
@test Tables.rowaccess(typeof(sel))
@test Tables.rows(sel) === sel
@test Tables.schema(sel) == Tables.Schema((:A,), (Union{Int, Missing},))
@test Base.IteratorSize(typeof(sel)) == Base.HasShape{1}()
@test length(sel) == 3
@test Base.IteratorEltype(typeof(sel)) == Base.HasEltype()
@test eltype(sel) == Tables.SelectRow{NamedTuple{(:A, :B, :C),Tuple{Union{Missing, Int},Float64,String}},(1,)}
@test isequal(Tables.columntable(sel), (A = [1, missing, 3],))
@test isequal(Tables.rowtable(sel), [(A=1,), (A=missing,), (A=3,)])
srow = first(sel)
@test propertynames(srow) == (:A,)

table = ctable |> Tables.select(:A) |> Tables.columntable
@test length(table) == 1
@test isequal(table.A, [1, missing, 3])

table = ctable |> Tables.select(1) |> Tables.columntable
@test length(table) == 1
@test isequal(table.A, [1, missing, 3])

table = ctable |> Tables.select("A") |> Tables.columntable
@test length(table) == 1
@test isequal(table.A, [1, missing, 3])

# column re-ordering
table = ctable |> Tables.select(:A, :C) |> Tables.columntable
@test length(table) == 2
@test isequal(table.A, [1, missing, 3])
@test isequal(table[2], ["hey", "there", "sailor"])

table = ctable |> Tables.select(1, 3) |> Tables.columntable
@test length(table) == 2
@test isequal(table.A, [1, missing, 3])
@test isequal(table[2], ["hey", "there", "sailor"])

table = ctable |> Tables.select(:C, :A) |> Tables.columntable
@test isequal(ctable.A, table.A)
@test isequal(ctable[1], table[2])

table = ctable |> Tables.select(3, 1) |> Tables.columntable
@test isequal(ctable.A, table.A)
@test isequal(ctable[1], table[2])

# row sink
table = ctable |> Tables.select(:A) |> Tables.rowtable
@test length(table[1]) == 1
@test isequal(map(x->x.A, table), [1, missing, 3])

table = ctable |> Tables.select(1) |> Tables.rowtable
@test length(table[1]) == 1
@test isequal(map(x->x.A, table), [1, missing, 3])

table = ctable |> Tables.select("A") |> Tables.rowtable
@test length(table[1]) == 1
@test isequal(map(x->x.A, table), [1, missing, 3])

# column re-ordering
table = ctable |> Tables.select(:A, :C) |> Tables.rowtable
@test length(table[1]) == 2
@test isequal(map(x->x.A, table), [1, missing, 3])
@test isequal(map(x->x[2], table), ["hey", "there", "sailor"])

table = ctable |> Tables.select(1, 3) |> Tables.rowtable
@test length(table[1]) == 2
@test isequal(map(x->x.A, table), [1, missing, 3])
@test isequal(map(x->x[2], table), ["hey", "there", "sailor"])

table = ctable |> Tables.select(:C, :A) |> Tables.rowtable
@test isequal(ctable.A, map(x->x.A, table))
@test isequal(ctable[1], map(x->x[2], table))

table = ctable |> Tables.select(3, 1) |> Tables.rowtable
@test isequal(ctable.A, map(x->x.A, table))
@test isequal(ctable[1], map(x->x[2], table))

end

@testset "TableTraits integration" begin
    rt = (a = Real[1, 2.0, 3], b = Union{Missing, Float64}[4.0, missing, 6.0], c = ["7", "8", "9"])

    dv = Tables.datavaluerows(rt)
    @test Base.IteratorSize(typeof(dv)) == Base.HasLength()
    @test eltype(dv) == NamedTuple{(:a, :b, :c),Tuple{Real,DataValue{Float64},String}}
    @test_throws MethodError size(dv)
    rt2 = collect(dv)
    @test rt2[1] == (a = 1, b = DataValue{Float64}(4.0), c = "7")

    ei = Tables.nondatavaluerows(QueryOperators.EnumerableIterable{eltype(dv), typeof(dv)}(dv))
    @test Tables.istable(typeof(ei))
    @test Tables.rowaccess(typeof(ei))
    @test Tables.rows(ei) === ei
    @test Base.IteratorEltype(typeof(ei)) == Base.HasEltype()
    @test Base.IteratorSize(typeof(ei)) == Base.HasLength()
    @test eltype(ei) == Tables.IteratorRow{NamedTuple{(:a, :b, :c),Tuple{Real,DataValue{Float64},String}}}
    @test eltype(typeof(ei)) == Tables.IteratorRow{NamedTuple{(:a, :b, :c),Tuple{Real,DataValue{Float64},String}}}
    @test_throws MethodError size(ei)
    nt = ei |> columntable
    @test isequal(rt, nt)
    rt3 = ei |> rowtable
    @test isequal(rt |> rowtable, rt3)

    # rt = [(a=1, b=4.0, c="7"), (a=2, b=5.0, c="8"), (a=3, b=6.0, c="9")]
    mt = Tables.nondatavaluerows(ei.x |> y->QueryOperators.map(y, x->(a=x.b, c=x.c), Expr(:block)))
    @inferred (mt |> columntable)
    @inferred (mt |> rowtable)

    # uninferrable case
    mt = Tables.nondatavaluerows(ei.x |> y->QueryOperators.map(y, x->(a=x.a, c=x.c), Expr(:block)))
    @test (mt |> columntable) == (a = Real[1, 2.0, 3], c = ["7", "8", "9"])
    @test length(mt |> rowtable) == 3

    rt = (a = Missing[missing, missing], b=[1,2])
    dv = Tables.datavaluerows(rt)
    @test eltype(dv) == NamedTuple{(:a, :b), Tuple{DataValue{Union{}}, Int}}

    # DataValue{Any}
    @test isequal(Tables.columntable(Tables.nondatavaluerows([(a=DataValue{Any}(), b=DataValue{Int}())])), (a = Any[missing], b = Union{Missing, Int64}[missing]))
end
