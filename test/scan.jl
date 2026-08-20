struct IndexOnlyColumn{T}
    data::Vector{T}
end
Base.length(c::IndexOnlyColumn) = length(c.data)
Base.getindex(c::IndexOnlyColumn, i::Int) = c.data[i]

struct IndexOnlyTable{T} <: Tables.AbstractColumns
    a::IndexOnlyColumn{T}
end
Tables.columnnames(::IndexOnlyTable) = (:a,)
Tables.getcolumn(t::IndexOnlyTable, ::Int) = getfield(t, :a)
Tables.getcolumn(t::IndexOnlyTable, ::Symbol) = getfield(t, :a)

struct ZeroColumnTable <: Tables.AbstractColumns
    nrows::Int
end
Tables.columnnames(::ZeroColumnTable) = ()
Tables.getcolumn(::ZeroColumnTable, i::Int) = throw(BoundsError((), i))
Tables.getcolumn(::ZeroColumnTable, name::Symbol) = throw(ArgumentError("unknown column $name"))
Tables.rowcount(t::ZeroColumnTable) = getfield(t, :nrows)

@testset "scan.jl" begin

    T = Tables
    nt = (a = [1, 2, 3, 4, missing],
          b = ["x", "yy", "zzz", "x", "w"],
          c = [1.5, 2.5, 3.5, 4.5, 5.5],
          x_one = [10, 20, 30, 40, 50],
          x_two = [-1, -2, -3, -4, -5])

    @testset "construction & lowering" begin
        s = T.Scan(select = (:a, "b" => :bee, :c => Float32, :a => Int64 => :a2, 4, r"^x_"))
        @test length(s.select) == 6
        @test s.select[1] == T.SelectItem(:a, nothing, nothing)
        @test s.select[2] == T.SelectItem("b", nothing, :bee)
        @test s.select[3] == T.SelectItem(:c, Float32, nothing)
        @test s.select[4] == T.SelectItem(:a, Int64, :a2)
        @test s.select[5] == T.SelectItem(4, nothing, nothing)
        @test s.select[6].ref isa Regex
        @test T.Scan(select = :a).select == [T.SelectItem(:a, nothing, nothing)]
        @test T._isidentity(T.Scan())
        @test !T._isidentity(T.Scan(limit = 5))
        @test !T._isidentity(T.Scan(offset = 1))
        @test_throws ArgumentError T.Scan(select = (:a => 1.5,))
        @test_throws ArgumentError T.Scan(select = (T.Not(:a), :b))   # mixed Not/positive
        @test_throws ArgumentError T.Scan(select = T.Not(1.5))
        @test_throws ArgumentError T.Scan(limit = -1)
        @test_throws ArgumentError T.Scan(offset = -1)
        @test_throws ArgumentError T.Scan(T.Scan(); limit = -1)
        @test_throws ArgumentError T.Scan(T.Scan(); offset = -1)
        @test_throws ArgumentError T.Scan(nothing, nothing, -1, 0, true)
        @test T.Scan(T.Scan(); limit = Int16(2)).limit == 2
        @test_throws ArgumentError T.Scan(filter = T.col(:a))         # bare column
        @test_throws ArgumentError T.Scan(filter = !T.col(:a))
        @test_throws ArgumentError T.Scan(filter = T.col(:a) & (T.col(:b) > 1))
        @test_throws ArgumentError T.Scan(filter = (x -> true))       # closures rejected
    end

    @testset "expression algebra" begin
        c = T.col(:a)
        @test (c > 1) isa T.Cmp{Int}
        @test (1 > c).op == T.OP_LT                                   # reversed comparison flips
        @test T.colcmp(==, c, 1) == T.Cmp(T.OP_EQ, T.Col(:a), 1)
        @test T.colcmp(!=, c, 1).op == T.OP_NE
        @test T.colcmp(<, c, 1).op == T.OP_LT
        @test T.colcmp(<=, c, 1).op == T.OP_LE
        @test T.colcmp(>, c, 1).op == T.OP_GT
        @test T.colcmp(>=, c, 1).op == T.OP_GE
        @test T.colcmp(==, c, :ready).rhs == :ready                    # general literal form
        @test T.colcmp(==, c, 1).op isa T.ComparisonOperator
        @test T.isnull(c) == T.IsNull(T.Col(:a), false)
        @test Base.ismissing(c) === false
        @test which(Base.ismissing, (typeof(c),)) === which(Base.ismissing, (Any,))
        @test Base.isequal(c, c) === true
        @test Base.isequal(c, 1) === false
        @test !T.isnull(c) == T.IsNull(T.Col(:a), true)
        @test c == T.col(:a)
        @test c != T.col(:b)
        @test_throws ArgumentError T.colcmp(==, T.col(:a), T.col(:b)) # col-to-col filter
        @test_throws ArgumentError T.colcmp(isless, c, 1)
        @test_throws ArgumentError T.Cmp(T.OP_EQ, T.col(:a), T.col(:b))
        @test_throws ArgumentError T.In(T.col(:a), T.col(:b))
        e = (c > 1) & (c < 5) & T.colin(T.col(:b), ("x", "w"))
        @test e isa T.AndExpr && length(e.args) == 3                  # flattened
        o = (c > 1) | (c < 0) | T.isnull(c)
        @test o isa T.OrExpr && length(o.args) == 3
        @test T.colin(T.col(:b), ["x"]) isa T.In
        @test startswith(T.col(:b), "z") isa T.StrPred
    end

    @testset "resolve" begin
        names = [:a, :b, :c, :x_one, :x_two]
        @test T.All === DataAPI.All
        b = T.resolve(T.Scan(), names)
        @test [c.index for c in b.columns] == 1:5
        b = T.resolve(T.Scan(select = (r"^x_", :a => :first)), names)
        @test [(c.index, c.name) for c in b.columns] == [(4, :x_one), (5, :x_two), (1, :first)]
        b = T.resolve(T.Scan(select = (:a => :first, T.All())), names)
        @test [(c.index, c.name) for c in b.columns] ==
              [(1, :first), (1, :a), (2, :b), (3, :c), (4, :x_one), (5, :x_two)]
        @test_throws ArgumentError T.resolve(T.Scan(select = (:a, T.All())), names)
        b = T.resolve(T.Scan(select = T.Not((r"^x_", :b))), names)
        @test [c.index for c in b.columns] == [1, 3]
        b = T.resolve(T.Scan(filter = (T.col(:a) > 1) & T.isnull(T.col(:c))), names)
        @test sort(b.filtercols) == [1, 3]
        positional = T.resolve(T.Scan(filter = T.col(3) > 1), names)
        @test positional.filter.lhs.ref === :c
        @test T.filtermask(positional, (c = [1, 2, 3],)) == [false, true, true]
        @test_throws ArgumentError T.resolve(T.Scan(select = :nope), names)
        @test_throws ArgumentError T.resolve(T.Scan(select = 6), names)
        @test_throws ArgumentError T.resolve(T.Scan(select = r"^nope"), names)
        @test_throws ArgumentError T.resolve(T.Scan(select = T.Not(r"^nope")), names)
        @test_throws ArgumentError T.resolve(T.Scan(select = (:a, :b => :a)), names)   # dup output
        @test_throws ArgumentError T.resolve(T.Scan(select = r"^x_" => :same), names)  # multi rename
        @test_throws ArgumentError T.resolve(T.Scan(filter = T.col(:nope) > 1), names)
        @test_throws ArgumentError T.resolve(
            T.Scan(filter = T.OpNode(:custom, Any[T.col(2), 1])), names,
        )
        # validate=false silently drops unmatched, keeps the rest
        b = T.resolve(T.Scan(select = (:nope, :a), validate = false), names)
        @test [c.index for c in b.columns] == [1]
        b = T.resolve(T.Scan(select = (9, :a), validate = false), names)
        @test [c.index for c in b.columns] == [1]
        b = T.resolve(T.Scan(select = T.Not((r"^nope", :b)), validate = false), names)
        @test [c.index for c in b.columns] == [1, 3, 4, 5]
        b = T.resolve(T.Scan(filter = T.col(:nope) > 1, validate = false), names)
        @test isempty(b.filtercols)
        @test T.filtermask(b, (a = [1, 2],)) == [false, false]
    end

    @testset "scan: filter semantics (SQL missing), limit/offset, projection" begin
        out = T.scan(nt, T.Scan(filter = T.col(:a) > 1))
        @test out.a == [2, 3, 4]                                      # missing row EXCLUDED
        out = T.scan(nt, T.Scan(filter = T.isnull(T.col(:a))))
        @test length(out.a) == 1 && ismissing(out.a[1])
        out = T.scan(nt, T.Scan(filter = !T.isnull(T.col(:a))))
        @test out.a == [1, 2, 3, 4]
        out = T.scan(nt, T.Scan(filter = T.colcmp(!=, T.col(:a), 1)))
        @test out.a == [2, 3, 4]                                      # != never matches missing
        out = T.scan(nt, T.Scan(filter = T.colin(T.col(:b), ("x", "w"))))
        @test out.b == ["x", "x", "w"]
        out = T.scan(nt, T.Scan(filter = startswith(T.col(:b), "z") | endswith(T.col(:b), "y")))
        @test out.b == ["yy", "zzz"]
        missingvals = (a = Union{Int, Missing}[1, missing, 3],
                       b = Union{String, Missing}["x", missing, "z"])
        @test T.filtermask(T.colin(T.col(:a), (1, missing)), missingvals) ==
              [true, false, false]
        @test T.filtermask(T.colin(T.col(:a), (2, missing)), missingvals) ==
              [false, false, false]
        @test T.filtermask(T.colin(T.col(:a), Set([missing])), missingvals) ==
              [false, false, false]
        @test T.filtermask(startswith(T.col(:b), "x"), missingvals) ==
              [true, false, false]
        @test T.filtermask(!T.colin(T.col(:a), (1, missing)), missingvals) ==
              [false, false, false]
        @test T.filtermask(T.Cmp(T.OP_NE, T.col(:a), 1), missingvals) ==
              [false, false, true]
        @test_throws ArgumentError T.filtermask(!T.col(:a), missingvals)
        @test T.filtermask(T.AndExpr(T.ScanExpr[]), missingvals) == [true, true, true]
        @test T.filtermask(T.OrExpr(T.ScanExpr[]), missingvals) == [false, false, false]
        # the final function barrier keeps only exact `true` (SQL WHERE)
        @test T._boolmask(BitVector([true, false])) == [true, false]
        @test T._boolmask(Bool[false, true]) == [false, true]
        @test T._boolmask(Union{Bool, Missing}[true, missing, false]) ==
              [true, false, false]
        @test T._boolmask(Any[true, 1, missing, nothing]) ==
              [true, false, false, false]
        @test_throws ArgumentError T.Cmp(0xff, T.col(:a), 1)
        @test_throws ArgumentError T.StrPred(0xff, T.col(:b), "x")
        out = T.scan(nt, T.Scan(limit = 2, offset = 1))
        @test isequal(out.a, [2, 3])
        out = T.scan(nt, T.Scan(filter = T.col(:c) > 2.0, limit = 2))
        @test out.c == [2.5, 3.5]
        out = T.scan(nt, T.Scan(offset = 10))
        @test isempty(out.a)
        out = T.scan(nt, T.Scan(offset = typemax(Int)))
        @test isempty(out.a)
        out = T.scan(nt, T.Scan(offset = 1, limit = typemax(Int)))
        @test isequal(out.a, [2, 3, 4, missing])
        # projection order, rename, type override
        out = T.scan(nt, T.Scan(select = (:c => Float32 => :cf, 1)))
        @test keys(out) == (:cf, :a)
        @test out.cf isa Vector{Float32}
        # type override preserves missing
        out = T.scan(nt, T.Scan(select = (:a => Float64,)))
        @test isequal(out.a, [1.0, 2.0, 3.0, 4.0, missing])
        @test eltype(out.a) == Union{Float64, Missing}
        converted = Union{Float64, Missing}[1.0, missing]
        @test T._converted(Float64, converted) === converted
        @test_throws InexactError T.scan((a = [1.5],), T.Scan(select = (:a => Int,)))
        indexonly = IndexOnlyTable(IndexOnlyColumn([10, 20, 30, 40]))
        @test T.scan(indexonly, T.Scan(select = :a, offset = 1, limit = 2)).a == [20, 30]
        @test T.filtermask(T.col(:a) > 20, indexonly) == [false, false, true, true]
        @test T.filtermask(T.colin(T.col(:a), (10, 40)), indexonly) ==
              [true, false, false, true]
        indexmissing = IndexOnlyTable(IndexOnlyColumn(Union{Int, Missing}[10, missing, 30]))
        @test T.filtermask(T.isnull(T.col(:a)), indexmissing) == [false, true, false]
        indexstrings = IndexOnlyTable(IndexOnlyColumn(["ab", "bc", "ax"]))
        @test T.filtermask(startswith(T.col(:a), "a"), indexstrings) == [true, false, true]
        # empty residual = identity
        @test T.scan(nt, T.Scan()) === nt
    end

    @testset "scan: validate=false filters treat unmatched refs as all-missing" begin
        nt2 = (a = [1, 2, 3], b = ["x", "y", "z"])
        # strict (default): unknown filter refs error, matching resolve
        @test_throws ArgumentError T.scan(nt2, T.Scan(filter = T.col(:gone) > 1))
        @test_throws ArgumentError T.filtermask(T.col(:gone) > 1, nt2)
        # lenient: comparisons/membership/strings against the absent column
        # evaluate to missing → rows excluded, SQL-style
        for f in (T.col(:gone) > 1, T.colin(T.col(:gone), (1, 2)),
                  startswith(T.col(:gone), "x"))
            out = T.scan(nt2, T.Scan(filter = f, validate = false))
            @test isempty(out.a)
        end
        # the absent column reads as missing: isnull keeps all, !isnull keeps none
        out = T.scan(nt2, T.Scan(filter = T.isnull(T.col(:gone)), validate = false))
        @test out.a == [1, 2, 3]
        out = T.scan(nt2, T.Scan(filter = !T.isnull(T.col(:gone)), validate = false))
        @test isempty(out.a)
        # three-valued composition with a matched predicate
        out = T.scan(nt2, T.Scan(filter = (T.col(:gone) > 1) | (T.col(:a) >= 3),
                                   validate = false))
        @test out.a == [3]
        out = T.scan(nt2, T.Scan(filter = (T.col(:gone) > 1) & (T.col(:a) >= 1),
                                   validate = false))
        @test isempty(out.a)
        # the Scan form of filtermask follows validate; resolve still omits the
        # unmatched ref from filtercols
        @test T.filtermask(T.Scan(filter = T.isnull(T.col(:gone)), validate = false),
                           nt2) == [true, true, true]
        b = T.resolve(T.Scan(filter = (T.col(:gone) > 1) & (T.col(:a) > 1),
                             validate = false), (:a, :b))
        @test b.filtercols == [1]
    end

    @testset "scan: collection-valued rows compare whole-value per row" begin
        lists = (l = [[1, 2], [3], [1, 2]], n = [10, 20, 30])
        # equality against a vector literal is per-row whole-value equality —
        # never an elementwise broadcast into the rows (which zips silently
        # when lengths happen to match and throws DimensionMismatch when not)
        out = T.scan(lists, T.Scan(filter = T.colcmp(==, T.col(:l), [1, 2])))
        @test out.n == [10, 30]
        out = T.scan(lists, T.Scan(filter = T.colcmp(!=, T.col(:l), [1, 2])))
        @test out.n == [20]
        @test T.filtermask(T.colin(T.col(:l), ([[3]], [[1, 2]])), lists) ==
              [false, false, false]
        @test T.filtermask(T.colin(T.col(:l), ([3],)), lists) == [false, true, false]
        # a 2-row column against a 2-element literal must still be whole-value
        two = (l = [[1, 2], [5, 6]], n = [1, 2])
        out = T.scan(two, T.Scan(filter = T.colcmp(==, T.col(:l), [5, 6])))
        @test out.n == [2]
        # missing rows keep SQL semantics through whole-value comparison
        lm = (l = Union{Missing, Vector{Int}}[[1], missing, [2]], n = [1, 2, 3])
        out = T.scan(lm, T.Scan(filter = T.colcmp(==, T.col(:l), [2])))
        @test out.n == [3]
    end

    @testset "generic executor + residual construction" begin
        s = T.Scan(select = (:b, :a), filter = T.col(:a) >= 2, limit = 1)
        out = T.scan(nt, s)
        @test out.b == ["yy"] && out.a == [2]
        # works through any Tables.jl source, e.g. a row iterator
        rows = Tables.rowtable(nt)
        out = T.scan(rows, s)
        @test out.b == ["yy"] && out.a == [2]
        @test T.filtermask(s, nt) == [false, true, true, true, false]
        # a source that pushed select/limit down hands the filter to Tables.scan
        residual = T.Scan(s; select = nothing, limit = nothing, offset = 0)
        @test residual.select === nothing && residual.limit === nothing && residual.filter === s.filter
        pushed = (b = nt.b, a = nt.a)                # what the source materialized (projection done)
        @test isequal(T.scan(pushed, residual).a, [2, 3, 4])   # filter applied generically
        @test T._isidentity(T.Scan(s; select = nothing, filter = nothing, limit = nothing))
        @test T.Scan(s; select = (:c,)).select == T.Scan(select = (:c,)).select
    end

    @testset "zero-column results preserve row counts" begin
        projected = T.scan(nt, T.Scan(select = ()))
        @test isempty(T.columnnames(projected))
        @test T.rowcount(projected) == 5
        @test DataAPI.nrow(projected) == 5
        @test length(T.rows(projected)) == 5

        filtered = T.scan(nt, T.Scan(select = (), filter = T.col(:a) > 1))
        @test T.rowcount(filtered) == 3

        source = ZeroColumnTable(1_000_000)
        windowed = T.scan(source, T.Scan(offset = 7, limit = 2))
        @test T.rowcount(windowed) == 2
        @test_throws ArgumentError T.scan(source, T.Scan(filter = T.isnull(T.col(:gone))))
        missingmatch = T.scan(source, T.Scan(
            filter = T.isnull(T.col(:gone)), validate = false, limit = 3,
        ))
        @test T.rowcount(missingmatch) == 3
        missingcmp = T.scan(source, T.Scan(
            filter = T.colcmp(==, T.col(:gone), 1), validate = false,
        ))
        @test T.rowcount(missingcmp) == 0
        overflowwindow = T.scan(source, T.Scan(offset = typemax(Int), limit = typemax(Int)))
        @test T.rowcount(overflowwindow) == 0

        T.scan(source, T.Scan(limit = 1))
        @test @allocated(T.scan(source, T.Scan(limit = 1))) < 100_000
    end

    @testset "display" begin
        s = T.Scan(select = (:a => Int64 => :z, T.All()), filter = !T.isnull(T.col(:a)), limit = 3)
        str = sprint(show, s)
        @test occursin("select =", str) && occursin("limit = 3", str)
        @test occursin("isnull", sprint(show, T.Scan(filter = T.isnull(T.col(:x)))))
        @test occursin("filter = true", sprint(show, T.Scan(filter = T.AndExpr(T.ScanExpr[]))))
        @test occursin("filter = false", sprint(show, T.Scan(filter = T.OrExpr(T.ScanExpr[]))))
        io = IOBuffer()
        T.describe(io, s, T.Scan())
        @test occursin("fully pushed down", String(take!(io)))
    end

end
