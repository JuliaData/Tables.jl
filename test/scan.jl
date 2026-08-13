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
        @test isempty(T.Scan())
        @test !isempty(T.Scan(limit = 5))
        @test !isempty(T.Scan(offset = 1))
        @test_throws ArgumentError T.Scan(select = (:a => 1.5,))
        @test_throws ArgumentError T.Scan(select = (T.Not(:a), :b))   # mixed Not/positive
        @test_throws ArgumentError T.Scan(limit = -1)
        @test_throws ArgumentError T.Scan(offset = -1)
        @test_throws ArgumentError T.Scan(filter = T.col(:a))         # bare column
        @test_throws ArgumentError T.Scan(filter = (x -> true))       # closures rejected
    end

    @testset "expression algebra" begin
        c = T.col(:a)
        @test (c > 1) isa T.Cmp{Int}
        @test (1 > c).op == T.OP_LT                                   # reversed comparison flips
        @test (c != 1) isa T.NotExpr
        @test !(c != 1) isa T.Cmp{Int}                                # double negation unwraps
        @test !T.isnull(c) == T.IsNull(T.Col(:a), true)
        @test_throws ArgumentError T.col(:a) == T.col(:b)             # col-to-col
        e = (c > 1) & (c < 5) & T.in_(T.col(:b), ("x", "w"))
        @test e isa T.AndExpr && length(e.args) == 3                  # flattened
        o = (c > 1) | (c < 0) | T.isnull(c)
        @test o isa T.OrExpr && length(o.args) == 3
        @test T.in(T.col(:b), ["x"]) isa T.In                         # Base.in sugar
        @test startswith(T.col(:b), "z") isa T.StrPred
    end

    @testset "bind" begin
        names = [:a, :b, :c, :x_one, :x_two]
        b = T.bind(T.Scan(), names)
        @test [c.index for c in b.columns] == 1:5
        b = T.bind(T.Scan(select = (r"^x_", :a => :first)), names)
        @test [(c.index, c.name) for c in b.columns] == [(4, :x_one), (5, :x_two), (1, :first)]
        b = T.bind(T.Scan(select = (:a => :first, T.All())), names)
        @test [(c.index, c.name) for c in b.columns] ==
              [(1, :first), (1, :a), (2, :b), (3, :c), (4, :x_one), (5, :x_two)]
        @test_throws ArgumentError T.bind(T.Scan(select = (:a, T.All())), names)
        b = T.bind(T.Scan(select = T.Not((r"^x_", :b))), names)
        @test [c.index for c in b.columns] == [1, 3]
        b = T.bind(T.Scan(filter = (T.col(:a) > 1) & T.isnull(T.col(:c))), names)
        @test sort(b.filtercols) == [1, 3]
        @test_throws ArgumentError T.bind(T.Scan(select = :nope), names)
        @test_throws ArgumentError T.bind(T.Scan(select = 6), names)
        @test_throws ArgumentError T.bind(T.Scan(select = r"^nope"), names)
        @test_throws ArgumentError T.bind(T.Scan(select = T.Not(r"^nope")), names)
        @test_throws ArgumentError T.bind(T.Scan(select = (:a, :b => :a)), names)   # dup output
        @test_throws ArgumentError T.bind(T.Scan(select = r"^x_" => :same), names)  # multi rename
        @test_throws ArgumentError T.bind(T.Scan(filter = T.col(:nope) > 1), names)
        @test_throws ArgumentError T.bind(T.Scan(filter = T.OpNode(:custom, Any[])), names)
        # validate=false silently drops unmatched, keeps the rest
        b = T.bind(T.Scan(select = (:nope, :a), validate = false), names)
        @test [c.index for c in b.columns] == [1]
        b = T.bind(T.Scan(select = T.Not((r"^nope", :b)), validate = false), names)
        @test [c.index for c in b.columns] == [1, 3, 4, 5]
        b = T.bind(T.Scan(filter = T.col(:nope) > 1, validate = false), names)
        @test isempty(b.filtercols)
    end

    @testset "finish: filter semantics (SQL missing), limit/offset, projection" begin
        out = T.finish(nt, T.Scan(filter = T.col(:a) > 1))
        @test out.a == [2, 3, 4]                                      # missing row EXCLUDED
        out = T.finish(nt, T.Scan(filter = T.isnull(T.col(:a))))
        @test length(out.a) == 1 && ismissing(out.a[1])
        out = T.finish(nt, T.Scan(filter = !T.isnull(T.col(:a))))
        @test out.a == [1, 2, 3, 4]
        out = T.finish(nt, T.Scan(filter = (T.col(:a) != 1)))
        @test out.a == [2, 3, 4]                                      # != never matches missing
        out = T.finish(nt, T.Scan(filter = T.in_(T.col(:b), ("x", "w"))))
        @test out.b == ["x", "x", "w"]
        out = T.finish(nt, T.Scan(filter = startswith(T.col(:b), "z") | endswith(T.col(:b), "y")))
        @test out.b == ["yy", "zzz"]
        out = T.finish(nt, T.Scan(limit = 2, offset = 1))
        @test isequal(out.a, [2, 3])
        out = T.finish(nt, T.Scan(filter = T.col(:c) > 2.0, limit = 2))
        @test out.c == [2.5, 3.5]
        out = T.finish(nt, T.Scan(offset = 10))
        @test isempty(out.a)
        # projection order, rename, type override
        out = T.finish(nt, T.Scan(select = (:c => Float32 => :cf, 1)))
        @test keys(out) == (:cf, :a)
        @test out.cf isa Vector{Float32}
        # type override preserves missing
        out = T.finish(nt, T.Scan(select = (:a => Float64,)))
        @test isequal(out.a, [1.0, 2.0, 3.0, 4.0, missing])
        @test eltype(out.a) == Union{Float64, Missing}
        # empty residual = identity
        @test T.finish(nt, T.Scan()) === nt
    end

    @testset "apply/read protocol" begin
        t, residual = T.apply(nt, T.Scan(limit = 2))
        @test t === nt && residual.limit == 2                          # fallback pushes nothing
        s = T.Scan(select = (:b, :a), filter = T.col(:a) >= 2, limit = 1)
        @test isequal(T.read(nt, s), T.finish(nt, s))                  # protocol equivalence
        # works through any Tables.jl source, e.g. a row iterator
        rows = Tables.rowtable(nt)
        out = T.read(rows, s)
        @test out.b == ["yy"] && out.a == [2]
        @test T.filtermask(s, nt) == [false, true, true, true, false]
    end

    @testset "display" begin
        s = T.Scan(select = (:a => Int64 => :z, T.All()), filter = !T.isnull(T.col(:a)), limit = 3)
        str = sprint(show, s)
        @test occursin("select =", str) && occursin("limit = 3", str)
        @test occursin("isnull", sprint(show, T.Scan(filter = T.isnull(T.col(:x)))))
        io = IOBuffer()
        T.describe(io, s, T.EMPTYSCAN)
        @test occursin("fully pushed down", String(take!(io)))
    end

end
