struct IndexOnlyColumn{T}
    data::Vector{T}
end
Base.length(c::IndexOnlyColumn) = length(c.data)
Base.getindex(c::IndexOnlyColumn, i::Int) = c.data[i]

struct CopyAwareColumn <: AbstractVector{Int}
    data::Vector{Int}
    copied::Base.RefValue{Bool}
end
Base.size(c::CopyAwareColumn) = size(c.data)
Base.getindex(c::CopyAwareColumn, i::Int) = c.data[i]
function Base.copyto!(dest::Vector{Float64}, src::CopyAwareColumn)
    src.copied[] = true
    @inbounds for i in eachindex(src)
        dest[i] = src[i]
    end
    return dest
end

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

struct IntegerScanComparison
    value::Int
end
Base.:(==)(x::IntegerScanComparison, ::Int) = x.value

struct EagerScanColumn{T, M} <: AbstractVector{T}
    values::Vector{T}
    mask::M
    calls::Base.RefValue{Int}
end
Base.size(c::EagerScanColumn) = size(c.values)
Base.getindex(c::EagerScanColumn, i::Int) = c.values[i]
function Base.Broadcast.broadcasted(f, c::EagerScanColumn)
    c.calls[] += 1
    return broadcast!(f, c.mask, c.values)
end

struct BooleanScanStyle <: Base.Broadcast.AbstractArrayStyle{1} end
struct BooleanScanColumn{T} <: AbstractVector{T}
    values::Vector{T}
end
Base.size(c::BooleanScanColumn) = size(c.values)
Base.getindex(c::BooleanScanColumn, i::Int) = c.values[i]
Base.BroadcastStyle(::Type{<:BooleanScanColumn}) = BooleanScanStyle()
function Base.copy(v::Base.Broadcast.Broadcasted{BooleanScanStyle})
    plain = Base.Broadcast.Broadcasted{Base.Broadcast.DefaultArrayStyle{1}}(v.f, v.args, v.axes)
    return BitVector(Base.copy(plain))
end

struct IntegerCopyScanColumn <: AbstractVector{Int}
    values::Vector{Int}
end
Base.size(c::IntegerCopyScanColumn) = size(c.values)
Base.getindex(c::IntegerCopyScanColumn, i::Int) = c.values[i]
function Base.copy(v::Base.Broadcast.Broadcasted{Base.Broadcast.DefaultArrayStyle{1}, A, F,
                   Tuple{IntegerCopyScanColumn}}) where {A, F}
    return Int[v.f(x) for x in v.args[1].values]
end

struct BooleanCopyScanValue
    value::Int
end
Base.:(==)(x::BooleanCopyScanValue, y::Int) = x.value == y
function Base.copy(v::Base.Broadcast.Broadcasted{Base.Broadcast.DefaultArrayStyle{1}, A, F,
                   Tuple{Vector{BooleanCopyScanValue}}}) where {A, F}
    return Int[v.f(x) for x in v.args[1]]
end

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
        @test T.Scan().select == [T.SelectItem(T.All(), nothing, nothing)]
        @test isempty(T.Scan(select = ()).select)
        @test_throws ArgumentError T.Scan(select = nothing)
        @test T._isidentity(T.Scan())
        @test T._isidentity(T.Scan(select = T.All()))
        @test !T._isidentity(T.Scan(limit = 5))
        @test !T._isidentity(T.Scan(offset = 1))
        @test_throws ArgumentError T.Scan(select = (:a => 1.5,))
        @test_throws ArgumentError T.Scan(select = (T.Not(:a), :b))   # mixed Not/positive
        @test_throws ArgumentError T.Scan(select = T.Not(1.5))
        @test_throws ArgumentError T.Scan(limit = -1)
        @test_throws ArgumentError T.Scan(offset = -1)
        @test_throws ArgumentError T.Scan(T.Scan(); limit = -1)
        @test_throws ArgumentError T.Scan(T.Scan(); offset = -1)
        @test_throws ArgumentError T.Scan(T.Scan().select, nothing, -1, 0, true)
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
        missingcols = T.columns(missingvals)
        @test isequal(
            Base.Broadcast.materialize(T._evalexpr(T.colcmp(==, T.col(:a), 1), missingcols)),
            Union{Bool, Missing}[true, missing, false],
        )
        @test isequal(
            Base.Broadcast.materialize(T._evalexpr(T.colin(T.col(:a), (1, 2)), missingcols)),
            Union{Bool, Missing}[true, missing, false],
        )
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
        copied = Ref(false)
        copyaware = CopyAwareColumn([1, 2, 3], copied)
        @test T._converted(Float64, copyaware) == [1.0, 2.0, 3.0]
        @test copied[]
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

    @testset "scan: logical masks preserve three-valued truth and ownership" begin
        combinations = vec(collect(Iterators.product((true, false, missing),
            (true, false, missing), (true, false, missing))))
        source = (a=[x[1] for x in combinations], b=[x[2] for x in combinations],
            c=[x[3] for x in combinations])
        a, b, c = (T.colcmp(==, T.col(name), true) for name in (:a, :b, :c))
        for op in (&, |), negate_a in (false, true), negate_b in (false, true)
            expr = op(negate_a ? !a : a, negate_b ? !b : b)
            expected = [op(negate_a ? !x : x, negate_b ? !y : y) for (x, y, _) in combinations]
            @test T.filtermask(expr, source) == [x === true for x in expected]
            @test T.filtermask(!expr, source) == [x === false for x in expected]
        end
        for (expr, expected) in (
            ((a | b) & c, [(x | y) & z for (x, y, z) in combinations]),
            (a | (b & c), [x | (y & z) for (x, y, z) in combinations]),
        )
            @test T.filtermask(expr, source) == [x === true for x in expected]
            @test T.filtermask(!expr, source) == [x === false for x in expected]
        end
        for expr in (T.AlwaysFalse() & (T.col(:gone) > 1),
                     T.AlwaysTrue() | (T.col(:gone) > 1))
            @test_throws ArgumentError T.filtermask(expr, source)
        end
        before = deepcopy(source)
        mask = T.filtermask((a | b) & !c, source)
        fill!(mask, true)
        @test isequal(source, before)
        @test T.filtermask((a | b) & !c, source) ==
            [((x | y) & !z) === true for (x, y, z) in combinations]

        # Custom comparison results still use elementwise &, | and exact true.
        custom = (a=IntegerScanComparison.([0, 1, 2]), b=[true, true, false])
        for op in (&, |)
            expr = op(T.colcmp(==, T.col(:a), 1), T.colcmp(==, T.col(:b), true))
            @test T.filtermask(expr, custom) ==
                [op(x.value, y) === true for (x, y) in zip(custom.a, custom.b)]
        end
        @test_throws MethodError T.filtermask(!T.colcmp(==, T.col(:a), 1), custom)

        # Eager custom broadcasts run once and may return shared storage.
        calls = Ref(0)
        eager = EagerScanColumn([0, 1, 2], falses(3), calls)
        expr = T.colcmp(==, T.col(:a), 1)
        mask = T.filtermask(expr, (a=eager,))
        @test mask == [false, true, false]
        @test calls[] == 1
        fill!(mask, true)
        @test eager.mask == [false, true, false]
        @test T.filtermask(expr, (a=eager,)) == [false, true, false]
        @test calls[] == 2
        # Result storage can convert Bool comparisons to integer zero/one.
        integers = EagerScanColumn([0, 1, 2], zeros(Int, 3), Ref(0))
        @test T.filtermask(expr | T.AlwaysTrue(), (a=integers,)) == falses(3)
        @test integers.calls[] == 1
        # A lazy custom style can convert integer comparisons to Bool on copy.
        styled = BooleanScanColumn(IntegerScanComparison.([0, 1, 0]))
        @test T.filtermask(expr, (a=styled,)) == [false, true, false]
        @test T.filtermask(!expr, (a=styled,)) == [true, false, true]

        # Default-style copy methods can also convert Bool values to Int,
        # including when the source is a Vector of user-defined values.
        for column in (IntegerCopyScanColumn([0, 1, 2]), BooleanCopyScanValue.([0, 1, 2]))
            table = (a=column,)
            for request in (expr, T.Scan(filter=expr), T.resolve(T.Scan(filter=expr), (:a,)))
                @test T.filtermask(request, table) == falses(3)
            end
            @test T.filtermask(expr | T.AlwaysTrue(), table) == falses(3)
            @test_throws MethodError T.filtermask(!expr, table)
        end

        n = 65536
        expr = (T.col(:a) > n ÷ 8) & (T.col(:a) < 7n ÷ 8) &
            (T.col(:b) >= n ÷ 4) & !T.isnull(T.col(:b))
        values = collect(1:n)
        for b in (values, Union{Missing,Int}[i % 7 == 0 ? missing : i for i in values])
            large = (a=values, b=b)
            T.filtermask(expr, large)
            allocated = @allocated T.filtermask(expr, large)
            if eltype(b) === Int
                @test allocated < n
            elseif VERSION >= v"1.13"
                # Older compilers can widen nullable broadcast output types,
                # requiring the conservative materialization path.
                @test allocated < 2n
            end
        end
    end

    @testset "scan: validate=false filters treat unmatched refs as all-missing" begin
        nt2 = (a = [1, 2, 3], b = ["x", "y", "z"])
        # strict (default): unknown filter refs error, matching resolve
        @test_throws ArgumentError T.scan(nt2, T.Scan(filter = T.col(:gone) > 1))
        @test_throws ArgumentError T.filtermask(T.col(:gone) > 1, nt2)
        for name in (Symbol("a b"), Symbol("a\nb"), Symbol("=foo\"bar\\baz"))
            err = try T.filtermask(T.col(name) > 0, nt2) catch caught; caught end
            @test err isa ArgumentError
            text = sprint(showerror, err)
            @test occursin(repr(String(name)), text)
            @test !occursin('\n', text)
        end
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
        residual = T.Scan(s; select = T.All(), limit = nothing, offset = 0)
        @test T._isallselection(residual.select) && residual.limit === nothing && residual.filter === s.filter
        pushed = (b = nt.b, a = nt.a)                # what the source materialized (projection done)
        @test isequal(T.scan(pushed, residual).a, [2, 3, 4])   # filter applied generically
        @test T._isidentity(T.Scan(s; select = T.All(), filter = nothing, limit = nothing))
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
        @test sprint(show, T.Scan()) == "Tables.Scan()"
        @test sprint(show, T.Scan(select = ())) == "Tables.Scan(select = ())"
        io = IOBuffer()
        T.describe(io, s, T.Scan())
        @test occursin("fully pushed down", String(take!(io)))
    end

    @testset "All arguments, Union{} override, residual composition" begin
        # DataAPI.All(cols...) with arguments must not silently select everything
        @test_throws ArgumentError T.Scan(select = T.All(:a))
        @test_throws ArgumentError T.Scan(select = (T.All(:a, :b),))

        # a Union{} column still honors the requested override type
        empty = T.scan((u = Union{}[],), T.Scan(select = (:u => Float64,)))
        @test T.columntable(empty).u isa Vector{Float64}

        # residual composition: stripping a consumed axis reproduces the
        # reference result, in execution order (filter, then row bounds,
        # then projection)
        source = (a = collect(1:6), b = collect(10.0:10.0:60.0))
        request = T.Scan(select = (:b => :value,), filter = T.col(:a) > 2,
                         limit = 2, offset = 1)
        reference = T.columntable(T.scan(source, request))
        @test reference == (value = [40.0, 50.0],)
        # source consumed the filter
        mask = T.filtermask(request, source)
        filtered = (a = source.a[mask], b = source.b[mask])
        residual = T.Scan(request; filter = nothing)
        @test T.columntable(T.scan(filtered, residual)) == reference
        # source consumed the filter and the row bounds
        bounded = (a = filtered.a[2:3], b = filtered.b[2:3])
        residual2 = T.Scan(request; filter = nothing, limit = nothing, offset = 0)
        @test T.columntable(T.scan(bounded, residual2)) == reference
    end

end
