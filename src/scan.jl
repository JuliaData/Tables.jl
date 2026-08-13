# Tables.Scan: a plain-data scan request — column selection/renaming/type
# overrides, row predicates, and limits — that any Tables.jl source can accept
# and push down. The protocol is two functions:
#
#     table, residual = Tables.apply(source, scan)   # source consumes what it can
#     table′          = Tables.finish(table, residual)  # generic layer does the rest
#
# with `Tables.read(source, scan)` composing the two. The default `apply`
# pushes nothing (everything lands in the residual), so every existing
# Tables.jl source already works — sources override `apply` to earn
# performance, never correctness.
#
# Design commitments (each learned the hard way by prior systems):
#   * Every node is a plain value — no `Function` fields anywhere. Closures are
#     opaque to pushdown and to static compilation; expression values are not.
#   * The source's answer is the residual itself, not a capability
#     advertisement. Pushed and residual work may overlap (a source that
#     prunes by a predicate but cannot guarantee exactness keeps the filter in
#     the residual).
#   * Missing follows SQL WHERE: a predicate evaluating to `missing` excludes
#     the row. `isnull`/`!isnull` are first-class nodes; `==` never matches
#     missing.
#   * Filters reference SOURCE column names (pre-rename); the pipeline order
#     is fixed: bind → filter → offset/limit → project/rename/type.
#   * Selection order defines output order.

# --- column references & selection items --------------------------------------

"""
    Tables.All()

Selection item matching every column (in file order). Useful combined with
renames. `select = (:id, Tables.All())` is an error because it produces the
output name `id` twice; renaming the explicit selection avoids that conflict.
"""
struct All end

"""
    Tables.Not(ref)

Selection item excluding `ref` (a name, index, `Regex`, or a `Tuple`/`Vector`
of those) from the full column set. `Not` items cannot be mixed with positive
selections in the same `select`.
"""
struct Not{T}
    ref::T
end

const ColRef = Union{Symbol, String, Int, Regex}

"""
    Tables.SelectItem

One resolved element of `Scan.select`: a column reference plus optional type
override and output name. Users never construct these directly — the `Scan`
constructor lowers `ref`, `ref => name`, `ref => Type`, and
`ref => Type => name` forms.
"""
struct SelectItem
    ref::Union{Symbol, String, Int, Regex, Not, All}
    type::Union{Nothing, Type}
    rename::Union{Nothing, Symbol}
end

_selectitem(x::Union{Symbol, String, Int, Regex, Not, All}) = SelectItem(x, nothing, nothing)
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, Symbol}) = SelectItem(p.first, nothing, p.second)
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, String}) = SelectItem(p.first, nothing, Symbol(p.second))
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, <:Type}) = SelectItem(p.first, p.second, nothing)
_selectitem(p::Pair{<:Union{Symbol, String, Int, Regex}, <:Pair{<:Type, Symbol}}) =
    SelectItem(p.first, p.second.first, p.second.second)
_selectitem(x) = throw(ArgumentError("unsupported select item $(repr(x)); expected a column " *
    "reference (Symbol/String/Int/Regex/Not/All), or ref => name, ref => Type, ref => Type => name"))

# --- the expression algebra ----------------------------------------------------
#
# A small closed vocabulary — the intersection every surveyed engine supports
# and every statistics pruner can use. Growth happens through `OpNode`
# (name + children), never through new struct kinds every consumer must learn.

abstract type ScanExpr end

"""
    Tables.col(ref)

A column reference inside a `Scan` filter: `col(:price) > 100`. Comparisons
against literals, `in_`, `isnull`, `startswith`/`endswith`/`contains`, and
`&`/`|`/`!` build plain expression values.
"""
struct Col <: ScanExpr
    ref::Union{Symbol, String, Int}
end
col(r::Union{Symbol, AbstractString, Int}) = Col(r isa AbstractString ? String(r) : r)

const OP_EQ, OP_NE, OP_LT, OP_LE, OP_GT, OP_GE = 0x01, 0x02, 0x03, 0x04, 0x05, 0x06
const _OPNAMES = ("==", "!=", "<", "<=", ">", ">=")

struct Cmp{T} <: ScanExpr
    op::UInt8
    lhs::Col
    rhs::T
end

struct In{T} <: ScanExpr
    lhs::Col
    values::T
end

"""
    Tables.in_(col, values)

Membership predicate: `in_(col(:status), ("active", "trial"))`.
"""
in_(c::Col, values) = In(c, values)

struct IsNull <: ScanExpr
    lhs::Col
    negated::Bool
end

"""
    Tables.isnull(col)

Missing-ness predicate. Use this — never `col(:x) == missing`, which follows
SQL semantics and matches no row.
"""
isnull(c::Col) = IsNull(c, false)

const STR_STARTSWITH, STR_ENDSWITH, STR_CONTAINS = 0x01, 0x02, 0x03

struct StrPred <: ScanExpr
    kind::UInt8
    lhs::Col
    s::String
end

struct AndExpr <: ScanExpr
    args::Vector{ScanExpr}
end
struct OrExpr <: ScanExpr
    args::Vector{ScanExpr}
end
struct NotExpr <: ScanExpr
    arg::ScanExpr
end
struct AlwaysTrue <: ScanExpr end
struct AlwaysFalse <: ScanExpr end

"""
    Tables.OpNode(name, args)

The expression algebra's growth channel: a named node with child values.
Sources that recognize `name` may push it down; the generic executor rejects
unknown names, so new operations deploy source-first without new node types.
"""
struct OpNode <: ScanExpr
    name::Symbol
    args::Vector{Any}
end

_colcolerr() = throw(ArgumentError("column-to-column comparisons are not supported; " *
                                   "compare a column against a literal value"))
for (f, op) in ((:(==), :OP_EQ), (:<, :OP_LT), (:<=, :OP_LE), (:>, :OP_GT), (:>=, :OP_GE))
    revop = f === :(==) ? :OP_EQ : f === :< ? :OP_GT : f === :<= ? :OP_GE :
            f === :> ? :OP_LT : :OP_LE
    @eval begin
        Base.$f(c::Col, v) = Cmp($op, c, v)
        Base.$f(v, c::Col) = Cmp($revop, c, v)
        Base.$f(::Col, ::Col) = _colcolerr()
    end
end
Base.:(!=)(c::Col, v) = NotExpr(Cmp(OP_EQ, c, v))
Base.:(!=)(v, c::Col) = NotExpr(Cmp(OP_EQ, c, v))
Base.:(!=)(::Col, ::Col) = _colcolerr()
Base.isequal(c::Col, v) = Cmp(OP_EQ, c, v)   # convenience alias; == semantics

Base.startswith(c::Col, s::AbstractString) = StrPred(STR_STARTSWITH, c, String(s))
Base.endswith(c::Col, s::AbstractString) = StrPred(STR_ENDSWITH, c, String(s))
Base.contains(c::Col, s::AbstractString) = StrPred(STR_CONTAINS, c, String(s))
Base.in(c::Col, values) = In(c, values)

_ands(e::AndExpr) = e.args
_ands(e::ScanExpr) = ScanExpr[e]
_ors(e::OrExpr) = e.args
_ors(e::ScanExpr) = ScanExpr[e]
Base.:&(a::ScanExpr, b::ScanExpr) = AndExpr(vcat(_ands(a), _ands(b)))
Base.:|(a::ScanExpr, b::ScanExpr) = OrExpr(vcat(_ors(a), _ors(b)))
Base.:!(e::ScanExpr) = NotExpr(e)
Base.:!(e::NotExpr) = e.arg
Base.:!(e::IsNull) = IsNull(e.lhs, !e.negated)

# a bare Col is not a predicate; catch the likely mistake early
_checkpredicate(::Nothing) = nothing
_checkpredicate(e::ScanExpr) = e
_checkpredicate(e::Col) =
    throw(ArgumentError("a bare column reference is not a predicate; compare it against a value"))
_checkpredicate(x) =
    throw(ArgumentError("filter must be a Tables.Scan expression (built from Tables.col), got $(typeof(x))"))

# --- the Scan value --------------------------------------------------------------

"""
    Tables.Scan(; select=nothing, filter=nothing, limit=nothing, offset=nothing, validate=true)

A scan request: what to keep, what to call it, how to type it, which rows
qualify, and how many. Plain data all the way down — see `Tables.apply`,
`Tables.finish`, and `Tables.read` for the protocol.

  * `select`: a column reference or tuple/vector of select items
    (`ref`, `ref => name`, `ref => Type`, `ref => Type => name`;
    refs are `Symbol`/`String`/`Int`/`Regex`/`Tables.Not`/`Tables.All`).
    `nothing` keeps every column. Selection order = output order.
  * `filter`: an expression built from `Tables.col`; a row is kept iff the
    predicate evaluates to exactly `true` (`missing` excludes, SQL-style).
    Filters see source column names, before renames.
  * `limit`/`offset`: applied to qualifying rows, after the filter.
  * `validate`: error on select/filter references that match no column
    (`false` silently drops unmatched names — the schema-evolution knob).
"""
struct Scan
    select::Union{Nothing, Vector{SelectItem}}
    filter::Union{Nothing, ScanExpr}
    limit::Union{Nothing, Int}
    offset::Int
    validate::Bool
end

function Scan(; select=nothing, filter=nothing, limit::Union{Nothing, Integer}=nothing,
                offset::Union{Nothing, Integer}=nothing, validate::Bool=true)
    items = select === nothing ? nothing :
            select isa Union{Tuple, AbstractVector} ? SelectItem[_selectitem(x) for x in select] :
            SelectItem[_selectitem(select)]
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    off = offset === nothing ? 0 : Int(offset)
    off >= 0 || throw(ArgumentError("offset must be ≥ 0 (got $offset)"))
    if items !== nothing
        nots = count(it -> it.ref isa Not, items)
        0 < nots < length(items) &&
            throw(ArgumentError("Not(...) selections cannot be mixed with positive selections"))
    end
    return Scan(items, _checkpredicate(filter), limit === nothing ? nothing : Int(limit), off, validate)
end

"""
    isempty(scan::Tables.Scan)

`true` when the scan requests nothing: no selection, no filter, no limit or
offset. `Tables.finish` returns the table unchanged for an empty residual.
"""
Base.isempty(s::Scan) =
    s.select === nothing && s.filter === nothing && s.limit === nothing && s.offset == 0

# an all-consumed residual, for sources that push everything down
const EMPTYSCAN = Scan(nothing, nothing, nothing, 0, true)

# --- binding ---------------------------------------------------------------------

"""
    Tables.BoundColumn

One output column after `Tables.bind`: the source column index, the output
name (post-rename, uniqueness enforced), and the optional type override.
"""
struct BoundColumn
    index::Int
    name::Symbol
    type::Union{Nothing, Type}
end

"""
    Tables.BoundScan

A `Scan` resolved against a concrete schema: output columns in order, the
(validated) filter with the source column indices it references, and the
row bounds. Sources consume this, not the raw `Scan`.
"""
struct BoundScan
    columns::Vector{BoundColumn}
    filter::Union{Nothing, ScanExpr}
    filtercols::Vector{Int}
    limit::Union{Nothing, Int}
    offset::Int
end

_findcol(names, r::Symbol) = findfirst(==(r), names)
_findcol(names, r::String) = findfirst(==(Symbol(r)), names)
_findcol(names, r::Int) = 1 <= r <= length(names) ? r : nothing
_refstr(r) = r isa Regex ? "r$(repr(r.pattern))" : repr(r)

function _expand!(out::Vector{BoundColumn}, names, it::SelectItem, validate::Bool)
    r = it.ref
    if r isa All
        append!(out, BoundColumn(i, nm, it.type) for (i, nm) in enumerate(names))
    elseif r isa Regex
        found = false
        for (i, nm) in enumerate(names)
            if occursin(r, String(nm))
                push!(out, BoundColumn(i, it.rename === nothing ? nm : it.rename, it.type))
                found = true
            end
        end
        validate && !found &&
            throw(ArgumentError("select pattern $(_refstr(r)) matches no column"))
        found && it.rename !== nothing && count(c -> c.name == it.rename, out) > 1 &&
            throw(ArgumentError("pattern rename $(repr(it.rename)) applies to multiple columns"))
    else
        i = _findcol(names, r)
        if i === nothing
            validate && throw(ArgumentError("select reference $(_refstr(r)) matches no column " *
                                            "(pass validate=false to skip unmatched references)"))
            return
        end
        push!(out, BoundColumn(i, it.rename === nothing ? names[i] : it.rename, it.type))
    end
    return
end

_notrefs(x::Union{Tuple, AbstractVector}) = collect(Any, x)
_notrefs(x) = Any[x]

function _filterrefs!(refs::Vector{Int}, e::ScanExpr, names, validate::Bool)
    if e isa Union{Cmp, In, IsNull, StrPred}
        i = _findcol(names, e.lhs.ref)
        i === nothing && validate &&
            throw(ArgumentError("filter references unknown column $(_refstr(e.lhs.ref))"))
        i === nothing || (i in refs || push!(refs, i))
    elseif e isa AndExpr || e isa OrExpr
        foreach(a -> _filterrefs!(refs, a, names, validate), e.args)
    elseif e isa NotExpr
        _filterrefs!(refs, e.arg, names, validate)
    elseif e isa OpNode
        throw(ArgumentError("cannot bind extension node $(repr(e.name)): no generic evaluation; " *
                            "only sources that recognize it may consume it"))
    end
    return refs
end

"""
    Tables.bind(scan::Scan, names) -> BoundScan

Resolve a `Scan` against a source's column names (any iterable of `Symbol`s).
Errors on unmatched references (under `validate`), mixed `Not`/positive
selections, and duplicate output names. Regex references expand in file
order; selection order defines output order.
"""
function bind(scan::Scan, names)
    nms = collect(Symbol, names)
    cols = BoundColumn[]
    if scan.select === nothing
        append!(cols, BoundColumn(i, nm, nothing) for (i, nm) in enumerate(nms))
    elseif !isempty(scan.select) && scan.select[1].ref isa Not
        excluded = Set{Int}()
        for it in scan.select, r in _notrefs((it.ref::Not).ref)
            if r isa Regex
                found = false
                for (i, nm) in enumerate(nms)
                    if occursin(r, String(nm))
                        push!(excluded, i)
                        found = true
                    end
                end
                scan.validate && !found &&
                    throw(ArgumentError("Not pattern $(_refstr(r)) matches no column"))
            else
                i = _findcol(nms, r)
                i === nothing && scan.validate &&
                    throw(ArgumentError("Not reference $(_refstr(r)) matches no column"))
                i === nothing || push!(excluded, i)
            end
        end
        append!(cols, BoundColumn(i, nm, nothing) for (i, nm) in enumerate(nms) if !(i in excluded))
    else
        foreach(it -> _expand!(cols, nms, it, scan.validate), scan.select)
    end
    seen = Set{Symbol}()
    for c in cols
        c.name in seen && throw(ArgumentError("duplicate output column name $(repr(c.name)); " *
                                              "rename one of the selections"))
        push!(seen, c.name)
    end
    refs = Int[]
    scan.filter === nothing || _filterrefs!(refs, scan.filter, nms, scan.validate)
    return BoundScan(cols, scan.filter, refs, scan.limit, scan.offset)
end

# --- generic evaluation ------------------------------------------------------------

_getcol(cols, ref::Symbol) = getcolumn(cols, ref)
_getcol(cols, ref::String) = getcolumn(cols, Symbol(ref))
_getcol(cols, ref::Int) = getcolumn(cols, ref)

# three-valued vectorized evaluation; the top level keeps rows where the
# result is exactly `true` (SQL WHERE)
function _evalexpr(e::ScanExpr, cols)
    if e isa Cmp
        a = _getcol(cols, e.lhs.ref)
        v = e.rhs
        return e.op == OP_EQ ? (a .== v) :
               e.op == OP_LT ? (a .< v) :
               e.op == OP_LE ? (a .<= v) :
               e.op == OP_GT ? (a .> v) : (a .>= v)
    elseif e isa In
        a = _getcol(cols, e.lhs.ref)
        return in.(a, Ref(e.values))
    elseif e isa IsNull
        a = _getcol(cols, e.lhs.ref)
        return e.negated ? .!ismissing.(a) : ismissing.(a)
    elseif e isa StrPred
        a = _getcol(cols, e.lhs.ref)
        f = e.kind == STR_STARTSWITH ? startswith :
            e.kind == STR_ENDSWITH ? endswith : contains
        return map(x -> ismissing(x) ? missing : f(x, e.s), a)
    elseif e isa AndExpr
        return mapreduce(a -> _evalexpr(a, cols), (x, y) -> x .& y, e.args)
    elseif e isa OrExpr
        return mapreduce(a -> _evalexpr(a, cols), (x, y) -> x .| y, e.args)
    elseif e isa NotExpr
        return .!_evalexpr(e.arg, cols)
    elseif e isa AlwaysTrue
        return trues(rowcount(cols))
    elseif e isa AlwaysFalse
        return falses(rowcount(cols))
    end
    throw(ArgumentError("cannot generically evaluate $(typeof(e))"))
end

"""
    Tables.filtermask(scan_or_expr, table) -> Vector{Bool}

Evaluate a scan's filter over a table's columns: `mask[i]` is `true` iff row
`i` qualifies (a `missing` predicate result excludes the row). Sources use
this for their own pushdown implementations.
"""
filtermask(e::ScanExpr, table) = Bool[x === true for x in _evalexpr(e, columns(table))]
filtermask(s::Scan, table) = s.filter === nothing ?
    trues(rowcount(columns(table))) : filtermask(s.filter, table)

_converted(::Nothing, c::AbstractVector) = c
function _converted(::Type{T}, c::AbstractVector) where {T}
    eltype(c) <: Union{T, Missing} && return c
    anymissing = any(ismissing, c)
    E = anymissing ? Union{T, Missing} : T
    return E[ismissing(x) ? missing : convert(T, x) for x in c]
end

"""
    Tables.finish(table, residual::Scan) -> table′

Apply whatever a source did NOT push down, generically, over any Tables.jl
table: filter (SQL missing semantics), offset/limit, projection, renames, and
type overrides (elementwise `convert` — a source that consumes `types` itself,
like a CSV parser, never leaves them in the residual). Returns the table
unchanged when the residual is empty; otherwise a `NamedTuple` of columns.
"""
function finish(table, scan::Scan)
    isempty(scan) && return table
    cols = columns(table)
    b = bind(scan, columnnames(cols))
    if scan.filter !== nothing
        keep = Bool[x === true for x in _evalexpr(scan.filter, cols)]
        idx = findall(keep)
    else
        idx = collect(1:rowcount(cols))
    end
    lo = scan.offset + 1
    hi = scan.limit === nothing ? length(idx) : min(length(idx), scan.offset + scan.limit)
    idx = idx[lo:max(hi, lo - 1)]
    outnames = Tuple(c.name for c in b.columns)
    outcols = Tuple(_converted(c.type, getcolumn(cols, c.index)[idx]) for c in b.columns)
    return NamedTuple{outnames}(outcols)
end

# --- the protocol -----------------------------------------------------------------

"""
    Tables.apply(source, scan::Scan) -> (table, residual::Scan)

Sources override this to push down whatever parts of `scan` they can, and
return the rest as a residual for `Tables.finish`. The fallback pushes
nothing, so every Tables.jl source works unmodified. Contract:

  * `Tables.finish(Tables.apply(src, scan)...)` must equal
    `Tables.finish(Tables.columns(src), scan)` (up to table type).
  * Pushed and residual work may overlap: a source that used a predicate to
    prune inexactly (chunks, row groups) keeps that predicate in the residual.
  * A source may consume `limit`/`offset` only if every filter it applied was
    exact — inexact filtering poisons limit pushdown.
  * Contradictions error (an unknown selected column under `validate`, a type
    override conflicting with a source-fixed schema); mere inability never
    errors — it lands in the residual.
"""
apply(source, scan::Scan) = (source, scan)

"""
    Tables.read(source, scan::Scan)

`Tables.finish(Tables.apply(source, scan)...)` — the one-call entry point.
"""
function read(source, scan::Scan)
    t, residual = apply(source, scan)
    return finish(t, residual)
end

# --- display -----------------------------------------------------------------------

function _exprstr(e::ScanExpr)
    e isa Cmp && return "col($(repr(e.lhs.ref))) $(_OPNAMES[e.op]) $(repr(e.rhs))"
    e isa In && return "in_(col($(repr(e.lhs.ref))), $(repr(e.values)))"
    e isa IsNull && return (e.negated ? "!isnull(" : "isnull(") * "col($(repr(e.lhs.ref))))"
    e isa StrPred && return (e.kind == STR_STARTSWITH ? "startswith" :
                             e.kind == STR_ENDSWITH ? "endswith" : "contains") *
                            "(col($(repr(e.lhs.ref))), $(repr(e.s)))"
    e isa AndExpr && return join(("(" * _exprstr(a) * ")" for a in e.args), " & ")
    e isa OrExpr && return join(("(" * _exprstr(a) * ")" for a in e.args), " | ")
    e isa NotExpr && return "!(" * _exprstr(e.arg) * ")"
    e isa AlwaysTrue && return "true"
    e isa AlwaysFalse && return "false"
    e isa OpNode && return "OpNode($(repr(e.name)), …)"
    return string(e)
end

function Base.show(io::IO, s::Scan)
    print(io, "Tables.Scan(")
    parts = String[]
    if s.select !== nothing
        sel = join((begin
            r = it.ref isa Not ? "Not($(_refstr(it.ref.ref)))" :
                it.ref isa All ? "All()" : _refstr(it.ref)
            it.type !== nothing && (r *= " => $(it.type)")
            it.rename !== nothing && (r *= " => $(repr(it.rename))")
            r
        end for it in s.select), ", ")
        push!(parts, "select = ($sel)")
    end
    s.filter === nothing || push!(parts, "filter = " * _exprstr(s.filter))
    s.limit === nothing || push!(parts, "limit = $(s.limit)")
    s.offset == 0 || push!(parts, "offset = $(s.offset)")
    s.validate || push!(parts, "validate = false")
    print(io, join(parts, ", "), ")")
end

"""
    Tables.describe([io,] scan, residual)

Print what a source pushed down versus what remains for the generic layer —
the EXPLAIN affordance for pushdown debugging.
"""
function describe(io::IO, scan::Scan, residual::Scan)
    println(io, "scan:     ", scan)
    println(io, "residual: ", isempty(residual) ? "(empty — fully pushed down)" : residual)
end
describe(scan::Scan, residual::Scan) = describe(stdout, scan, residual)
