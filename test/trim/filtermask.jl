using Tables

function @main(args::Vector{String})::Cint
    threshold = isempty(args) ? 2 : parse(Int, args[1])
    values = Union{Missing, Int}[1, missing, 3, 4]
    table = (x=values,)
    greater = Tables.filtermask(Tables.col(:x) > threshold, table)
    equal = Tables.filtermask(Tables.colcmp(==, Tables.col("x"), threshold), table)
    absent = Tables.filtermask(Tables.isnull(Tables.col(1)), table)
    present = Tables.filtermask(!Tables.isnull(Tables.col(:x)), table)
    member = Tables.filtermask(Tables.colin(Tables.col(:x), (threshold, 4)), table)
    for i in eachindex(values)
        greater[i] == ((values[i] > threshold) === true) || return 1
        equal[i] == ((values[i] == threshold) === true) || return 2
        absent[i] == ismissing(values[i]) || return 3
        present[i] == !ismissing(values[i]) || return 4
        member[i] == (in(values[i], (threshold, 4)) === true) || return 5
    end
    strings = (x=Union{Missing, String}["alpha", missing, "beta", "alphabet"],)
    prefix = Tables.filtermask(startswith(Tables.col(:x), "al"), strings)
    suffix = Tables.filtermask(endswith(Tables.col(:x), "ta"), strings)
    substring = Tables.filtermask(contains(Tables.col(:x), "ha"), strings)
    prefix == [true, false, false, true] || return 6
    suffix == [false, false, true, false] || return 7
    substring == [true, false, false, true] || return 8
    return 0
end

Base.Experimental.entrypoint(main, (Vector{String},))
