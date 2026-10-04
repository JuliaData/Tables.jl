using Tables

function checknumbers(values::Vector, threshold::Int)::Cint
    table = (x=values,)
    greater = Tables.filtermask(Tables.col(:x) > threshold, table)
    equal = Tables.filtermask(Tables.colcmp(==, Tables.col("x"), threshold), table)
    absent = Tables.filtermask(Tables.isnull(Tables.col(1)), table)
    present = Tables.filtermask(!Tables.isnull(Tables.col(:x)), table)
    member = Tables.filtermask(Tables.colin(Tables.col(:x), (threshold, 4)), table)
    unequal = Tables.filtermask(Tables.colcmp(!=, Tables.col(:x), threshold), table)
    less = Tables.filtermask(Tables.col(:x) < threshold, table)
    atmost = Tables.filtermask(Tables.col(:x) <= threshold, table)
    atleast = Tables.filtermask(Tables.col(:x) >= threshold, table)
    for i in eachindex(values)
        greater[i] == ((values[i] > threshold) === true) || return 1
        equal[i] == ((values[i] == threshold) === true) || return 2
        absent[i] == ismissing(values[i]) || return 3
        present[i] == !ismissing(values[i]) || return 4
        member[i] == (in(values[i], (threshold, 4)) === true) || return 5
        unequal[i] == ((values[i] != threshold) === true) || return 6
        less[i] == ((values[i] < threshold) === true) || return 7
        atmost[i] == ((values[i] <= threshold) === true) || return 8
        atleast[i] == ((values[i] >= threshold) === true) || return 9
    end
    return 0
end

function checkstrings(values::Vector, threshold::String)::Cint
    strings = (x=values,)
    prefix = Tables.filtermask(startswith(Tables.col(:x), "al"), strings)
    suffix = Tables.filtermask(endswith(Tables.col(:x), "ta"), strings)
    substring = Tables.filtermask(contains(Tables.col(:x), "ha"), strings)
    equal = Tables.filtermask(Tables.colcmp(==, Tables.col(:x), threshold), strings)
    unequal = Tables.filtermask(Tables.colcmp(!=, Tables.col(:x), threshold), strings)
    less = Tables.filtermask(Tables.col(:x) < threshold, strings)
    atmost = Tables.filtermask(Tables.col(:x) <= threshold, strings)
    greater = Tables.filtermask(Tables.col(:x) > threshold, strings)
    atleast = Tables.filtermask(Tables.col(:x) >= threshold, strings)
    for i in eachindex(values)
        value = values[i]
        if ismissing(value)
            !prefix[i] && !suffix[i] && !substring[i] || return 10
        else
            prefix[i] == startswith(value, "al") || return 11
            suffix[i] == endswith(value, "ta") || return 12
            substring[i] == contains(value, "ha") || return 13
        end
        equal[i] == ((value == threshold) === true) || return 14
        unequal[i] == ((value != threshold) === true) || return 15
        less[i] == ((value < threshold) === true) || return 16
        atmost[i] == ((value <= threshold) === true) || return 17
        greater[i] == ((value > threshold) === true) || return 18
        atleast[i] == ((value >= threshold) === true) || return 19
    end
    return 0
end

function @main(args::Vector{String})::Cint
    threshold = isempty(args) ? 2 : parse(Int, args[1])
    stringthreshold = length(args) > 1 ? args[2] : "beta"
    for values in (Union{Missing, Int}[1, missing, 3, 4], Int[1, 3, 4])
        result = checknumbers(values, threshold)
        result == 0 || return result
    end
    for values in (Union{Missing, String}["alpha", missing, "beta", "alphabet"],
                   String["alpha", "beta", "alphabet"])
        result = checkstrings(values, stringthreshold)
        result == 0 || return result
    end
    return 0
end

Base.Experimental.entrypoint(main, (Vector{String},))
