####
#### automatically generate a list of integrations
####

###
### Usage
###
### 1. ensure a development version of Tables.jl (`pkg> dev Tables`) in `DEPOT_PATH[1]/dev/Tables`
### 2. make sure the General registry is up to date (`pkg> up`)
### 3. run this script, which uses the first depot from DEPOT_PATH

DEPOT = first(DEPOT_PATH)
REGISTRIES = joinpath(DEPOT, "registries")

# find each package w/ a direct dependency on Tables.jl
pkgs = cd(REGISTRIES) do
    dirname.(readlines(`grep -rl ^Tables General/. --include=Deps.toml`))
end

pkgnames = sort([splitpath(x)[end] for x in pkgs])

function parseurl(file)
    repo = readlines(file)[end]
    return strip(split(repo, " = ")[end], '"')
end

urls = [parseurl(joinpath(REGISTRIES, "General", string(first(nm)), nm, "Package.toml"))
        for nm in pkgnames]

open(joinpath(DEPOT, "dev", "Tables", "INTEGRATIONS.md"), "w+") do io
    println(io, "Packages currently integrating with Tables.jl:")
    for (nm, url) in zip(pkgnames, urls)
        println(io, "* [$nm]($url)")
    end
end
