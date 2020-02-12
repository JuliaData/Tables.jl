# Run in .julia/registries directory

# find each package w/ a direct dependency on Tables.jl
pkgs = dirname.(readlines(`grep -rl ^Tables General/. --include=Deps.toml`))

pkgnames = [splitpath(x)[end] for x in pkgs]

function parseurl(file)
    repo = readlines(file)[end]
    return strip(split(repo, " = ")[end], '"')
end

urls = [parseurl(joinpath("General", string(first(nm)), nm, "Package.toml")) for nm in pkgnames]

open("/Users/jacobquinn/.julia/dev/Tables/INTEGRATIONS.md", "w+") do io
    println(io, "Packages currently integrating with Tables.jl:")
    for (nm, url) in zip(pkgnames, urls)
        println(io, "* [$nm]($url)")
    end
end