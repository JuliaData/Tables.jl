using Documenter, Tables

makedocs(;
    modules=[Tables],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/JuliaData/Tables.jl/blob/{commit}{path}#L{line}",
    sitename="Tables.jl",
    authors="Jacob Quinn",
    assets=String[],
)

deploydocs(;
    repo="github.com/JuliaData/Tables.jl",
    devbranch = "main"
)
