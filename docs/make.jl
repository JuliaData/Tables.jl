using Documenter, Tables

makedocs(;
    modules=[Tables],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
        "Using the Interface" => "using-the-interface.md",
        "Implementing the Interface" => "implementing-the-interface.md",
    ],
    repo="https://github.com/JuliaData/Tables.jl/blob/{commit}{path}#L{line}",
    sitename="Tables.jl",
    authors="Jacob Quinn",
    checkdocs=:none,
)

deploydocs(;
    repo="github.com/JuliaData/Tables.jl",
    devbranch = "main"
)
