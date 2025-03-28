using Tables
using Documenter
using Documenter.Remotes: GitHub

makedocs(;
    modules=[Tables],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
        "Using the Interface" => "using-the-interface.md",
        "Implementing the Interface" => "implementing-the-interface.md",
        "API Reference" => "api.md",
    ],
    repo=GitHub("JuliaData/Tables.jl"),
    sitename="Tables.jl",
    authors="Jacob Quinn",
    checkdocs=:none,
)

deploydocs(;
    repo="github.com/JuliaData/Tables.jl",
    devbranch = "main"
)
