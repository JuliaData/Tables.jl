using Documenter, Tables, DocumenterMarkdown

makedocs(;
    modules=[Tables],
    format=Markdown(),
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
)
