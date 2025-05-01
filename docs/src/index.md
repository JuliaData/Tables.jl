# Tables.jl Documentation

This guide provides documentation around the powerful tables interfaces in the Tables.jl package.
Note that the package, and hence, documentation, are geared towards package and library developers
who intend to implement and consume the interfaces. Users, on the other hand, benefit from these
other packages that provide useful access to table data in various formats or workflows. While everyone
is encouraged to understand the interfaces and the functionality they allow, just note that most users
don't need to use Tables.jl directly.

With that said, don't hesitate to [open a new issue](https://github.com/JuliaData/Tables.jl/issues/new), even
just for a question, or come chat with us on the [#data](https://julialang.slack.com/messages/data/) slack
channel with questions, concerns, or clarifications. Also one can find list of packages that supports
Tables.jl interface in [INTEGRATIONS.md](https://github.com/JuliaData/Tables.jl/blob/master/INTEGRATIONS.md).

Please refer to [TableOperations.jl](https://github.com/JuliaData/TableOperations.jl) for common table operations
such as  `select`, `transform`, `filter` and  `map` and to [TableTransforms.jl](https://github.com/JuliaML/TableTransforms.jl)
for more sophisticated transformations.

```@contents
Depth = 3
Pages = [
    "index.md",
    "using-the-interface.md",
    "implementing-the-interface.md",
    "api.md",
]
```
