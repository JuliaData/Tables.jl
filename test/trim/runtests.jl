using JuliaC

mktempdir() do output
    name = "filtermask" * (Sys.iswindows() ? ".exe" : "")
    cd(output) do
        JuliaC.main(["--output-exe", name, "--project", @__DIR__, "--trim=safe", joinpath(@__DIR__, "filtermask.jl")])
        executable = joinpath(output, name)
        for (threshold, stringthreshold) in ((-10, "alpha"), (0, "beta"), (1, "gamma"),
                                            (2, ""), (4, "alphabet"), (999, "zz"))
            run(`$executable $threshold $stringthreshold`)
        end
    end
end
