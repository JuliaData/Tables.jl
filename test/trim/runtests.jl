using JuliaC

mktempdir() do output
    name = "filtermask" * (Sys.iswindows() ? ".exe" : "")
    cd(output) do
        JuliaC.main(["--output-exe", name, "--project", @__DIR__, "--trim=safe", joinpath(@__DIR__, "filtermask.jl")])
        executable = joinpath(output, name)
        for threshold in (-10, 0, 1, 2, 4, 999)
            run(`$executable $threshold`)
        end
    end
end
