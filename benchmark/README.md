# Column construction benchmarks

From the repository root, create a benchmark environment using this checkout:

```sh
julia --project=benchmark -e 'using Pkg; Pkg.develop(path="."); Pkg.instantiate()'
julia --project=benchmark benchmark/buffering.jl 100000
```

For before/after comparisons, run the same script in separate Julia processes and
point each environment at the corresponding Tables checkout. Use the same Julia
version, dependency versions, and thread count. The script prints its Julia and
Tables versions and CPU target. Input generation and the first construction are
outside the timed samples. Each trial uses one evaluation per sample, up to 200
samples, and a three-second time budget.

The CSV output reports median/minimum elapsed time, allocated bytes, allocation
count, sample count, and a precision check for the late-widening case. Allocated
bytes measure allocation traffic, not peak memory or retained output size.

Cases cover known schemas, deliberately hidden schemas on the same dense rows,
unknown-length iteration, late widening to `Any`, 32-column rows, and sparse rows
with changing column names. The hidden-schema cases measure the cost of generic
inference; providing the known schema avoids that path. The late-widening input
contains a large integer that the old implementation rounded before the column
reached its final `Any` type.

For a separate whole-process peak-RSS measurement on macOS:

```sh
/usr/bin/time -l julia --project=benchmark benchmark/buffering.jl 1000000 rss unknown_dense columntable
```

On Linux, use `/usr/bin/time -v`. RSS mode warms up on 1,000 rows, runs a full GC,
then constructs the requested input once. Peak RSS includes the Julia runtime,
compilation, the input, buffering, and output. It is not a measurement of the
buffer alone. Run `known_dense` as a control. The RSS-mode output also reports
`Base.summarysize` of the resulting columns.
