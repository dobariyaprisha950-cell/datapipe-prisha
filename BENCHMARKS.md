# Benchmarks

No benchmark measurements are claimed in this repository. Run `uv run python benchmarks/benchmark.py --records 200000 --workers 4` on the target host and preserve its emitted JSON here with CPU, OS, Python and memory notes. The benchmark prints actual wall time, throughput, peak RSS high-water mark, and speedup for sequential and process-pool normalization. Process startup and serialization can make the parallel path slower for modest input sizes; the 3x target is not guaranteed.
