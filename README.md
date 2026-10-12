# pq2ipc: bounded-memory Parquet → Arrow IPC for image tables

A page-level transcoder built on Arrow C++ 26's low-level APIs, benchmarked
against two `parquet::arrow` baselines over a simulated GCS link.

## Files

- `transcode.h`: the transcoder (`ByteBudget`, `Lease`, `LeasedBuffer`, `PartOutputStream`, `RowGroupTranscoder`)
- `sim.h`: simulated GCS (latency + per-stream + aggregate bandwidth), ranged-read file, bounded uploader, memory sampling
- `bench.cc`: modes `table`, `stream`, `transcode`, `verify`
- `gen.cc`: generates test files (Arrow default writer settings + page index)
- `run_bench.sh`, `summarize.py`, `RESULTS.md`: single-file sweep
- `run_dir.sh`, `summarize_dir.py`, `RESULTS_DIR.md`: 16-file directory sweep

## Build and run

    brew install apache-arrow pkgconf
    make
    mkdir -p data && ./gen --out=data/snappy.parquet --codec=snappy
    ./bench --mode=transcode --file=data/snappy.parquet --budget-mb=256
    ./run_bench.sh results.jsonl && python3 summarize.py results.jsonl

    # many files: (file, row group) work queue across the directory
    for i in $(seq 1 16); do ./gen --out=data/dir/part-$(printf %02d $i).parquet --rows=1000 --rg-rows=400 --seed=$i; done
    ./bench --mode=transcode --dir=data/dir --workers=16 --budget-mb=1024 --inflight=8 --io-threads=64 --upload-threads=32
    ./run_dir.sh results_dir.jsonl && python3 summarize_dir.py results_dir.jsonl   # -> RESULTS_DIR.md

Correctness: `./bench --mode=X --net=0 --out-dir=out/X` for each mode, then
`./bench --mode=verify --a=out/table --b=out/transcode`. Output is identical
for snappy, zstd and uncompressed.

## How the transcoder works

Per row group:
1. Read the small columns with `parquet::arrow` and compute the keep mask.
2. Use the offset index to plan ranged reads over image pages with kept rows
   (pages with no kept rows are never downloaded).
3. Keep up to N reads in flight, each holding a budget lease for its raw bytes.
4. Decode each page with `PageReader` plus PLAIN or dictionary decoders into
   `(ptr, len)` views, then copy once into an exactly sized values buffer. The
   lease moves to that buffer.
5. Write with the stock IPC stream writer. Body buffers reach the output
   stream by reference (0 bytes copied), and the lease is released when the
   uploader drops the part.

Memory ≈ budget + workers × (one decoded unit + the row group's small columns
and dictionary page). Deadlock rule: a worker only blocks on the budget after
it has flushed its output and holds no leases.

## Not handled

Nested image columns, files without a page index (would need sequential
page-header reads), encrypted files, and a single unit over 2 GB (int32
offsets).
