#!/bin/bash
# Clean sweep: every mode gets the same IO pool (32) and uploader (16 threads).
# 3 reps per config; any failed run aborts the sweep.
set -eo pipefail
out=${1:-results.jsonl}; : > "$out"
common=(--io-threads=32 --upload-threads=16)
run() { for rep in 1 2 3; do ./bench "${common[@]}" "$@" 2>/dev/null | sed "s/^{/{\"rep\":$rep,/" >> "$out"; done; }
for codec in snappy zstd none; do
  f=data/$codec.parquet
  for w in 2 4 8; do
    run --mode=table --file=$f --workers=$w
    run --mode=stream --file=$f --workers=$w
  done
  for b in 64 128 256 512 1024; do run --mode=transcode --file=$f --workers=4 --inflight=8 --budget-mb=$b; done
  for m in table stream; do run --mode=$m --file=$f --workers=4 --net=0; done
  run --mode=transcode --file=$f --workers=4 --inflight=8 --budget-mb=512 --net=0
done
