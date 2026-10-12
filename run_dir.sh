#!/bin/bash
# Directory sweep: 16 files x 3 row groups, cross-file work queue.
# Same IO pool (64) and uploader (32) for every mode. 3 reps per config.
set -eo pipefail
out=${1:-results_dir.jsonl}; : > "$out"
run() { local link=$1; shift
  for rep in 1 2 3; do
    ./bench --dir=data/dir --io-threads=64 --upload-threads=32 --global-mbps=$link "$@" 2>/dev/null \
      | sed "s/^{/{\"rep\":$rep,\"link_mbps\":$link,/" >> "$out"
  done; }
for link in 1500 5000; do
  run $link --mode=table --workers=8
  for w in 8 16; do run $link --mode=stream --workers=$w; done
  budgets="512 1024"; [ $link = 5000 ] && budgets="512 1024 2048"
  for b in $budgets; do for w in 4 8 16 32; do
    run $link --mode=transcode --workers=$w --inflight=8 --budget-mb=$b
  done; done
done
