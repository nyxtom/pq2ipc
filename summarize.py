import json, sys, statistics as st
from collections import defaultdict
rows = [json.loads(l) for l in open(sys.argv[1] if len(sys.argv) > 1 else "results.jsonl")]
g = defaultdict(list)
for r in rows:
    codec = r["file"].split("/")[-1].split(".")[0]
    knob = f"budget {r['budget_mb']} MB" if r["mode"] == "transcode" else ""
    g[(r["net"], codec, r["mode"], r["workers"], knob)].append(r)
order = {"table": 0, "stream": 1, "transcode": 2}
for net in (1, 0):
    print(f"\n### {'Simulated GCS: 25 ms latency, 150 MB/s per stream, 1.5 GB/s aggregate each way. All modes: 32 IO threads, 16 upload threads. Transcoder: 8 reads in flight per worker. Link floor ≈ 1.5 s (1533 MB through 1.5 GB/s + startup + drain)' if net else 'CPU only (no network)'}\n")
    print("| codec | mode | workers | knob | wall s (median) | MB/s | peak memory MB (max) | GET requests |")
    print("|---|---|---|---|---|---|---|---|")
    for k in sorted((k for k in g if k[0] == net), key=lambda k: (k[1], order[k[2]], k[3], int(k[4].split()[1]) if k[4] else 0)):
        rs = g[k]
        print(f"| {k[1]} | {k[2]} | {k[3]} | {k[4]} | {st.median(r['wall_s'] for r in rs):.2f} | "
              f"{st.median(r['throughput_mbps'] for r in rs):.0f} | {max(r['peak_footprint_mb'] for r in rs):.0f} | "
              f"{rs[0]['get_requests']} |")
