import json, sys, statistics as st
from collections import defaultdict
rows = [json.loads(l) for l in open(sys.argv[1] if len(sys.argv) > 1 else "results_dir.jsonl")]
g = defaultdict(list)
for r in rows:
    g[(r["link_mbps"], r["mode"], r["budget_mb"] if r["mode"] == "transcode" else 0, r["workers"])].append(r)
order = {"table": 0, "stream": 1, "transcode": 2}
for link in sorted({k[0] for k in g}):
    floor = rows[0]["input_mb"] / link
    print(f"\n### Link {link/1000:g} GB/s aggregate (data-transfer floor {floor:.2f} s for {rows[0]['input_mb']:.0f} MB, {rows[0]['files']} files)\n")
    print("| mode | budget | workers | wall s (median) | MB/s | peak memory MB (max) |")
    print("|---|---|---|---|---|---|")
    for k in sorted((k for k in g if k[0] == link), key=lambda k: (order[k[1]], k[2], k[3])):
        rs = g[k]
        print(f"| {k[1]} | {str(k[2]) + ' MB' if k[2] else ''} | {k[3]} | {st.median(r['wall_s'] for r in rs):.2f} | "
              f"{st.median(r['throughput_mbps'] for r in rs):.0f} | {max(r['peak_footprint_mb'] for r in rs):.0f} |")
