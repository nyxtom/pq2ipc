
### Simulated GCS: 25 ms latency, 150 MB/s per stream, 1.5 GB/s aggregate each way. All modes: 32 IO threads, 16 upload threads. Transcoder: 8 reads in flight per worker. Link floor ≈ 1.5 s (1533 MB through 1.5 GB/s + startup + drain)

| codec | mode | workers | knob | wall s (median) | MB/s | peak memory MB (max) | GET requests |
|---|---|---|---|---|---|---|---|
| none | table | 2 |  | 7.01 | 219 | 1886 | 25 |
| none | table | 4 |  | 4.15 | 370 | 3102 | 25 |
| none | table | 8 |  | 2.78 | 550 | 5732 | 25 |
| none | stream | 2 |  | 5.98 | 256 | 1182 | 25 |
| none | stream | 4 |  | 3.41 | 449 | 1913 | 25 |
| none | stream | 8 |  | 2.37 | 646 | 2841 | 25 |
| none | transcode | 4 | budget 64 MB | 5.84 | 263 | 270 | 153 |
| none | transcode | 4 | budget 128 MB | 3.24 | 473 | 341 | 153 |
| none | transcode | 4 | budget 256 MB | 2.01 | 763 | 500 | 153 |
| none | transcode | 4 | budget 512 MB | 1.61 | 953 | 776 | 153 |
| none | transcode | 4 | budget 1024 MB | 1.58 | 973 | 851 | 153 |
| snappy | table | 2 |  | 7.07 | 217 | 1887 | 25 |
| snappy | table | 4 |  | 4.19 | 366 | 3163 | 25 |
| snappy | table | 8 |  | 3.33 | 460 | 6011 | 25 |
| snappy | stream | 2 |  | 6.02 | 254 | 1101 | 25 |
| snappy | stream | 4 |  | 3.44 | 446 | 1972 | 25 |
| snappy | stream | 8 |  | 2.35 | 652 | 3043 | 25 |
| snappy | transcode | 4 | budget 64 MB | 5.64 | 272 | 340 | 153 |
| snappy | transcode | 4 | budget 128 MB | 3.19 | 481 | 448 | 153 |
| snappy | transcode | 4 | budget 256 MB | 1.93 | 795 | 565 | 153 |
| snappy | transcode | 4 | budget 512 MB | 1.57 | 976 | 837 | 153 |
| snappy | transcode | 4 | budget 1024 MB | 1.55 | 986 | 900 | 153 |
| zstd | table | 2 |  | 7.04 | 218 | 1894 | 25 |
| zstd | table | 4 |  | 4.17 | 368 | 3097 | 25 |
| zstd | table | 8 |  | 2.78 | 551 | 5785 | 25 |
| zstd | stream | 2 |  | 6.02 | 255 | 1234 | 25 |
| zstd | stream | 4 |  | 3.43 | 447 | 1980 | 25 |
| zstd | stream | 8 |  | 2.35 | 651 | 2997 | 25 |
| zstd | transcode | 4 | budget 64 MB | 5.66 | 271 | 336 | 153 |
| zstd | transcode | 4 | budget 128 MB | 3.22 | 475 | 424 | 153 |
| zstd | transcode | 4 | budget 256 MB | 1.90 | 808 | 570 | 153 |
| zstd | transcode | 4 | budget 512 MB | 1.58 | 970 | 860 | 153 |
| zstd | transcode | 4 | budget 1024 MB | 1.55 | 986 | 914 | 153 |

### CPU only (no network)

| codec | mode | workers | knob | wall s (median) | MB/s | peak memory MB (max) | GET requests |
|---|---|---|---|---|---|---|---|
| none | table | 4 |  | 0.18 | 8705 | 3882 | 25 |
| none | stream | 4 |  | 0.14 | 10977 | 1346 | 25 |
| none | transcode | 4 | budget 512 MB | 0.07 | 21804 | 691 | 153 |
| snappy | table | 4 |  | 0.20 | 7568 | 3739 | 25 |
| snappy | stream | 4 |  | 0.17 | 9028 | 1444 | 25 |
| snappy | transcode | 4 | budget 512 MB | 0.10 | 16045 | 762 | 153 |
| zstd | table | 4 |  | 0.20 | 7547 | 3782 | 25 |
| zstd | stream | 4 |  | 0.17 | 9025 | 1473 | 25 |
| zstd | transcode | 4 | budget 512 MB | 0.09 | 16145 | 772 | 153 |
