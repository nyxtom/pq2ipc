
### Link 1.5 GB/s aggregate (data-transfer floor 2.70 s for 4049 MB, 16 files)

| mode | budget | workers | wall s (median) | MB/s | peak memory MB (max) |
|---|---|---|---|---|---|
| table |  | 8 | 5.69 | 712 | 3751 |
| stream |  | 8 | 5.04 | 804 | 2313 |
| stream |  | 16 | 4.23 | 956 | 4158 |
| transcode | 512 MB | 4 | 4.00 | 1013 | 687 |
| transcode | 512 MB | 8 | 3.29 | 1229 | 988 |
| transcode | 512 MB | 16 | 3.02 | 1339 | 1343 |
| transcode | 512 MB | 32 | 3.06 | 1322 | 1948 |
| transcode | 1024 MB | 4 | 3.97 | 1021 | 680 |
| transcode | 1024 MB | 8 | 3.29 | 1233 | 1208 |
| transcode | 1024 MB | 16 | 3.05 | 1326 | 1703 |
| transcode | 1024 MB | 32 | 3.01 | 1344 | 2750 |

### Link 5 GB/s aggregate (data-transfer floor 0.81 s for 4049 MB, 16 files)

| mode | budget | workers | wall s (median) | MB/s | peak memory MB (max) |
|---|---|---|---|---|---|
| table |  | 8 | 5.59 | 725 | 3677 |
| stream |  | 8 | 4.76 | 851 | 2181 |
| stream |  | 16 | 2.84 | 1427 | 4229 |
| transcode | 512 MB | 4 | 3.62 | 1119 | 724 |
| transcode | 512 MB | 8 | 2.19 | 1851 | 1089 |
| transcode | 512 MB | 16 | 1.97 | 2059 | 1487 |
| transcode | 512 MB | 32 | 1.95 | 2074 | 2302 |
| transcode | 1024 MB | 4 | 3.64 | 1113 | 716 |
| transcode | 1024 MB | 8 | 1.91 | 2117 | 1354 |
| transcode | 1024 MB | 16 | 1.52 | 2666 | 2026 |
| transcode | 1024 MB | 32 | 1.61 | 2513 | 2671 |
| transcode | 2048 MB | 4 | 3.62 | 1118 | 733 |
| transcode | 2048 MB | 8 | 1.92 | 2113 | 1345 |
| transcode | 2048 MB | 16 | 1.51 | 2683 | 2482 |
| transcode | 2048 MB | 32 | 1.52 | 2670 | 3599 |
