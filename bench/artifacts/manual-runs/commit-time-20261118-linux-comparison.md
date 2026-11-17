# Hot-path budget comparison

UTC: 2026-11-17T19:09:41.619458+00:00 to 2026-11-17T19:20:50.314266+00:00.
Machine: 82EY; CPU AMD Ryzen 5 4600H with Radeon Graphics; cores 6 physical / 12 logical; RAM 30.22 GiB.
OS: Linux-7.0.0-43-generic-x86_64-with-glibc2.43; architecture x86_64. Benchmark temporary-store filesystem: ext2/ext3 at `<ext4 directory>/tmp-budget`.
Values are medians [min–max] across runs. Latency is the median of each run's p50/p99, with the range of those run percentiles.
Blocks count positions handled by successful requests, not changed blocks or bytes; point requests count one and chunk/area forms use actual geometry and returned entries.
Load sampling: start and end of each benchmark process; observed ranges, not continuous sampling. Process niceness: 0.

## relaxed

Durability `relaxed`; clients/workers 4/4; pipeline 1; I/O timeout 5000 ms; requests/run 200,000; keyspace 2048 blocks/axis; 15 alternating runs/build; load average ranges (1/5/15 min) 7.86–11.19 / 8.66–9.27 / 6.70–7.76.
Gate: paired upper95% ratio CI below1−5.0/100
WATCH before: none; after: none.
Before: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `ae818c7b23837f54fbc2cfa1d877d492bb334399bd433ed4b23894a0d47aa87b`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.
After: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `6724faf46da71412ce2d5f47f7fdd6430dc9b0bfd8c7df93d201e7b2a436a8c8`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.

| Scenario / request | Actual mix before / after (%) | Blocks/request before / after | Before req/s | After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 ms | Verdict | Change req/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `world`: Per client: move and read AREA AROUND radius 2 each 50 requests; one SET CHUNK; otherwise SET/GET BLOCK. | set_block 76.000 [76.000–76.000]%; get_block 20.000 [20.000–20.000]%; set_chunk 2.000 [2.000–2.000]%; get_area 2.000 [2.000–2.000]% / set_block 76.000 [76.000–76.000]%; get_block 20.000 [20.000–20.000]%; set_chunk 2.000 [2.000–2.000]%; get_area 2.000 [2.000–2.000]% | 69.108 [68.173–70.740] / 69.108 [68.173–70.740] | 57,365.5 [56,427.0–58,668.7] | 56,642.7 [54,936.1–57,344.7] | 3,985,845.7 [3,919,365.5–4,048,623.5] | 3,907,161.6 [3,835,155.0–4,012,743.8] | 0.046 [0.045–0.047] | 0.047 [0.047–0.048] | 0.118 [0.106–0.132] | 0.122 [0.106–0.136] | regression criterion not met | -1.26% |
| `canvas`: Per client: one GET AREA viewport (up to 4x4 chunks) each 20 requests; otherwise random SET BLOCK. | set_block 95.000 [95.000–95.000]%; get_area 5.000 [5.000–5.000]% / set_block 95.000 [95.000–95.000]%; get_area 5.000 [5.000–5.000]% | 205.750 [205.750–205.750] / 205.750 [205.750–205.750] | 49,890.8 [48,690.6–51,133.8] | 49,384.3 [47,883.6–49,961.5] | 10,265,028.7 [10,018,098.8–10,520,784.8] | 10,160,828.8 [9,852,057.9–10,279,571.4] | 0.048 [0.047–0.049] | 0.049 [0.049–0.050] | 0.168 [0.164–0.178] | 0.170 [0.165–0.179] | regression criterion not met | -1.02% |
| `simulation`: Per client: alternate GET CHUNK and SET CHUNK while sweeping the region, starting with GET. | set_chunk 50.000 [50.000–50.000]%; get_chunk 50.000 [50.000–50.000]% / set_chunk 50.000 [50.000–50.000]%; get_chunk 50.000 [50.000–50.000]% | 256.000 [256.000–256.000] / 256.000 [256.000–256.000] | 50,804.1 [50,120.1–51,508.9] | 50,264.8 [49,751.5–51,052.7] | 13,005,844.9 [12,830,732.7–13,186,276.6] | 12,867,793.4 [12,736,372.1–13,069,484.0] | 0.051 [0.051–0.052] | 0.052 [0.051–0.052] | 0.147 [0.140–0.159] | 0.147 [0.145–0.151] | regression criterion not met | -1.06% |

## fsync-wal

Durability `fsync-wal`; clients/workers 16/16; pipeline 1; I/O timeout 5000 ms; requests/run 5,000; keyspace 256 blocks/axis; 15 alternating runs/build; load average ranges (1/5/15 min) 8.56–11.55 / 9.23–9.79 / 7.76–8.09.
Gate: reported only (does not affect exit status).
WATCH before: none; after: none.
Before: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `ae818c7b23837f54fbc2cfa1d877d492bb334399bd433ed4b23894a0d47aa87b`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.
After: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `6724faf46da71412ce2d5f47f7fdd6430dc9b0bfd8c7df93d201e7b2a436a8c8`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.

| Scenario / request | Actual mix before / after (%) | Blocks/request before / after | Before req/s | After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 ms | Verdict | Change req/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `world`: Per client: move and read AREA AROUND radius 2 each 50 requests; one SET CHUNK; otherwise SET/GET BLOCK. | set_block 75.680 [75.680–75.680]%; get_block 20.160 [20.160–20.160]%; set_chunk 1.920 [1.920–1.920]%; get_area 2.240 [2.240–2.240]% / set_block 75.680 [75.680–75.680]%; get_block 20.160 [20.160–20.160]%; set_chunk 1.920 [1.920–1.920]%; get_area 2.240 [2.240–2.240]% | 65.112 [62.245–70.386] / 65.112 [62.245–70.386] | 7,239.0 [6,819.5–7,598.3] | 7,255.7 [6,767.4–7,671.4] | 474,149.0 [438,388.1–529,897.5] | 476,045.4 [429,899.7–523,461.7] | 2.293 [2.202–2.371] | 2.302 [2.219–2.366] | 6.063 [5.267–6.981] | 5.888 [5.561–8.540] | reported only | +0.23% |
| `canvas`: Per client: one GET AREA viewport (up to 4x4 chunks) each 20 requests; otherwise random SET BLOCK. | set_block 94.880 [94.880–94.880]%; get_area 5.120 [5.120–5.120]% / set_block 94.880 [94.880–94.880]%; get_area 5.120 [5.120–5.120]% | 210.664 [210.664–210.664] / 210.664 [210.664–210.664] | 6,298.2 [5,834.6–6,491.2] | 6,247.1 [5,858.0–6,454.3] | 1,326,796.5 [1,229,135.6–1,367,472.2] | 1,316,032.1 [1,234,067.4–1,359,683.8] | 2.425 [2.353–2.505] | 2.448 [2.353–2.549] | 4.896 [4.765–5.296] | 4.953 [4.822–5.155] | reported only | -0.81% |
| `simulation`: Per client: alternate GET CHUNK and SET CHUNK while sweeping the region, starting with GET. | set_chunk 49.920 [49.920–49.920]%; get_chunk 50.080 [50.080–50.080]% / set_chunk 49.920 [49.920–49.920]%; get_chunk 50.080 [50.080–50.080]% | 256.000 [256.000–256.000] / 256.000 [256.000–256.000] | 11,779.6 [10,995.1–12,253.7] | 11,839.4 [11,410.9–12,406.1] | 3,015,567.3 [2,814,752.0–3,136,937.6] | 3,030,879.5 [2,921,188.6–3,175,964.2] | 0.346 [0.254–0.482] | 0.283 [0.228–0.443] | 3.493 [3.297–5.192] | 3.515 [3.212–4.197] | reported only | +0.51% |

## Paired inference

Window: `commit-time-20261118`; kind: source-comparison.
100000 whole-pair resamples; seed20261011 resets per row; median ratio;95% percentile CI, Type7; sample SD n−1.
Matching uses(pair,seed), not execution order. Pairs are exchangeable; cross-pair drift is not modeled. Small pair counts limit inference; one pair has no CI/SD or PASS claim.
Regression requires upper CI ratio strictly below1−budget/100. Inconclusive is not equivalence. Exit0 means no gated criterion met, not proof of equal speed.
Build source_commit may identify an instrumentation overlay; underlying production core is unavailable here. External reviewed manifests can supply that mapping. Source fingerprint covers benchmark inputs, not whole-core contents.

| Profile / scenario | Before req/s | After req/s | Before blocks/s | After blocks/s | Paired median change |95% CI change | Ratio SD pp | CI decision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| relaxed / world | 57,365.5 [56,427.0–58,668.7] | 56,642.7 [54,936.1–57,344.7] | 3,985,845.7 [3,919,365.5–4,048,623.5] | 3,907,161.6 [3,835,155.0–4,012,743.8] | -1.30% | [-2.13%, -0.85%] | 1.38 | regression criterion not met |
| relaxed / canvas | 49,890.8 [48,690.6–51,133.8] | 49,384.3 [47,883.6–49,961.5] | 10,265,028.7 [10,018,098.8–10,520,784.8] | 10,160,828.8 [9,852,057.9–10,279,571.4] | -0.90% | [-2.89%, -0.05%] | 1.81 | regression criterion not met |
| relaxed / simulation | 50,804.1 [50,120.1–51,508.9] | 50,264.8 [49,751.5–51,052.7] | 13,005,844.9 [12,830,732.7–13,186,276.6] | 12,867,793.4 [12,736,372.1–13,069,484.0] | -0.82% | [-1.44%, -0.34%] | 1.02 | regression criterion not met |
| fsync-wal / world | 7,239.0 [6,819.5–7,598.3] | 7,255.7 [6,767.4–7,671.4] | 474,149.0 [438,388.1–529,897.5] | 476,045.4 [429,899.7–523,461.7] | +0.14% | [-4.37%, +5.58%] | 5.17 | regression criterion not met |
| fsync-wal / canvas | 6,298.2 [5,834.6–6,491.2] | 6,247.1 [5,858.0–6,454.3] | 1,326,796.5 [1,229,135.6–1,367,472.2] | 1,316,032.1 [1,234,067.4–1,359,683.8] | -0.21% | [-3.34%, +1.37%] | 4.14 | regression criterion not met |
| fsync-wal / simulation | 11,779.6 [10,995.1–12,253.7] | 11,839.4 [11,410.9–12,406.1] | 3,015,567.3 [2,814,752.0–3,136,937.6] | 3,030,879.5 [2,921,188.6–3,175,964.2] | +0.88% | [-2.41%, +3.52%] | 3.47 | regression criterion not met |

## Same-window noise reference

Raw control `<ext4 directory>/noise.json` SHA256 `660f97cde4c1bd399f3c8693d594875b39c90e15d99c0812c9b0b117f2cd997a`.
Its actual build source_commit and binary baseline are printed separately below and may differ from this comparison’s before build. Noise is not subtracted.
# Hot-path budget comparison

UTC: 2026-11-17T18:57:58.008490+00:00 to 2026-11-17T19:09:41.599024+00:00.
Machine: 82EY; CPU AMD Ryzen 5 4600H with Radeon Graphics; cores 6 physical / 12 logical; RAM 30.22 GiB.
OS: Linux-7.0.0-43-generic-x86_64-with-glibc2.43; architecture x86_64. Benchmark temporary-store filesystem: ext2/ext3 at `<ext4 directory>/tmp-budget`.
Values are medians [min–max] across runs. Latency is the median of each run's p50/p99, with the range of those run percentiles.
Blocks count positions handled by successful requests, not changed blocks or bytes; point requests count one and chunk/area forms use actual geometry and returned entries.
Load sampling: start and end of each benchmark process; observed ranges, not continuous sampling. Process niceness: 0.

## relaxed

Durability `relaxed`; clients/workers 4/4; pipeline 1; I/O timeout 5000 ms; requests/run 200,000; keyspace 2048 blocks/axis; 15 alternating runs/build; load average ranges (1/5/15 min) 2.53–11.05 / 3.30–8.32 / 3.75–6.16.
Gate: paired upper95% ratio CI below1−5.0/100
WATCH before: none; after: none.
Before: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `6724faf46da71412ce2d5f47f7fdd6430dc9b0bfd8c7df93d201e7b2a436a8c8`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.
After: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `6724faf46da71412ce2d5f47f7fdd6430dc9b0bfd8c7df93d201e7b2a436a8c8`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.

| Scenario / request | Actual mix before / after (%) | Blocks/request before / after | Before req/s | After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 ms | Verdict | Change req/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `world`: Per client: move and read AREA AROUND radius 2 each 50 requests; one SET CHUNK; otherwise SET/GET BLOCK. | set_block 76.000 [76.000–76.000]%; get_block 20.000 [20.000–20.000]%; set_chunk 2.000 [2.000–2.000]%; get_area 2.000 [2.000–2.000]% / set_block 76.000 [76.000–76.000]%; get_block 20.000 [20.000–20.000]%; set_chunk 2.000 [2.000–2.000]%; get_area 2.000 [2.000–2.000]% | 69.108 [68.173–70.740] / 69.108 [68.173–70.740] | 56,551.6 [46,608.7–57,333.1] | 56,197.0 [21,800.0–57,277.4] | 3,908,974.9 [3,215,566.1–4,003,516.6] | 3,894,921.3 [1,503,995.2–3,991,541.5] | 0.047 [0.047–0.048] | 0.047 [0.047–0.048] | 0.117 [0.107–0.136] | 0.122 [0.111–0.133] | regression criterion not met | -0.63% |
| `canvas`: Per client: one GET AREA viewport (up to 4x4 chunks) each 20 requests; otherwise random SET BLOCK. | set_block 95.000 [95.000–95.000]%; get_area 5.000 [5.000–5.000]% / set_block 95.000 [95.000–95.000]%; get_area 5.000 [5.000–5.000]% | 205.750 [205.750–205.750] / 205.750 [205.750–205.750] | 49,099.2 [18,623.7–50,249.6] | 49,128.3 [32,118.0–49,642.2] | 10,102,167.6 [3,831,830.4–10,338,846.0] | 10,108,139.2 [6,608,272.9–10,213,872.8] | 0.050 [0.049–0.050] | 0.050 [0.049–0.050] | 0.170 [0.164–0.187] | 0.170 [0.164–0.174] | regression criterion not met | +0.06% |
| `simulation`: Per client: alternate GET CHUNK and SET CHUNK while sweeping the region, starting with GET. | set_chunk 50.000 [50.000–50.000]%; get_chunk 50.000 [50.000–50.000]% / set_chunk 50.000 [50.000–50.000]%; get_chunk 50.000 [50.000–50.000]% | 256.000 [256.000–256.000] / 256.000 [256.000–256.000] | 50,146.6 [21,392.6–51,173.9] | 50,355.6 [26,435.2–50,488.7] | 12,837,517.4 [5,476,496.9–13,100,509.5] | 12,891,020.8 [6,767,399.0–12,925,102.9] | 0.052 [0.051–0.052] | 0.052 [0.051–0.052] | 0.147 [0.143–0.155] | 0.147 [0.145–0.163] | regression criterion not met | +0.42% |

## fsync-wal

Durability `fsync-wal`; clients/workers 16/16; pipeline 1; I/O timeout 5000 ms; requests/run 5,000; keyspace 256 blocks/axis; 15 alternating runs/build; load average ranges (1/5/15 min) 9.51–14.15 / 8.32–9.49 / 6.16–6.70.
Gate: reported only (does not affect exit status).
WATCH before: none; after: none.
Before: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `6724faf46da71412ce2d5f47f7fdd6430dc9b0bfd8c7df93d201e7b2a436a8c8`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.
After: `d900641e3e9ee70ca43c52f1ee07ed4d12ccfb34` (clean), Release; chunk 16x16; binary SHA256 `6724faf46da71412ce2d5f47f7fdd6430dc9b0bfd8c7df93d201e7b2a436a8c8`; benchmark source SHA256 `4946d556d6faba6f14fa6c8f92a225ab36dcb0f17b21da5cf274afdf81a09e06`.

| Scenario / request | Actual mix before / after (%) | Blocks/request before / after | Before req/s | After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 ms | Verdict | Change req/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `world`: Per client: move and read AREA AROUND radius 2 each 50 requests; one SET CHUNK; otherwise SET/GET BLOCK. | set_block 75.680 [75.680–75.680]%; get_block 20.160 [20.160–20.160]%; set_chunk 1.920 [1.920–1.920]%; get_area 2.240 [2.240–2.240]% / set_block 75.680 [75.680–75.680]%; get_block 20.160 [20.160–20.160]%; set_chunk 1.920 [1.920–1.920]%; get_area 2.240 [2.240–2.240]% | 65.112 [62.245–70.386] / 65.112 [62.245–70.386] | 7,287.5 [7,093.1–7,787.7] | 7,403.9 [6,503.1–7,646.0] | 472,457.3 [449,527.3–529,973.6] | 479,698.4 [425,097.1–508,418.7] | 2.293 [2.215–2.355] | 2.314 [2.225–2.402] | 5.866 [5.370–7.576] | 5.977 [5.392–7.865] | reported only | +1.60% |
| `canvas`: Per client: one GET AREA viewport (up to 4x4 chunks) each 20 requests; otherwise random SET BLOCK. | set_block 94.880 [94.880–94.880]%; get_area 5.120 [5.120–5.120]% / set_block 94.880 [94.880–94.880]%; get_area 5.120 [5.120–5.120]% | 210.664 [210.664–210.664] / 210.664 [210.664–210.664] | 6,208.9 [5,902.5–6,472.5] | 6,225.7 [5,923.7–6,552.6] | 1,307,991.4 [1,243,443.4–1,363,529.7] | 1,311,528.5 [1,247,905.8–1,380,407.2] | 2.452 [2.364–2.509] | 2.453 [2.345–2.514] | 5.081 [4.746–5.656] | 4.971 [4.664–5.316] | reported only | +0.27% |
| `simulation`: Per client: alternate GET CHUNK and SET CHUNK while sweeping the region, starting with GET. | set_chunk 49.920 [49.920–49.920]%; get_chunk 50.080 [50.080–50.080]% / set_chunk 49.920 [49.920–49.920]%; get_chunk 50.080 [50.080–50.080]% | 256.000 [256.000–256.000] / 256.000 [256.000–256.000] | 11,654.3 [10,597.9–12,173.1] | 11,624.8 [11,193.7–12,429.3] | 2,983,496.5 [2,713,070.4–3,116,322.8] | 2,975,948.2 [2,865,597.2–3,181,901.7] | 0.324 [0.233–0.622] | 0.326 [0.263–0.528] | 3.493 [3.394–5.417] | 3.589 [3.359–4.364] | reported only | -0.25% |

## Paired inference

Window: `commit-time-20261118`; kind: noise-floor.
100000 whole-pair resamples; seed20261011 resets per row; median ratio;95% percentile CI, Type7; sample SD n−1.
Matching uses(pair,seed), not execution order. Pairs are exchangeable; cross-pair drift is not modeled. Small pair counts limit inference; one pair has no CI/SD or PASS claim.
Regression requires upper CI ratio strictly below1−budget/100. Inconclusive is not equivalence. Exit0 means no gated criterion met, not proof of equal speed.
Build source_commit may identify an instrumentation overlay; underlying production core is unavailable here. External reviewed manifests can supply that mapping. Source fingerprint covers benchmark inputs, not whole-core contents.

| Profile / scenario | Before req/s | After req/s | Before blocks/s | After blocks/s | Paired median change |95% CI change | Ratio SD pp | CI decision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| relaxed / world | 56,551.6 [46,608.7–57,333.1] | 56,197.0 [21,800.0–57,277.4] | 3,908,974.9 [3,215,566.1–4,003,516.6] | 3,894,921.3 [1,503,995.2–3,991,541.5] | -1.08% | [-2.43%, +0.40%] | 13.61 | regression criterion not met |
| relaxed / canvas | 49,099.2 [18,623.7–50,249.6] | 49,128.3 [32,118.0–49,642.2] | 10,102,167.6 [3,831,830.4–10,338,846.0] | 10,108,139.2 [6,608,272.9–10,213,872.8] | +0.87% | [-0.57%, +1.98%] | 18.71 | regression criterion not met |
| relaxed / simulation | 50,146.6 [21,392.6–51,173.9] | 50,355.6 [26,435.2–50,488.7] | 12,837,517.4 [5,476,496.9–13,100,509.5] | 12,891,020.8 [6,767,399.0–12,925,102.9] | +0.52% | [-1.52%, +0.70%] | 6.37 | regression criterion not met |
| fsync-wal / world | 7,287.5 [7,093.1–7,787.7] | 7,403.9 [6,503.1–7,646.0] | 472,457.3 [449,527.3–529,973.6] | 479,698.4 [425,097.1–508,418.7] | +1.18% | [-2.62%, +2.43%] | 3.91 | regression criterion not met |
| fsync-wal / canvas | 6,208.9 [5,902.5–6,472.5] | 6,225.7 [5,923.7–6,552.6] | 1,307,991.4 [1,243,443.4–1,363,529.7] | 1,311,528.5 [1,247,905.8–1,380,407.2] | -0.78% | [-2.47%, +4.31%] | 4.30 | regression criterion not met |
| fsync-wal / simulation | 11,654.3 [10,597.9–12,173.1] | 11,624.8 [11,193.7–12,429.3] | 2,983,496.5 [2,713,070.4–3,116,322.8] | 2,975,948.2 [2,865,597.2–3,181,901.7] | +1.91% | [-1.53%, +6.65%] | 4.80 | regression criterion not met |

