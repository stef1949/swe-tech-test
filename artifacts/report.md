# Trace Viewer Analysis

Generated from `C:\Users\stef1\Downloads\swe-tech-test\swe-tech-test\mock48_2500hz_1.5h.zarr` on 2026-04-14T11:55:03+00:00.

## Recording Facts

- Device: `mock-48ch-001`
- Channels: 48
- Sample rate: 2500.0 Hz
- Duration: 5400 s
- Current scale: 0.06103515625000001 pA per int16 unit
- Voltage scale: 0.0625 mV per int16 unit
- Raw signal volume across both arrays: 2.41 GiB

## Object Layout

| Array | Shape | Chunks | Shards | Data objects | Compressed size | Largest object |
| --- | --- | --- | --- | ---: | ---: | ---: |
| current_data | [48, 13500000] | [1, 50000] | [1, 1600000] | 432 | 653.42 MiB | 2.16 MiB |
| voltage_data | [48, 13500000] | [1, 300000] | [1, 9600000] | 96 | 1.33 MiB | 20.00 KiB |

Interpretation: `current_data` dominates both compressed bytes and object-count fan-out, so it should be the only array on the critical latency path. `voltage_data` is cheap enough to treat as secondary metadata for the viewer.

## Benchmark Results

| Scenario | Mode | Samples/ch | Payload | Est. objects | Cold ms | Warm avg ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1s | single_channel | 2500 | 4.88 KiB | 1 | 3.737 | 1.146 |
| 1s | all_channels | 2500 | 234.38 KiB | 48 | 36.699 | 27.346 |
| 10s | single_channel | 25000 | 48.83 KiB | 1 | 1.073 | 1.124 |
| 10s | all_channels | 25000 | 2.29 MiB | 48 | 32.931 | 27.477 |
| 60s | single_channel | 150000 | 292.97 KiB | 1 | 1.675 | 1.695 |
| 60s | all_channels | 150000 | 13.73 MiB | 48 | 60.285 | 54.754 |
| 5min | single_channel | 750000 | 1.43 MiB | 1 | 5.82 | 6.04 |
| 5min | all_channels | 750000 | 68.66 MiB | 48 | 221.236 | 227.19 |
| cross_shard | single_channel | 100000 | 195.31 KiB | 2 | 2.192 | 1.781 |
| cross_shard | all_channels | 100000 | 9.16 MiB | 96 | 65.302 | 62.813 |

Interpretation:
- Raw 48-channel reads stay modest for the 1-second window but reach 68.66 MiB for 5 minutes, which is already too heavy for a responsive viewport refresh.
- A full-recording raw fetch would move 1.21 GiB; a 1200 px min/max envelope keeps that to 225.00 KiB.
- Crossing shard boundaries is manageable for a single channel but grows quickly for all-channel reads because each channel lives in separate time shards.

## Recommended Viewer Delivery Model

Service contracts:
- `GET /recordings/{id}/metadata`
- `GET /recordings/{id}/overview?start=<sample>&end=<sample>&channels=<list>&width_px=<n>`
- `GET /recordings/{id}/detail?start=<sample>&end=<sample>&channels=<list>&max_points=<n>`

Decision rules:
- Use raw samples only when the requested window is at or below 1.2 s per channel (3000 samples at 2.5 samples/pixel).
- Switch to precomputed or on-demand min/max envelopes for broader windows to cap payload size and improve cacheability.
- Optimize current_data first; treat voltage_data as metadata-rich but latency-insensitive.

Viewport math at 1200 px:
- `1s`: 2.08 samples/pixel, raw 48-channel payload 234.38 KiB, envelope payload 225.00 KiB, recommended `raw`.
- `10s`: 20.83 samples/pixel, raw 48-channel payload 2.29 MiB, envelope payload 225.00 KiB, recommended `envelope`.
- `60s`: 125.00 samples/pixel, raw 48-channel payload 13.73 MiB, envelope payload 225.00 KiB, recommended `envelope`.
- `5min`: 625.00 samples/pixel, raw 48-channel payload 68.66 MiB, envelope payload 225.00 KiB, recommended `envelope`.
- `cross_shard`: 83.33 samples/pixel, raw 48-channel payload 9.16 MiB, envelope payload 225.00 KiB, recommended `envelope`.

## AWS Deployment Recommendation

- S3 for canonical Zarr stores and precomputed overview pyramids.
- CloudFront for the browser bundle and cacheable overview responses.
- FastAPI on ECS Fargate behind an ALB for metadata and detail-window reads.
- Background jobs on ECS tasks or AWS Batch to build and refresh min/max pyramid levels.

Data-delivery responsibilities:
- GET /recordings/{id}/metadata returns recording dimensions, scales, duration, and overview levels.
- GET /recordings/{id}/overview returns min/max envelopes sized to the viewport width and can be cached aggressively.
- GET /recordings/{id}/detail returns raw int16 samples for narrow windows only and preserves the prior viewport until replacement data arrives.

Key risks:
- Cross-shard all-channel reads fan out into many object fetches because current_data is sharded by channel and time.
- Generating overview envelopes on every pan or zoom will add cost and tail latency unless higher levels are precomputed and cached.
- The UI must retain the previous frame until the next payload is ready or users will see blank redraws during navigation.

## Phased Implementation Plan

1. V1: metadata endpoint, detail reads, one overview envelope level, and optimistic client swap without blank states.
2. V2: multiresolution pyramid generation, cache headers, and prefetching adjacent overview/detail windows.
3. V3: observability, backpressure controls, SLO-driven tuning, and admission limits for expensive multi-channel queries.
