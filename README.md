# SWE Tech Test — Mock Recording

See `TEST_DESCRIPTION.md` for the exercise brief.

The mock recording is not included. Generate it locally with:

```bash
pip install zarr numpy
py generate_mock_recording.py
```

Output: `./mock48_2500hz_1.5h.zarr` — a **Zarr v3** directory store,
48 channels × 2.5 kHz × 1.5 h (= 13,500,000 samples per channel), `int16`.
Generation takes ~1 min on an M-series Mac.

## What's in the file

### Structure

```
mock48_2500hz_1.5h.zarr/
├── zarr.json               # group metadata + root attrs (see below)
├── current_data/           # (48, 13_500_000) int16 — ADC counts
└── voltage_data/           # (48, 13_500_000) int16 — ADC counts
```

### Root attributes

| Attr | Value | Meaning |
|---|---|---|
| `device_id` | `mock-48ch-001` | |
| `number_of_channels` | `48` | |
| `sample_rate_hz` | `2500.0` | |
| `duration_sec` | `5400` | = 1.5 h (13,500,000 samples per channel) |
| `current_units` | `"pA"` | |
| `current_range` | `2.0` | ± 2 nA full scale |
| `current_scale` | `0.061035…` | **pA per int16 unit** (= 2 nA / 32768) |
| `current_offset` | `0` | |
| `voltage_scale` | `0.0625` | **mV per int16 unit** |
| `voltage_offset` | `0` | |

To convert raw counts to physical units:
```python
current_pA = current_data[ch, a:b].astype("float32") * 0.06103515625
voltage_mV = voltage_data[ch, a:b].astype("float32") * 0.0625
```

## Reading the file

zarr-python:
```python
import zarr, numpy as np
f = zarr.open("mock48_2500hz_1.5h.zarr", mode="r")
pA = f["current_data"][0, 0:25000].astype("float32") * f.attrs["current_scale"]
```

## Analysis script

Generate the recording if needed and write the engineering-spec support artifacts:

```bash
py analyze_trace_viewer.py --generate-if-missing
```

Write results to a custom directory or point at a recording generated elsewhere:

```bash
py analyze_trace_viewer.py --input C:\path\to\mock48_2500hz_1.5h.zarr --output-dir C:\path\to\artifacts
```

This creates:

- `artifacts/metrics.json` for machine-readable layout, benchmark, and recommendation data
- `artifacts/report.md` for a human-readable summary you can reuse in the written engineering spec

Run the test suite with:

```bash
py -m unittest discover -s tests
```

## Viewer prototype

Start the browser-based trace viewer:

```bash
py trace_viewer_server.py --generate-if-missing
```

To precompute and persist the envelope pyramid sidecar without starting the server:

```bash
py trace_viewer_server.py --generate-if-missing --build-pyramid-only
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

To bind to a non-loopback interface such as `0.0.0.0`, pass `--allow-remote` explicitly:

```bash
 py trace_viewer_server.py --host 0.0.0.0 --allow-remote
```

The viewer defaults to the first six channels, keeps the previous detail frame visible while new requests are loading, uses envelopes for the full-recording overview, and switches the detail pane between raw samples and min/max envelopes based on samples per pixel.

## API summary

Primary endpoints:

- `GET /v1/recordings/active/metadata`
- `GET /v1/recordings/{recording_id}/metadata`
- `GET /v1/recordings/{recording_id}/revisions/{revision}/overview?channels=<list>&viewport_px=<n>`
- `GET /v1/recordings/{recording_id}/revisions/{revision}/detail?start_sample=<sample>&end_sample=<sample>&channels=<list>&viewport_px=<n>&representation=auto|raw|envelope`
- `GET /v1/recordings/{recording_id}/revisions/{revision}/envelope-tiles/{level_index}/{tile_index}?channels=<list>`
- `GET /livez`
- `GET /readyz`
- `GET /metrics`

Legacy compatibility aliases remain available for the prototype UI:

- `GET /api/metadata`
- `GET /api/overview?channels=<list>&viewport_px=<n>`
- `GET /api/detail?start_sample=<sample>&end_sample=<sample>&channels=<list>&viewport_px=<n>`

### `GET /v1/recordings/{recording_id}/metadata`

Returns recording-level metadata plus the canonical `revisioned_api_base` the browser should use for data reads.

Important fields:

- `recording_id`
- `revision`
- `channels`
- `total_samples`
- `sample_rate_hz`
- `duration_sec`
- `current_scale`, `current_offset`, `current_units`
- `voltage_scale`, `voltage_offset`, `voltage_units`
- `channel_voltage_mv`
- `voltage_range_mv`
- `default_channels`
- `default_window`
- `detail_threshold`
- `limits`
- `pyramid`
- `links`

### `GET /v1/recordings/{recording_id}/revisions/{revision}/overview`

Returns a full-recording min/max envelope for the selected channels at the requested viewport width.

Query parameters:

- `channels`
- `viewport_px`

Behavior:

- always returns `mode: "envelope"`
- always covers the full recording span
- uses the overview pyramid
- is cacheable and revision-stable
- can be returned as JSON or binary depending on `Accept`

### `GET /v1/recordings/{recording_id}/revisions/{revision}/detail`

Returns the buffered detail window for the selected channels.

Query parameters:

- `start_sample`
- `end_sample`
- `channels`
- `viewport_px`
- `representation=auto|raw|envelope`

Behavior:

- `representation=auto` chooses raw only when the window stays under the raw density and payload limits
- `representation=raw` is rejected if the request is too wide
- `representation=envelope` forces min/max summaries
- narrow windows use `source: "raw_window"`
- moderate wide windows use `source: "envelope_slice"`
- very wide windows use `source: "envelope_pyramid"`

### `GET /v1/recordings/{recording_id}/revisions/{revision}/envelope-tiles/{level_index}/{tile_index}`

Returns a fixed envelope tile from the precomputed pyramid.

This endpoint exists to support CDN-friendly and cache-friendly zoomed-out views. Tiles are revisioned, globally aligned, and immutable for a given recording revision.

### Health and diagnostics

- `GET /livez` returns simple process liveness.
- `GET /readyz` returns readiness plus recording identity and pyramid status.
- `GET /metrics` returns Prometheus-style counters and gauges for requests, cache hits, bytes written, and detail-slot pressure.

## Transport, caching, and errors

### Content negotiation

Trace endpoints support:

- `Accept: application/json`
- `Accept: application/vnd.nanopore-trace.v1+binary`

The browser uses the binary media type for overview and detail reads to keep `int16` payloads compact. The binary payload format is still the custom `TVB1` container with a compact JSON header and packed `int16` body arrays.

### Cache behavior

- metadata responses include `ETag` and short-lived cache headers
- revisioned overview responses are immutable and aggressively cacheable
- revisioned envelope tiles are immutable and aggressively cacheable
- detail responses include `ETag` plus short browser-oriented cache headers
- conditional GETs using `If-None-Match` return `304 Not Modified`

### Error format

Validation and operational failures use `application/problem+json`.

Typical cases:

- invalid parameters
- windows outside recording bounds
- raw requests that are too wide
- responses that exceed configured size budgets
- overload on the detail path with `503` and `Retry-After`

## Server architecture

### What changed

The server is still a single-process Python HTTP service over one immutable Zarr recording, but it now behaves more like a production-shaped read API:

- the public surface is versioned and recording-scoped
- data reads are revisioned and cache-aware
- overview and envelope tiles are treated as immutable artifacts
- detail reads have explicit representation rules
- the service exposes readiness and metrics endpoints

### High-level architecture

```text
Browser UI
  -> metadata bootstrap
  -> revisioned overview, detail, and tile reads
Python HTTP server
  -> TraceViewerHandler
  -> TraceDataService
    -> Zarr arrays on disk
    -> persisted envelope-pyramid sidecar (.npz)
```

There is still no database, auth layer, or background worker in this repository. This remains a read-only prototype server for one recording file, but the API contract and caching semantics now match a more production-credible design.

### Core backend components

#### HTTP layer

`TraceViewerHandler` still subclasses `BaseHTTPRequestHandler`, and `TraceViewerServer` still subclasses `ThreadingHTTPServer`.

The HTTP layer now adds:

- versioned route parsing for `/v1/recordings/...`
- `Accept`-based binary negotiation
- `ETag` handling and `304` responses
- `application/problem+json` errors
- `livez`, `readyz`, and `metrics`
- bounded admission control for expensive detail requests

#### TraceDataService

`TraceDataService` still owns the loaded Zarr arrays and all request shaping logic, but it now also owns:

- recording id and revision generation
- strict request parsing for overview, detail, and tile reads
- explicit raw versus envelope strategy selection
- response budget enforcement
- envelope tile generation from fixed pyramid levels
- revision-aware cache keys

#### Overview pyramid and sidecar

The overview pyramid is no longer only an in-memory startup artifact.

The service now:

- loads a persisted `.trace-pyramid.npz` sidecar when it matches the current recording revision
- falls back to rebuilding the pyramid when the sidecar is missing, stale, or corrupted
- writes sidecars atomically so interrupted writes do not leave the server stuck on startup
- exposes `--build-pyramid-only` to precompute the sidecar ahead of time

The service still keeps a small in-process LRU cache for overview payloads, but the important change is that the canonical pyramid can survive process restarts.

### Request handling model

#### Metadata bootstrap

The browser boots from `/v1/recordings/active/metadata`, reads `revisioned_api_base`, and then issues all trace requests against revisioned URLs.

#### Overview

The browser requests a full-recording envelope at the overview canvas width. Overview reads are immutable for a given revision and are safe to cache aggressively.

#### Detail

The browser requests a buffered window larger than the visible span so pan and zoom can render from stale or buffered data while fresh reads are in flight.

The server returns:

- raw samples only when density and payload limits allow it
- an on-demand envelope slice for moderate windows
- a pyramid-backed envelope for very wide windows

#### Envelope tiles

Tiles are fixed, globally aligned slices of a given pyramid level. They are intended for cacheable, repeatable zoomed-out access patterns rather than arbitrary ad hoc windows.

## Frontend behavior

The frontend still prioritizes smooth interaction over immediate blank redraws.

It does this by:

- keeping old detail data visible during in-flight requests
- buffering detail reads beyond the current visible window
- throttling drag-triggered refreshes
- rendering directly from the buffered detail window when possible
- applying temporary canvas transforms during zoom before the next response lands

## Guard rails and limits

The server enforces:

- strict integer parsing for `start_sample`, `end_sample`, and `viewport_px` on the new API
- maximum viewport width
- maximum channel token count and query length
- maximum JSON point budgets
- maximum binary response sizes
- a density cutoff for raw rendering
- bounded concurrent detail work
- loopback-only binding unless `--allow-remote` is passed

## Short summary

The current server is still pragmatic single-process prototype, but has a production-shaped API structure: revisioned trace reads, explicit raw versus envelope behavior, immutable overview and tile resources, cache validators, diagnostics endpoints, sidecar-backed pyramid persistence, and a browser client that bootstraps from metadata and keeps stale traces visible while fresh requests are in flight.
