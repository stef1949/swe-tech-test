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

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

Available endpoints:

- `GET /api/metadata`
- `GET /api/overview?start=<sample>&end=<sample>&channels=<list>&width_px=<n>`
- `GET /api/detail?start=<sample>&end=<sample>&channels=<list>&width_px=<n>`

The viewer defaults to the first six channels, keeps the previous detail frame visible while new requests are loading, uses envelopes for the full-recording overview, and switches the detail pane between raw samples and min/max envelopes based on samples per pixel.
