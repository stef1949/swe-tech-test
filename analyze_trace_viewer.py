from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import zarr

DEFAULT_INPUT = Path(__file__).parent / "mock48_2500hz_1.5h.zarr"
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "artifacts"
DEFAULT_VIEWPORT_WIDTH_PX = 1200
DEFAULT_WARM_RUNS = 3
RAW_DETAIL_MAX_SAMPLES_PER_PIXEL = 2.5
SECONDS_TO_BENCHMARK = (
    ("1s", 1),
    ("10s", 10),
    ("60s", 60),
    ("5min", 5 * 60),
)

CLOUD_ARCHITECTURE = {
    "aws": {
        "cloud": "AWS",
        "storage": "S3 for canonical Zarr stores and precomputed overview pyramids.",
        "edge": "CloudFront for the browser bundle and cacheable overview responses.",
        "compute": "FastAPI on ECS Fargate behind an ALB for metadata and detail-window reads.",
        "batch": "Background jobs on ECS tasks or AWS Batch to build and refresh min/max pyramid levels.",
    },
    "gcp": {
        "cloud": "GCP",
        "storage": "Cloud Storage for canonical Zarr stores and precomputed overview pyramids.",
        "edge": "Cloud CDN for static assets and cacheable overview responses.",
        "compute": "FastAPI on Cloud Run for metadata and detail-window reads.",
        "batch": "Cloud Run jobs or Batch for pyramid generation and backfills.",
    },
    "azure": {
        "cloud": "Azure",
        "storage": "Blob Storage for canonical Zarr stores and precomputed overview pyramids.",
        "edge": "Azure Front Door for static assets and cacheable overview responses.",
        "compute": "FastAPI on Azure Container Apps for metadata and detail-window reads.",
        "batch": "Container Apps jobs or Azure Batch for pyramid generation and backfills.",
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect a trace-viewer recording and emit analysis artifacts."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to the recording Zarr directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where report.md and metrics.json will be written.",
    )
    parser.add_argument(
        "--generate-if-missing",
        action="store_true",
        help="Generate the default mock recording if the input path is missing.",
    )
    parser.add_argument(
        "--cloud",
        choices=sorted(CLOUD_ARCHITECTURE),
        default="aws",
        help="Cloud provider to target in the generated recommendation.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_WARM_RUNS,
        help="Number of warm benchmark runs after the cold pass.",
    )
    return parser.parse_args(argv)


def generate_recording(input_path: Path) -> None:
    from generate_mock_recording import main as generate_mock_recording_main

    generate_mock_recording_main(input_path)


def ensure_recording_exists(input_path: Path, generate_if_missing: bool) -> None:
    if input_path.exists():
        return
    if not generate_if_missing:
        raise SystemExit(
            f"Input recording does not exist: {input_path}. "
            "Pass --generate-if-missing to create it."
        )

    input_path.parent.mkdir(parents=True, exist_ok=True)
    generate_recording(input_path)


def load_root_attrs(root: zarr.Group) -> dict[str, Any]:
    return {str(key): root.attrs[key] for key in root.attrs.keys()}


def load_array_metadata(array_dir: Path) -> dict[str, Any]:
    metadata_path = array_dir / "zarr.json"
    if not metadata_path.exists():
        return {}
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def count_segments_touched(start: int, end: int, segment_size: int | None) -> int:
    if segment_size is None or segment_size <= 0 or end <= start:
        return 0
    return ((end - 1) // segment_size) - (start // segment_size) + 1


def format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    return f"{size:.2f} {unit}"


def slice_payload_bytes(samples: int, channels: int, dtype_size: int) -> int:
    return samples * channels * dtype_size


def envelope_payload_bytes(width_px: int, samples: int, channels: int, dtype_size: int) -> int:
    bucket_count = min(width_px, samples)
    return bucket_count * channels * 2 * dtype_size


def build_benchmark_cases(
    sample_rate_hz: float,
    n_samples: int,
    shard_size: int | None,
) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for label, seconds in SECONDS_TO_BENCHMARK:
        requested_samples = max(1, int(round(seconds * sample_rate_hz)))
        actual_samples = min(requested_samples, n_samples)
        cases.append(
            {
                "name": label,
                "start": 0,
                "end": actual_samples,
                "requested_seconds": seconds,
            }
        )

    if shard_size:
        cross_half_width = max(1, min(shard_size // 20, int(sample_rate_hz * 20)))
        cross_start = max(0, shard_size - cross_half_width)
        cross_end = min(n_samples, shard_size + cross_half_width)
        if cross_end > cross_start:
            cases.append(
                {
                    "name": "cross_shard",
                    "start": cross_start,
                    "end": cross_end,
                    "requested_seconds": None,
                }
            )

    return cases


def timed_slice_read(
    arr: zarr.Array,
    start: int,
    end: int,
    all_channels: bool,
) -> tuple[float, np.ndarray]:
    read_start = time.perf_counter()
    data = arr[:, start:end] if all_channels else arr[0, start:end]
    materialized = np.asarray(data)
    elapsed_ms = (time.perf_counter() - read_start) * 1000
    return elapsed_ms, materialized


def summarize_timings(elapsed_ms: list[float]) -> dict[str, float]:
    if not elapsed_ms:
        return {"average_ms": 0.0, "median_ms": 0.0, "min_ms": 0.0, "max_ms": 0.0}
    return {
        "average_ms": round(statistics.fmean(elapsed_ms), 3),
        "median_ms": round(statistics.median(elapsed_ms), 3),
        "min_ms": round(min(elapsed_ms), 3),
        "max_ms": round(max(elapsed_ms), 3),
    }


def benchmark_current_array(
    arr: zarr.Array,
    sample_rate_hz: float,
    warm_runs: int,
    width_px: int,
) -> list[dict[str, Any]]:
    shard_size = arr.shards[-1] if arr.shards else None
    chunk_size = arr.chunks[-1] if arr.chunks else None
    dtype_size = arr.dtype.itemsize
    cases = build_benchmark_cases(sample_rate_hz, arr.shape[-1], shard_size)
    results: list[dict[str, Any]] = []

    for case in cases:
        start = case["start"]
        end = case["end"]
        sample_count = end - start
        shared = {
            "scenario": case["name"],
            "start_sample": start,
            "end_sample": end,
            "samples_per_channel": sample_count,
            "seconds": round(sample_count / sample_rate_hz, 3),
            "samples_per_pixel": round(sample_count / width_px, 3),
            "time_chunks_touched": count_segments_touched(start, end, chunk_size),
            "time_shards_touched": count_segments_touched(start, end, shard_size),
        }

        mode_results: dict[str, Any] = {}
        for mode_name, all_channels in (
            ("single_channel", False),
            ("all_channels", True),
        ):
            cold_ms, cold_data = timed_slice_read(arr, start, end, all_channels)
            warm_ms = [
                round(timed_slice_read(arr, start, end, all_channels)[0], 3)
                for _ in range(warm_runs)
            ]
            channels_read = arr.shape[0] if all_channels else 1
            mode_results[mode_name] = {
                "channels_read": channels_read,
                "raw_payload_bytes": slice_payload_bytes(
                    sample_count, channels_read, dtype_size
                ),
                "estimated_object_reads": shared["time_shards_touched"] * channels_read,
                "estimated_logical_chunks": shared["time_chunks_touched"] * channels_read,
                "cold_ms": round(cold_ms, 3),
                "warm_runs_ms": warm_ms,
                "warm_summary_ms": summarize_timings(warm_ms),
                "materialized_shape": list(cold_data.shape),
            }

        results.append({**shared, "modes": mode_results})

    return results


def analyze_array_layout(recording_path: Path, name: str, arr: zarr.Array) -> dict[str, Any]:
    array_dir = recording_path / name
    files = sorted(path for path in array_dir.rglob("*") if path.is_file())
    metadata_files = [path for path in files if path.name.endswith(".json")]
    data_files = [path for path in files if path not in metadata_files]
    data_sizes = [path.stat().st_size for path in data_files]
    compressed_bytes = sum(path.stat().st_size for path in files)
    raw_bytes = int(np.prod(arr.shape) * arr.dtype.itemsize)
    metadata = load_array_metadata(array_dir)
    codecs = metadata.get("codecs", [])

    return {
        "path": str(array_dir),
        "shape": list(arr.shape),
        "dtype": str(arr.dtype),
        "dtype_itemsize": arr.dtype.itemsize,
        "chunks": list(arr.chunks) if arr.chunks else None,
        "shards": list(arr.shards) if arr.shards else None,
        "logical_chunks_per_channel": math.ceil(arr.shape[-1] / arr.chunks[-1])
        if arr.chunks
        else None,
        "shards_per_channel": math.ceil(arr.shape[-1] / arr.shards[-1])
        if arr.shards
        else None,
        "raw_bytes": raw_bytes,
        "compressed_bytes": compressed_bytes,
        "compression_ratio": round(raw_bytes / compressed_bytes, 3)
        if compressed_bytes
        else None,
        "file_count": len(files),
        "metadata_file_count": len(metadata_files),
        "data_object_count": len(data_files),
        "largest_data_object_bytes": max(data_sizes) if data_sizes else 0,
        "smallest_data_object_bytes": min(data_sizes) if data_sizes else 0,
        "average_data_object_bytes": round(statistics.fmean(data_sizes), 3)
        if data_sizes
        else 0.0,
        "codecs": codecs,
    }


def build_viewport_models(
    dataset: dict[str, Any],
    benchmarks: list[dict[str, Any]],
    current_layout: dict[str, Any],
    width_px: int,
) -> dict[str, Any]:
    sample_rate_hz = float(dataset["sample_rate_hz"])
    n_channels = int(dataset["number_of_channels"])
    n_samples = int(dataset["shape"][1])
    dtype_size = int(current_layout["dtype_itemsize"])
    raw_cutoff_samples = int(width_px * RAW_DETAIL_MAX_SAMPLES_PER_PIXEL)

    windows = []
    for benchmark in benchmarks:
        sample_count = int(benchmark["samples_per_channel"])
        raw_single = slice_payload_bytes(sample_count, 1, dtype_size)
        raw_all = slice_payload_bytes(sample_count, n_channels, dtype_size)
        envelope_single = envelope_payload_bytes(width_px, sample_count, 1, dtype_size)
        envelope_all = envelope_payload_bytes(width_px, sample_count, n_channels, dtype_size)
        windows.append(
            {
                "scenario": benchmark["scenario"],
                "samples_per_channel": sample_count,
                "seconds": benchmark["seconds"],
                "samples_per_pixel": benchmark["samples_per_pixel"],
                "raw_payload_bytes_single_channel": raw_single,
                "raw_payload_bytes_all_channels": raw_all,
                "envelope_payload_bytes_single_channel": envelope_single,
                "envelope_payload_bytes_all_channels": envelope_all,
                "payload_reduction_ratio_all_channels": round(raw_all / envelope_all, 3)
                if envelope_all
                else None,
                "recommended_mode": "raw"
                if sample_count <= raw_cutoff_samples
                else "envelope",
            }
        )

    full_raw_all = slice_payload_bytes(n_samples, n_channels, dtype_size)
    full_envelope_all = envelope_payload_bytes(width_px, n_samples, n_channels, dtype_size)

    return {
        "viewport_width_px": width_px,
        "raw_detail_cutoff_samples_per_channel": raw_cutoff_samples,
        "raw_detail_cutoff_seconds": round(raw_cutoff_samples / sample_rate_hz, 3),
        "raw_detail_cutoff_samples_per_pixel": RAW_DETAIL_MAX_SAMPLES_PER_PIXEL,
        "service_payload_encoding": "Binary int16 ADC counts plus scale metadata.",
        "window_models": windows,
        "full_recording_overview": {
            "samples_per_channel": n_samples,
            "seconds": round(n_samples / sample_rate_hz, 3),
            "samples_per_pixel": round(n_samples / width_px, 3),
            "raw_payload_bytes_all_channels": full_raw_all,
            "envelope_payload_bytes_all_channels": full_envelope_all,
            "payload_reduction_ratio_all_channels": round(
                full_raw_all / full_envelope_all, 3
            )
            if full_envelope_all
            else None,
        },
    }


def build_recommendation(cloud: str, derived: dict[str, Any]) -> dict[str, Any]:
    architecture = CLOUD_ARCHITECTURE[cloud]
    return {
        "cloud": architecture["cloud"],
        "architecture": [
            architecture["storage"],
            architecture["edge"],
            architecture["compute"],
            architecture["batch"],
        ],
        "data_delivery": [
            "GET /recordings/{id}/metadata returns recording dimensions, scales, duration, and overview levels.",
            "GET /recordings/{id}/overview returns min/max envelopes sized to the viewport width and can be cached aggressively.",
            "GET /recordings/{id}/detail returns raw int16 samples for narrow windows only and preserves the prior viewport until replacement data arrives.",
        ],
        "decision_rules": [
            f"Use raw samples only when the requested window is at or below {derived['raw_detail_cutoff_seconds']} s per channel "
            f"({derived['raw_detail_cutoff_samples_per_channel']} samples at {derived['raw_detail_cutoff_samples_per_pixel']} samples/pixel).",
            "Switch to precomputed or on-demand min/max envelopes for broader windows to cap payload size and improve cacheability.",
            "Optimize current_data first; treat voltage_data as metadata-rich but latency-insensitive.",
        ],
        "risks": [
            "Cross-shard all-channel reads fan out into many object fetches because current_data is sharded by channel and time.",
            "Generating overview envelopes on every pan or zoom will add cost and tail latency unless higher levels are precomputed and cached.",
            "The UI must retain the previous frame until the next payload is ready or users will see blank redraws during navigation.",
        ],
        "phases": [
            "V1: metadata endpoint, detail reads, one overview envelope level, and optimistic client swap without blank states.",
            "V2: multiresolution pyramid generation, cache headers, and prefetching adjacent overview/detail windows.",
            "V3: observability, backpressure controls, SLO-driven tuning, and admission limits for expensive multi-channel queries.",
        ],
    }


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    return value


def build_metrics(
    recording_path: Path,
    cloud: str,
    warm_runs: int,
    width_px: int,
) -> dict[str, Any]:
    root = zarr.open(str(recording_path), mode="r")
    current_arr = root["current_data"]
    voltage_arr = root["voltage_data"]
    attrs = load_root_attrs(root)

    current_layout = analyze_array_layout(recording_path, "current_data", current_arr)
    voltage_layout = analyze_array_layout(recording_path, "voltage_data", voltage_arr)
    dataset = {
        "recording_path": str(recording_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "device_id": attrs.get("device_id"),
        "shape": list(current_arr.shape),
        "number_of_channels": int(attrs.get("number_of_channels", current_arr.shape[0])),
        "sample_rate_hz": float(attrs.get("sample_rate_hz")),
        "duration_sec": int(attrs.get("duration_sec")),
        "current_units": attrs.get("current_units"),
        "current_scale": float(attrs.get("current_scale")),
        "current_offset": int(attrs.get("current_offset", 0)),
        "voltage_scale": float(attrs.get("voltage_scale")),
        "voltage_offset": int(attrs.get("voltage_offset", 0)),
        "current_raw_bytes": current_layout["raw_bytes"],
        "voltage_raw_bytes": voltage_layout["raw_bytes"],
        "total_raw_bytes": current_layout["raw_bytes"] + voltage_layout["raw_bytes"],
    }
    benchmarks = benchmark_current_array(
        current_arr,
        sample_rate_hz=dataset["sample_rate_hz"],
        warm_runs=warm_runs,
        width_px=width_px,
    )
    derived = build_viewport_models(dataset, benchmarks, current_layout, width_px)
    recommendation = build_recommendation(cloud, derived)

    return json_safe(
        {
            "dataset": dataset,
            "layout": {
                "current_data": current_layout,
                "voltage_data": voltage_layout,
            },
            "benchmarks": benchmarks,
            "derived": derived,
            "recommendation": recommendation,
        }
    )


def benchmark_table_rows(benchmarks: list[dict[str, Any]]) -> str:
    rows = [
        "| Scenario | Mode | Samples/ch | Payload | Est. objects | Cold ms | Warm avg ms |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for benchmark in benchmarks:
        for mode_name, mode in benchmark["modes"].items():
            rows.append(
                "| {scenario} | {mode_name} | {samples} | {payload} | {objects} | {cold_ms} | {warm_avg} |".format(
                    scenario=benchmark["scenario"],
                    mode_name=mode_name,
                    samples=benchmark["samples_per_channel"],
                    payload=format_bytes(mode["raw_payload_bytes"]),
                    objects=mode["estimated_object_reads"],
                    cold_ms=mode["cold_ms"],
                    warm_avg=mode["warm_summary_ms"]["average_ms"],
                )
            )
    return "\n".join(rows)


def render_report(metrics: dict[str, Any]) -> str:
    dataset = metrics["dataset"]
    current_layout = metrics["layout"]["current_data"]
    voltage_layout = metrics["layout"]["voltage_data"]
    benchmarks = metrics["benchmarks"]
    derived = metrics["derived"]
    recommendation = metrics["recommendation"]
    full_overview = derived["full_recording_overview"]

    layout_rows = [
        "| Array | Shape | Chunks | Shards | Data objects | Compressed size | Largest object |",
        "| --- | --- | --- | --- | ---: | ---: | ---: |",
        "| current_data | {shape} | {chunks} | {shards} | {objects} | {size} | {largest} |".format(
            shape=current_layout["shape"],
            chunks=current_layout["chunks"],
            shards=current_layout["shards"],
            objects=current_layout["data_object_count"],
            size=format_bytes(current_layout["compressed_bytes"]),
            largest=format_bytes(current_layout["largest_data_object_bytes"]),
        ),
        "| voltage_data | {shape} | {chunks} | {shards} | {objects} | {size} | {largest} |".format(
            shape=voltage_layout["shape"],
            chunks=voltage_layout["chunks"],
            shards=voltage_layout["shards"],
            objects=voltage_layout["data_object_count"],
            size=format_bytes(voltage_layout["compressed_bytes"]),
            largest=format_bytes(voltage_layout["largest_data_object_bytes"]),
        ),
    ]

    window_lines = []
    for model in derived["window_models"]:
        window_lines.append(
            "- `{scenario}`: {spp:.2f} samples/pixel, raw 48-channel payload {raw}, envelope payload {env}, recommended `{mode}`.".format(
                scenario=model["scenario"],
                spp=model["samples_per_pixel"],
                raw=format_bytes(model["raw_payload_bytes_all_channels"]),
                env=format_bytes(model["envelope_payload_bytes_all_channels"]),
                mode=model["recommended_mode"],
            )
        )

    return "\n".join(
        [
            "# Trace Viewer Analysis",
            "",
            f"Generated from `{dataset['recording_path']}` on {dataset['generated_at_utc']}.",
            "",
            "## Recording Facts",
            "",
            f"- Device: `{dataset['device_id']}`",
            f"- Channels: {dataset['number_of_channels']}",
            f"- Sample rate: {dataset['sample_rate_hz']} Hz",
            f"- Duration: {dataset['duration_sec']} s",
            f"- Current scale: {dataset['current_scale']} {dataset['current_units']} per int16 unit",
            f"- Voltage scale: {dataset['voltage_scale']} mV per int16 unit",
            f"- Raw signal volume across both arrays: {format_bytes(dataset['total_raw_bytes'])}",
            "",
            "## Object Layout",
            "",
            *layout_rows,
            "",
            "Interpretation: `current_data` dominates both compressed bytes and object-count fan-out, so it should be the only array on the critical latency path. `voltage_data` is cheap enough to treat as secondary metadata for the viewer.",
            "",
            "## Benchmark Results",
            "",
            benchmark_table_rows(benchmarks),
            "",
            "Interpretation:",
            f"- Raw 48-channel reads stay modest for the 1-second window but reach {format_bytes(next(item for item in derived['window_models'] if item['scenario'] == '5min')['raw_payload_bytes_all_channels'])} for 5 minutes, which is already too heavy for a responsive viewport refresh.",
            f"- A full-recording raw fetch would move {format_bytes(full_overview['raw_payload_bytes_all_channels'])}; a 1200 px min/max envelope keeps that to {format_bytes(full_overview['envelope_payload_bytes_all_channels'])}.",
            "- Crossing shard boundaries is manageable for a single channel but grows quickly for all-channel reads because each channel lives in separate time shards.",
            "",
            "## Recommended Viewer Delivery Model",
            "",
            "Service contracts:",
            "- `GET /recordings/{id}/metadata`",
            "- `GET /recordings/{id}/overview?start=<sample>&end=<sample>&channels=<list>&width_px=<n>`",
            "- `GET /recordings/{id}/detail?start=<sample>&end=<sample>&channels=<list>&max_points=<n>`",
            "",
            "Decision rules:",
            *[f"- {item}" for item in recommendation["decision_rules"]],
            "",
            "Viewport math at 1200 px:",
            *window_lines,
            "",
            "## {cloud} Deployment Recommendation".format(cloud=recommendation["cloud"]),
            "",
            *[f"- {item}" for item in recommendation["architecture"]],
            "",
            "Data-delivery responsibilities:",
            *[f"- {item}" for item in recommendation["data_delivery"]],
            "",
            "Key risks:",
            *[f"- {item}" for item in recommendation["risks"]],
            "",
            "## Phased Implementation Plan",
            "",
            "1. {item}".format(item=recommendation["phases"][0]),
            "2. {item}".format(item=recommendation["phases"][1]),
            "3. {item}".format(item=recommendation["phases"][2]),
            "",
        ]
    )


def write_outputs(output_dir: Path, metrics: dict[str, Any], report: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "metrics.json"
    report_path = output_dir / "report.md"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    report_path.write_text(report, encoding="utf-8")
    return metrics_path, report_path


def run_analysis(
    input_path: Path,
    output_dir: Path,
    cloud: str,
    warm_runs: int,
    width_px: int = DEFAULT_VIEWPORT_WIDTH_PX,
) -> tuple[dict[str, Any], str]:
    metrics = build_metrics(
        recording_path=input_path,
        cloud=cloud,
        warm_runs=warm_runs,
        width_px=width_px,
    )
    report = render_report(metrics)
    write_outputs(output_dir, metrics, report)
    return metrics, report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.runs < 0:
        raise SystemExit("--runs must be 0 or greater.")

    input_path = args.input.resolve()
    output_dir = args.output_dir.resolve()
    ensure_recording_exists(input_path, args.generate_if_missing)
    metrics, _ = run_analysis(
        input_path=input_path,
        output_dir=output_dir,
        cloud=args.cloud,
        warm_runs=args.runs,
    )
    print(f"Wrote {output_dir / 'metrics.json'}")
    print(f"Wrote {output_dir / 'report.md'}")
    print(
        "Raw detail cutoff: "
        f"{metrics['derived']['raw_detail_cutoff_seconds']} s per channel at "
        f"{metrics['derived']['raw_detail_cutoff_samples_per_pixel']} samples/pixel."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
