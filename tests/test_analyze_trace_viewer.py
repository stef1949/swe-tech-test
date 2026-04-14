from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import zarr
from zarr.codecs import BloscCodec, BloscShuffle

import analyze_trace_viewer


def create_fixture(recording_path: Path) -> Path:
    store = zarr.storage.LocalStore(str(recording_path))
    root = zarr.create_group(store=store, zarr_format=3)
    root.attrs.update(
        {
            "device_id": "fixture-4ch-001",
            "number_of_channels": 4,
            "sample_rate_hz": 10.0,
            "duration_sec": 400,
            "current_units": "pA",
            "current_scale": 0.125,
            "current_offset": 0,
            "voltage_scale": 0.25,
            "voltage_offset": 0,
        }
    )

    current_arr = root.create_array(
        name="current_data",
        shape=(4, 4000),
        dtype="int16",
        chunks=(1, 50),
        shards=(1, 1000),
        compressors=BloscCodec(
            cname="zstd",
            clevel=1,
            shuffle=BloscShuffle.shuffle,
            typesize=2,
        ),
    )
    voltage_arr = root.create_array(
        name="voltage_data",
        shape=(4, 4000),
        dtype="int16",
        chunks=(1, 100),
        shards=(1, 2000),
        compressors=BloscCodec(
            cname="zstd",
            clevel=1,
            shuffle=BloscShuffle.shuffle,
            typesize=2,
        ),
    )

    current_data = np.arange(4 * 4000, dtype=np.int16).reshape(4, 4000)
    voltage_data = np.full((4, 4000), 720, dtype=np.int16)
    current_arr[:] = current_data
    voltage_arr[:] = voltage_data
    return recording_path


class AnalyzeTraceViewerTests(unittest.TestCase):
    def test_count_segments_touched(self) -> None:
        self.assertEqual(analyze_trace_viewer.count_segments_touched(0, 50, 50), 1)
        self.assertEqual(analyze_trace_viewer.count_segments_touched(0, 51, 50), 2)
        self.assertEqual(analyze_trace_viewer.count_segments_touched(950, 1050, 1000), 2)

    def test_build_viewport_models_prefers_raw_only_for_narrow_windows(self) -> None:
        dataset = {
            "sample_rate_hz": 1000.0,
            "number_of_channels": 4,
            "shape": [4, 500_000],
        }
        benchmarks = [
            {
                "scenario": "1s",
                "samples_per_channel": 1000,
                "seconds": 1.0,
                "samples_per_pixel": 0.833,
            },
            {
                "scenario": "10s",
                "samples_per_channel": 10_000,
                "seconds": 10.0,
                "samples_per_pixel": 8.333,
            },
        ]
        current_layout = {"dtype_itemsize": 2}

        derived = analyze_trace_viewer.build_viewport_models(
            dataset=dataset,
            benchmarks=benchmarks,
            current_layout=current_layout,
            width_px=1200,
        )

        self.assertEqual(derived["raw_detail_cutoff_samples_per_channel"], 3000)
        models = {item["scenario"]: item for item in derived["window_models"]}
        self.assertEqual(models["1s"]["recommended_mode"], "raw")
        self.assertEqual(models["10s"]["recommended_mode"], "envelope")

    def test_ensure_recording_exists_calls_generator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = Path(tmpdir) / "missing.zarr"
            with mock.patch.object(
                analyze_trace_viewer, "generate_recording"
            ) as mocked_generate:
                analyze_trace_viewer.ensure_recording_exists(
                    missing_path, generate_if_missing=True
                )

        mocked_generate.assert_called_once_with(missing_path)

    def test_build_metrics_collects_layout_and_benchmarks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            metrics = analyze_trace_viewer.build_metrics(
                recording_path=recording_path,
                cloud="aws",
                warm_runs=1,
                width_px=1200,
            )

        self.assertEqual(metrics["dataset"]["shape"], [4, 4000])
        self.assertEqual(
            metrics["layout"]["current_data"]["data_object_count"],
            16,
        )
        self.assertEqual(
            metrics["layout"]["voltage_data"]["data_object_count"],
            8,
        )
        scenarios = {item["scenario"] for item in metrics["benchmarks"]}
        self.assertTrue({"1s", "10s", "60s", "5min", "cross_shard"}.issubset(scenarios))
        self.assertEqual(metrics["recommendation"]["cloud"], "AWS")

    def test_render_report_includes_required_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            metrics = analyze_trace_viewer.build_metrics(
                recording_path=recording_path,
                cloud="aws",
                warm_runs=0,
                width_px=1200,
            )
            report = analyze_trace_viewer.render_report(metrics)

        self.assertIn("# Trace Viewer Analysis", report)
        self.assertIn("## Recording Facts", report)
        self.assertIn("## Object Layout", report)
        self.assertIn("## Benchmark Results", report)
        self.assertIn("## Recommended Viewer Delivery Model", report)
        self.assertIn("## AWS Deployment Recommendation", report)

    def test_run_analysis_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            output_dir = Path(tmpdir) / "artifacts"

            metrics, report = analyze_trace_viewer.run_analysis(
                input_path=recording_path,
                output_dir=output_dir,
                cloud="aws",
                warm_runs=1,
                width_px=1200,
            )

            metrics_path = output_dir / "metrics.json"
            report_path = output_dir / "report.md"
            self.assertTrue(metrics_path.exists())
            self.assertTrue(report_path.exists())
            self.assertIn("recommendation", metrics)
            self.assertIn("Trace Viewer Analysis", report)

            loaded_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(loaded_metrics["dataset"]["device_id"], "fixture-4ch-001")
            self.assertIn("## Benchmark Results", report_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
