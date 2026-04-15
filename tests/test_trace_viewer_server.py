from __future__ import annotations

import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import zarr
from zarr.codecs import BloscCodec, BloscShuffle

import trace_viewer_server


def decode_trace_binary(payload: bytes) -> dict[str, object]:
    magic = payload[:4]
    if magic != trace_viewer_server.TRACE_BINARY_MAGIC:
        raise AssertionError("unexpected binary magic")

    header_length, payload_offset = struct.unpack("<II", payload[4:12])
    header = json.loads(payload[12 : 12 + header_length].decode("utf-8"))
    body = np.frombuffer(payload[payload_offset:], dtype="<i2")
    cursor = 0

    for trace in header["traces"]:
        length = trace["length"]
        if header["mode"] == "raw":
            trace["values"] = body[cursor : cursor + length].copy()
            cursor += length
        else:
            trace["mins"] = body[cursor : cursor + length].copy()
            cursor += length
            trace["maxs"] = body[cursor : cursor + length].copy()
            cursor += length
    return header


def create_fixture(
    recording_path: Path,
    *,
    current_dtype: str = "int16",
    voltage_dtype: str = "int16",
) -> Path:
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
        dtype=current_dtype,
        chunks=(1, 50),
        shards=(1, 1000),
        compressors=BloscCodec(
            cname="zstd",
            clevel=1,
            shuffle=BloscShuffle.shuffle,
            typesize=np.dtype(current_dtype).itemsize,
        ),
    )
    voltage_arr = root.create_array(
        name="voltage_data",
        shape=(4, 4000),
        dtype=voltage_dtype,
        chunks=(1, 100),
        shards=(1, 2000),
        compressors=BloscCodec(
            cname="zstd",
            clevel=1,
            shuffle=BloscShuffle.shuffle,
            typesize=np.dtype(voltage_dtype).itemsize,
        ),
    )

    current_data = np.arange(4 * 4000, dtype=np.int16).reshape(4, 4000).astype(current_dtype)
    voltage_data = np.full((4, 4000), 720, dtype=np.int16).astype(voltage_dtype)
    current_arr[:] = current_data
    voltage_arr[:] = voltage_data
    return recording_path


class TraceViewerServerTests(unittest.TestCase):
    def test_is_client_disconnect_error_matches_windows_abort(self) -> None:
        self.assertTrue(trace_viewer_server.is_client_disconnect_error(ConnectionAbortedError(10053, "aborted")))
        self.assertFalse(trace_viewer_server.is_client_disconnect_error(ValueError("boom")))

    def test_parse_int_param_accepts_fractional_values(self) -> None:
        self.assertEqual(trace_viewer_server.parse_int_param("10", "start"), 10)
        self.assertEqual(trace_viewer_server.parse_int_param("10.5", "start"), 11)
        self.assertEqual(trace_viewer_server.parse_int_param("10.4", "start"), 10)

    def test_parse_channel_list_defaults_to_first_visible_channels(self) -> None:
        self.assertEqual(trace_viewer_server.parse_channel_list(None, 4), [0, 1, 2, 3])
        self.assertEqual(trace_viewer_server.parse_channel_list("2,0,2", 4), [2, 0])

    def test_parse_channel_list_rejects_out_of_range_channel(self) -> None:
        with self.assertRaises(ValueError):
            trace_viewer_server.parse_channel_list("7", 4)

    def test_parse_channel_list_rejects_excessive_channel_tokens(self) -> None:
        with self.assertRaises(ValueError):
            trace_viewer_server.parse_channel_list(",".join(["0"] * 65), 4)

    def test_reduce_to_envelope_computes_min_and_max(self) -> None:
        data = np.array([3, 7, 2, 8, 4, 9], dtype=np.int16)
        mins, maxs = trace_viewer_server.reduce_to_envelope(data, 3)
        self.assertEqual(mins.tolist(), [3, 2, 4])
        self.assertEqual(maxs.tolist(), [7, 8, 9])

    def test_detail_mode_switches_at_density_threshold(self) -> None:
        self.assertEqual(trace_viewer_server.detail_mode_for_window(1000, 500), "raw")
        self.assertEqual(trace_viewer_server.detail_mode_for_window(5000, 500), "envelope")

    def test_validate_bind_host_rejects_non_loopback_without_allow_remote(self) -> None:
        trace_viewer_server.validate_bind_host("127.0.0.1", allow_remote=False)
        with self.assertRaises(SystemExit):
            trace_viewer_server.validate_bind_host("0.0.0.0", allow_remote=False)
        trace_viewer_server.validate_bind_host("0.0.0.0", allow_remote=True)

    def test_resolve_static_path_blocks_sibling_prefix_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            viewer_dir = root / "viewer"
            viewer_dir.mkdir()
            (viewer_dir / "index.html").write_text("ok", encoding="utf-8")
            secret_dir = root / "viewer-secrets"
            secret_dir.mkdir()
            (secret_dir / "secret.txt").write_text("leak", encoding="utf-8")

            with patch.object(trace_viewer_server, "VIEWER_DIR", viewer_dir):
                self.assertIsNone(trace_viewer_server.resolve_static_path("/../viewer-secrets/secret.txt"))
                self.assertEqual(
                    trace_viewer_server.resolve_static_path("/index.html"),
                    (viewer_dir / "index.html").resolve(),
                )

    def test_service_metadata_overview_and_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            service = trace_viewer_server.TraceDataService(recording_path)

            metadata = service.metadata()
            self.assertEqual(metadata["channels"], 4)
            self.assertEqual(metadata["default_channels"], [0, 1, 2, 3])
            self.assertEqual(metadata["current_offset"], 0.0)
            self.assertEqual(metadata["current_count_min"], -32768)
            self.assertEqual(metadata["current_count_max"], 32767)
            self.assertEqual(metadata["voltage_units"], "mV")
            self.assertEqual(len(metadata["channel_voltage_mv"]), 4)
            self.assertAlmostEqual(metadata["channel_voltage_mv"][0], 180.0)
            self.assertNotIn("recording_path", metadata)

            overview = service.overview(
                {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(overview["mode"], "envelope")
            self.assertEqual(len(overview["traces"]), 2)
            self.assertEqual(overview["cache"], "miss")
            self.assertEqual(overview["source"], "pyramid")

            overview_cached = service.overview(
                {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(overview_cached["cache"], "hit")

            detail_raw = service.detail(
                {"start": ["0"], "end": ["20"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(detail_raw["mode"], "raw")
            self.assertEqual(detail_raw["source"], "slice")
            self.assertIn("values", detail_raw["traces"][0])

            detail_envelope = service.detail(
                {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(detail_envelope["mode"], "envelope")
            self.assertEqual(detail_envelope["source"], "slice")
            self.assertIn("mins", detail_envelope["traces"][0])

            detail_fractional = service.detail(
                {"start": ["10.5"], "end": ["29.5"], "width_px": ["200.0"], "channels": ["0,1"]}
            )
            self.assertEqual(detail_fractional["start"], 11)
            self.assertEqual(detail_fractional["end"], 30)
            self.assertEqual(detail_fractional["mode"], "raw")

            overview_binary = decode_trace_binary(
                service.overview_binary({"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]})
            )
            self.assertEqual(overview_binary["mode"], "envelope")
            self.assertEqual(overview_binary["source"], "pyramid")
            self.assertEqual(len(overview_binary["traces"]), 2)
            self.assertIn("mins", overview_binary["traces"][0])

            detail_binary = decode_trace_binary(
                service.detail_binary({"start": ["0"], "end": ["20"], "width_px": ["200"], "channels": ["0,1"]})
            )
            self.assertEqual(detail_binary["mode"], "raw")
            self.assertEqual(detail_binary["traces"][0]["values"].shape[0], 20)

    def test_detail_uses_pyramid_for_wide_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            service = trace_viewer_server.TraceDataService(recording_path)

            with patch.object(trace_viewer_server, "MAX_DETAIL_SLICE_SAMPLES_PER_CHANNEL", 100):
                detail = service.detail(
                    {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
                )

            self.assertEqual(detail["mode"], "envelope")
            self.assertEqual(detail["source"], "pyramid")

    def test_detail_rejects_excessive_width(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            service = trace_viewer_server.TraceDataService(recording_path)

            with self.assertRaises(ValueError):
                service.detail(
                    {
                        "start": ["0"],
                        "end": ["100"],
                        "width_px": [str(trace_viewer_server.MAX_WIDTH_PX + 1)],
                        "channels": ["0,1"],
                    }
                )

    def test_json_budget_rejects_large_detail_responses(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            service = trace_viewer_server.TraceDataService(recording_path)

            with patch.object(trace_viewer_server, "MAX_JSON_TRACE_POINTS", 50):
                with self.assertRaises(ValueError):
                    service.detail(
                        {"start": ["0"], "end": ["40"], "width_px": ["200"], "channels": ["0,1"]}
                    )

    def test_service_rejects_non_int16_recording(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr", current_dtype="float32")
            with self.assertRaises(ValueError):
                trace_viewer_server.TraceDataService(recording_path)

    def test_write_json_ignores_client_disconnects(self) -> None:
        class FakeWfile:
            def write(self, _: bytes) -> None:
                raise ConnectionAbortedError(10053, "aborted")

        handler = trace_viewer_server.TraceViewerHandler.__new__(trace_viewer_server.TraceViewerHandler)
        handler.wfile = FakeWfile()
        handler.send_response = lambda *args, **kwargs: None
        handler.send_header = lambda *args, **kwargs: None
        handler.end_headers = lambda *args, **kwargs: None

        handler._write_json({"ok": True})


if __name__ == "__main__":
    unittest.main()
