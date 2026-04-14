from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
import zarr
from zarr.codecs import BloscCodec, BloscShuffle

import trace_viewer_server


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

    def test_reduce_to_envelope_computes_min_and_max(self) -> None:
        data = np.array([3, 7, 2, 8, 4, 9], dtype=np.int16)
        mins, maxs = trace_viewer_server.reduce_to_envelope(data, 3)
        self.assertEqual(mins.tolist(), [3, 2, 4])
        self.assertEqual(maxs.tolist(), [7, 8, 9])

    def test_detail_mode_switches_at_density_threshold(self) -> None:
        self.assertEqual(trace_viewer_server.detail_mode_for_window(1000, 500), "raw")
        self.assertEqual(trace_viewer_server.detail_mode_for_window(5000, 500), "envelope")

    def test_service_metadata_overview_and_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            recording_path = create_fixture(Path(tmpdir) / "fixture.zarr")
            service = trace_viewer_server.TraceDataService(recording_path)

            metadata = service.metadata()
            self.assertEqual(metadata["channels"], 4)
            self.assertEqual(metadata["default_channels"], [0, 1, 2, 3])
            self.assertEqual(metadata["current_count_min"], -32768)
            self.assertEqual(metadata["current_count_max"], 32767)

            overview = service.overview(
                {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(overview["mode"], "envelope")
            self.assertEqual(len(overview["traces"]), 2)
            self.assertEqual(overview["cache"], "miss")

            overview_cached = service.overview(
                {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(overview_cached["cache"], "hit")

            detail_raw = service.detail(
                {"start": ["0"], "end": ["20"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(detail_raw["mode"], "raw")
            self.assertIn("values", detail_raw["traces"][0])

            detail_envelope = service.detail(
                {"start": ["0"], "end": ["4000"], "width_px": ["200"], "channels": ["0,1"]}
            )
            self.assertEqual(detail_envelope["mode"], "envelope")
            self.assertIn("mins", detail_envelope["traces"][0])

            detail_fractional = service.detail(
                {"start": ["10.5"], "end": ["29.5"], "width_px": ["200.0"], "channels": ["0,1"]}
            )
            self.assertEqual(detail_fractional["start"], 11)
            self.assertEqual(detail_fractional["end"], 30)
            self.assertEqual(detail_fractional["mode"], "raw")

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
