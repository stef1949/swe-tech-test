from __future__ import annotations

import argparse
import json
import math
import mimetypes
import threading
from collections import OrderedDict
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import numpy as np
import zarr

from analyze_trace_viewer import DEFAULT_INPUT, ensure_recording_exists

VIEWER_DIR = Path(__file__).parent / "viewer"
DEFAULT_PORT = 8000
DEFAULT_HOST = "127.0.0.1"
DEFAULT_VISIBLE_CHANNELS = 6
DEFAULT_DETAIL_SECONDS = 1.0
DEFAULT_WIDTH_PX = 1200
DETAIL_MAX_SAMPLES_PER_PIXEL = 2.5
OVERVIEW_CACHE_SIZE = 16
CLIENT_DISCONNECT_ERRNOS = {32, 54, 104, 10053, 10054}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the mock trace viewer.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the Zarr directory.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Interface to bind.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind.")
    parser.add_argument(
        "--generate-if-missing",
        action="store_true",
        help="Generate the default recording if it does not exist.",
    )
    return parser.parse_args(argv)


def parse_channel_list(raw_value: str | None, total_channels: int) -> list[int]:
    if not raw_value:
        return list(range(min(DEFAULT_VISIBLE_CHANNELS, total_channels)))

    channels: list[int] = []
    for part in raw_value.split(","):
        token = part.strip()
        if not token:
            continue
        channel = int(token)
        if channel < 0 or channel >= total_channels:
            raise ValueError(f"channel out of range: {channel}")
        if channel not in channels:
            channels.append(channel)
    if not channels:
        raise ValueError("at least one channel must be selected")
    return channels


def is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError) and exc.errno in CLIENT_DISCONNECT_ERRNOS:
        return True
    return False


def parse_int_param(raw_value: str, name: str) -> int:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"invalid {name}: {raw_value!r}") from exc

    if not math.isfinite(value):
        raise ValueError(f"invalid {name}: {raw_value!r}")
    if value >= 0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def clamp_window(start: int, end: int, total_samples: int) -> tuple[int, int]:
    if total_samples <= 0:
        return 0, 0
    span = max(1, end - start)
    start = max(0, min(start, total_samples - 1))
    end = min(total_samples, start + span)
    if end <= start:
        end = min(total_samples, start + 1)
    if end - start < span and end == total_samples:
        start = max(0, end - span)
    return start, end


def reduce_to_envelope(data: np.ndarray, bucket_count: int) -> tuple[np.ndarray, np.ndarray]:
    sample_count = int(data.shape[0])
    if sample_count == 0:
        empty = np.empty(0, dtype=np.int16)
        return empty, empty
    bucket_count = max(1, min(bucket_count, sample_count))
    if bucket_count == sample_count:
        return data.astype(np.int16, copy=False), data.astype(np.int16, copy=False)

    bounds = np.linspace(0, sample_count, num=bucket_count + 1, dtype=np.int64)
    starts = bounds[:-1]
    mins = np.minimum.reduceat(data, starts)
    maxs = np.maximum.reduceat(data, starts)
    return mins.astype(np.int16, copy=False), maxs.astype(np.int16, copy=False)


def samples_per_pixel(sample_count: int, width_px: int) -> float:
    return sample_count / max(1, width_px)


def detail_mode_for_window(sample_count: int, width_px: int) -> str:
    if samples_per_pixel(sample_count, width_px) <= DETAIL_MAX_SAMPLES_PER_PIXEL:
        return "raw"
    return "envelope"


class LRUCache:
    def __init__(self, max_items: int):
        self.max_items = max_items
        self._items: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()

    def get(self, key: tuple[Any, ...]) -> dict[str, Any] | None:
        item = self._items.get(key)
        if item is None:
            return None
        self._items.move_to_end(key)
        return item

    def set(self, key: tuple[Any, ...], value: dict[str, Any]) -> None:
        self._items[key] = value
        self._items.move_to_end(key)
        while len(self._items) > self.max_items:
            self._items.popitem(last=False)


@dataclass(frozen=True)
class WindowRequest:
    start: int
    end: int
    width_px: int
    channels: tuple[int, ...]


class TraceDataService:
    def __init__(self, input_path: Path):
        self.input_path = input_path
        root = zarr.open(str(input_path), mode="r")
        self.root = root
        self.current_arr = root["current_data"]
        self.voltage_arr = root["voltage_data"]
        self.attrs = {str(key): root.attrs[key] for key in root.attrs.keys()}
        self.total_channels = int(self.current_arr.shape[0])
        self.total_samples = int(self.current_arr.shape[1])
        self.sample_rate_hz = float(self.attrs["sample_rate_hz"])
        self.current_scale = float(self.attrs["current_scale"])
        self.current_units = str(self.attrs["current_units"])
        self.current_dtype_info = np.iinfo(self.current_arr.dtype)
        self._overview_cache = LRUCache(OVERVIEW_CACHE_SIZE)
        self._cache_lock = threading.Lock()
        default_window_samples = int(self.sample_rate_hz * DEFAULT_DETAIL_SECONDS)
        self._metadata = {
            "device_id": self.attrs.get("device_id"),
            "recording_path": str(self.input_path),
            "channels": self.total_channels,
            "total_samples": self.total_samples,
            "sample_rate_hz": self.sample_rate_hz,
            "duration_sec": int(self.attrs.get("duration_sec")),
            "current_scale": self.current_scale,
            "current_units": self.current_units,
            "current_count_min": int(self.current_dtype_info.min),
            "current_count_max": int(self.current_dtype_info.max),
            "voltage_scale": float(self.attrs.get("voltage_scale", 1.0)),
            "default_channels": list(range(min(DEFAULT_VISIBLE_CHANNELS, self.total_channels))),
            "default_window": {
                "start": 0,
                "end": min(self.total_samples, default_window_samples),
            },
            "detail_threshold": {
                "samples_per_pixel": DETAIL_MAX_SAMPLES_PER_PIXEL,
                "seconds": round((DEFAULT_WIDTH_PX * DETAIL_MAX_SAMPLES_PER_PIXEL) / self.sample_rate_hz, 3),
            },
        }

    def metadata(self) -> dict[str, Any]:
        return self._metadata

    def _normalize_request(self, query: dict[str, list[str]]) -> WindowRequest:
        start = parse_int_param(query.get("start", ["0"])[0], "start")
        end = parse_int_param(query.get("end", [str(self.total_samples)])[0], "end")
        width_px = max(1, parse_int_param(query.get("width_px", [str(DEFAULT_WIDTH_PX)])[0], "width_px"))
        start, end = clamp_window(start, end, self.total_samples)
        channels = tuple(parse_channel_list(query.get("channels", [None])[0], self.total_channels))
        return WindowRequest(start=start, end=end, width_px=width_px, channels=channels)

    def _read_channel_slice(self, channel: int, start: int, end: int) -> np.ndarray:
        return np.asarray(self.current_arr[channel, start:end], dtype=np.int16)

    def overview(self, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self._normalize_request(query)
        cache_key = ("overview", request.start, request.end, request.width_px, request.channels)
        with self._cache_lock:
            cached = self._overview_cache.get(cache_key)
        if cached is not None:
            return {**cached, "cache": "hit"}

        traces = []
        bucket_count = max(1, min(request.width_px, request.end - request.start))
        for channel in request.channels:
            data = self._read_channel_slice(channel, request.start, request.end)
            mins, maxs = reduce_to_envelope(data, bucket_count)
            traces.append(
                {
                    "channel": channel,
                    "mins": mins.tolist(),
                    "maxs": maxs.tolist(),
                    "min_count": int(mins.min()) if mins.size else 0,
                    "max_count": int(maxs.max()) if maxs.size else 0,
                }
            )

        response = {
            "mode": "envelope",
            "cache": "miss",
            "start": request.start,
            "end": request.end,
            "sample_count": request.end - request.start,
            "samples_per_pixel": round(samples_per_pixel(request.end - request.start, request.width_px), 3),
            "width_px": request.width_px,
            "bucket_count": bucket_count,
            "channels": list(request.channels),
            "current_scale": self.current_scale,
            "current_units": self.current_units,
            "traces": traces,
        }
        with self._cache_lock:
            self._overview_cache.set(cache_key, response)
        return response

    def detail(self, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self._normalize_request(query)
        sample_count = request.end - request.start
        render_mode = detail_mode_for_window(sample_count, request.width_px)
        traces = []
        for channel in request.channels:
            data = self._read_channel_slice(channel, request.start, request.end)
            if render_mode == "raw":
                traces.append(
                    {
                        "channel": channel,
                        "values": data.tolist(),
                        "min_count": int(data.min()) if data.size else 0,
                        "max_count": int(data.max()) if data.size else 0,
                    }
                )
                continue

            bucket_count = max(1, min(request.width_px, sample_count))
            mins, maxs = reduce_to_envelope(data, bucket_count)
            traces.append(
                {
                    "channel": channel,
                    "mins": mins.tolist(),
                    "maxs": maxs.tolist(),
                    "min_count": int(mins.min()) if mins.size else 0,
                    "max_count": int(maxs.max()) if maxs.size else 0,
                }
            )

        response = {
            "mode": render_mode,
            "start": request.start,
            "end": request.end,
            "sample_count": sample_count,
            "seconds": round(sample_count / self.sample_rate_hz, 3),
            "samples_per_pixel": round(samples_per_pixel(sample_count, request.width_px), 3),
            "width_px": request.width_px,
            "channels": list(request.channels),
            "current_scale": self.current_scale,
            "current_units": self.current_units,
            "traces": traces,
        }
        if render_mode == "envelope":
            response["bucket_count"] = max(1, min(request.width_px, sample_count))
        return response


class TraceViewerHandler(BaseHTTPRequestHandler):
    server_version = "TraceViewer/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/metadata":
            return self._handle_json(lambda: self.server.data_service.metadata())
        if parsed.path == "/api/overview":
            return self._handle_json(lambda: self.server.data_service.overview(parse_qs(parsed.query)))
        if parsed.path == "/api/detail":
            return self._handle_json(lambda: self.server.data_service.detail(parse_qs(parsed.query)))
        if parsed.path == "/health":
            return self._write_json({"ok": True})
        return self._serve_static(parsed.path)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _handle_json(self, callback: Any) -> None:
        try:
            payload = callback()
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except FileNotFoundError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        except Exception as exc:  # pragma: no cover
            self._write_json({"error": f"internal error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._write_json(payload)

    def _write_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except OSError as exc:
            if is_client_disconnect_error(exc):
                return
            raise

    def _serve_static(self, raw_path: str) -> None:
        relative = raw_path.lstrip("/") or "index.html"
        candidate = (VIEWER_DIR / relative).resolve()
        if not str(candidate).startswith(str(VIEWER_DIR.resolve())) or not candidate.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if candidate.is_dir():
            candidate = candidate / "index.html"
        body = candidate.read_bytes()
        content_type, _ = mimetypes.guess_type(candidate.name)
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError as exc:
            if is_client_disconnect_error(exc):
                return
            raise


class TraceViewerServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], data_service: TraceDataService):
        super().__init__(server_address, TraceViewerHandler)
        self.data_service = data_service


def run_server(host: str, port: int, input_path: Path, generate_if_missing: bool) -> None:
    ensure_recording_exists(input_path, generate_if_missing)
    data_service = TraceDataService(input_path)
    httpd = TraceViewerServer((host, port), data_service)
    print(f"Serving trace viewer at http://{host}:{port}")
    print(f"Reading from {input_path}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.resolve()
    run_server(args.host, args.port, input_path, args.generate_if_missing)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
