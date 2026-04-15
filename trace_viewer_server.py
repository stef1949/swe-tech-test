from __future__ import annotations

import argparse
import ipaddress
import json
import math
import mimetypes
import struct
import threading
from collections import OrderedDict
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
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
TRACE_BINARY_MAGIC = b"TVB1"
TRACE_BINARY_CONTENT_TYPE = "application/vnd.traceviewer.binary"
OVERVIEW_PYRAMID_MIN_BUCKETS = 1024
OVERVIEW_PYRAMID_MULTIPLIER = 8
CLIENT_DISCONNECT_ERRNOS = {32, 54, 104, 10053, 10054}
MAX_WIDTH_PX = 8192
MAX_CHANNEL_QUERY_LENGTH = 512
MAX_CHANNEL_TOKENS = 64
MAX_JSON_TRACE_POINTS = 250_000
MAX_BINARY_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_DETAIL_SLICE_SAMPLES_PER_CHANNEL = 250_000
MAX_RECORDING_CHANNELS = 256
MAX_RECORDING_SAMPLES_PER_CHANNEL = 20_000_000
MAX_CONCURRENT_DETAIL_REQUESTS = 4
DETAIL_SLOT_ACQUIRE_TIMEOUT_SEC = 0.5
REQUEST_SOCKET_TIMEOUT_SEC = 10.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the mock trace viewer.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the Zarr directory.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Interface to bind.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind.")
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow binding to a non-loopback interface such as 0.0.0.0.",
    )
    parser.add_argument(
        "--generate-if-missing",
        action="store_true",
        help="Generate the default recording if it does not exist.",
    )
    return parser.parse_args(argv)


def is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower()
    if normalized in {"localhost", "127.0.0.1", "::1"}:
        return True

    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def validate_bind_host(host: str, allow_remote: bool) -> None:
    if allow_remote or is_loopback_host(host):
        return
    raise SystemExit(
        f"Refusing to bind {host!r} without --allow-remote. "
        "Use a loopback host for local-only access or pass --allow-remote explicitly."
    )


def parse_channel_list(raw_value: str | None, total_channels: int) -> list[int]:
    if not raw_value:
        return list(range(min(DEFAULT_VISIBLE_CHANNELS, total_channels)))
    if len(raw_value) > MAX_CHANNEL_QUERY_LENGTH:
        raise ValueError("channels parameter is too long")

    channels: list[int] = []
    token_count = 0
    for part in raw_value.split(","):
        token = part.strip()
        if not token:
            continue
        token_count += 1
        if token_count > MAX_CHANNEL_TOKENS:
            raise ValueError("too many channels requested")
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


def reduce_envelope_pair(
    mins: np.ndarray,
    maxs: np.ndarray,
    bucket_count: int,
) -> tuple[np.ndarray, np.ndarray]:
    sample_count = int(mins.shape[0])
    if sample_count == 0:
        empty = np.empty(0, dtype=np.int16)
        return empty, empty
    bucket_count = max(1, min(bucket_count, sample_count))
    if bucket_count == sample_count:
        return mins.astype(np.int16, copy=False), maxs.astype(np.int16, copy=False)

    bounds = np.linspace(0, sample_count, num=bucket_count + 1, dtype=np.int64)
    starts = bounds[:-1]
    reduced_mins = np.minimum.reduceat(mins, starts)
    reduced_maxs = np.maximum.reduceat(maxs, starts)
    return reduced_mins.astype(np.int16, copy=False), reduced_maxs.astype(np.int16, copy=False)


def samples_per_pixel(sample_count: int, width_px: int) -> float:
    return sample_count / max(1, width_px)


def detail_mode_for_window(sample_count: int, width_px: int) -> str:
    if samples_per_pixel(sample_count, width_px) <= DETAIL_MAX_SAMPLES_PER_PIXEL:
        return "raw"
    return "envelope"


def estimate_trace_point_count(mode: str, sample_count: int, width_px: int, channel_count: int) -> int:
    if mode == "raw":
        return sample_count * channel_count
    bucket_count = max(1, min(width_px, sample_count))
    return bucket_count * channel_count * 2


def resolve_static_path(raw_path: str) -> Path | None:
    viewer_root = VIEWER_DIR.resolve()
    normalized = raw_path.replace("\\", "/").lstrip("/")
    relative = PurePosixPath(normalized) if normalized else PurePosixPath("index.html")
    candidate = viewer_root.joinpath(*relative.parts)
    if candidate.is_dir():
        candidate = candidate / "index.html"

    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError:
        return None

    if not resolved.is_relative_to(viewer_root) or not resolved.is_file():
        return None
    return resolved


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


@dataclass(frozen=True)
class OverviewLevel:
    bucket_count: int
    mins_by_channel: tuple[np.ndarray, ...]
    maxs_by_channel: tuple[np.ndarray, ...]


def jsonify_trace_payload(payload: dict[str, Any]) -> dict[str, Any]:
    traces: list[dict[str, Any]] = []
    for trace in payload["traces"]:
        trace_payload = {
            "channel": trace["channel"],
            "min_count": trace["min_count"],
            "max_count": trace["max_count"],
        }
        if payload["mode"] == "raw":
            trace_payload["values"] = trace["values"].tolist()
        else:
            trace_payload["mins"] = trace["mins"].tolist()
            trace_payload["maxs"] = trace["maxs"].tolist()
        traces.append(trace_payload)

    return {
        **{key: value for key, value in payload.items() if key != "traces"},
        "channels": list(payload["channels"]),
        "traces": traces,
    }


def encode_trace_payload(payload: dict[str, Any]) -> bytes:
    traces_header: list[dict[str, Any]] = []
    body_parts: list[bytes] = []

    for trace in payload["traces"]:
        trace_header = {
            "channel": trace["channel"],
            "min_count": trace["min_count"],
            "max_count": trace["max_count"],
        }
        if payload["mode"] == "raw":
            values = np.asarray(trace["values"], dtype="<i2")
            trace_header["length"] = int(values.shape[0])
            body_parts.append(values.tobytes(order="C"))
        else:
            mins = np.asarray(trace["mins"], dtype="<i2")
            maxs = np.asarray(trace["maxs"], dtype="<i2")
            trace_header["length"] = int(mins.shape[0])
            body_parts.append(mins.tobytes(order="C"))
            body_parts.append(maxs.tobytes(order="C"))
        traces_header.append(trace_header)

    header = {
        **{key: value for key, value in payload.items() if key != "traces"},
        "channels": list(payload["channels"]),
        "traces": traces_header,
    }
    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    prefix_size = 12
    payload_offset = prefix_size + len(header_bytes)
    if payload_offset % 2:
        payload_offset += 1
    padding = b"\0" * (payload_offset - prefix_size - len(header_bytes))
    return b"".join(
        [
            TRACE_BINARY_MAGIC,
            struct.pack("<II", len(header_bytes), payload_offset),
            header_bytes,
            padding,
            *body_parts,
        ]
    )


class TraceDataService:
    def __init__(self, input_path: Path):
        self.input_path = input_path
        root = zarr.open(str(input_path), mode="r")
        self.root = root
        self.current_arr = root["current_data"]
        self.voltage_arr = root["voltage_data"]
        self.attrs = {str(key): root.attrs[key] for key in root.attrs.keys()}
        self._validate_recording_layout()
        self.total_channels = int(self.current_arr.shape[0])
        self.total_samples = int(self.current_arr.shape[1])
        self.sample_rate_hz = float(self.attrs["sample_rate_hz"])
        self.current_scale = float(self.attrs["current_scale"])
        self.current_units = str(self.attrs["current_units"])
        self.current_dtype_info = np.iinfo(self.current_arr.dtype)
        self._overview_cache = LRUCache(OVERVIEW_CACHE_SIZE)
        self._cache_lock = threading.Lock()
        self._overview_levels = self._build_overview_levels()
        default_window_samples = int(self.sample_rate_hz * DEFAULT_DETAIL_SECONDS)
        self._metadata = {
            "device_id": self.attrs.get("device_id"),
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

    def _validate_recording_layout(self) -> None:
        if self.current_arr.ndim != 2 or self.voltage_arr.ndim != 2:
            raise ValueError("recording arrays must be two-dimensional")
        if self.current_arr.shape != self.voltage_arr.shape:
            raise ValueError("current_data and voltage_data must have matching shapes")
        if np.dtype(self.current_arr.dtype) != np.dtype(np.int16):
            raise ValueError("current_data must use int16 samples")
        if np.dtype(self.voltage_arr.dtype) != np.dtype(np.int16):
            raise ValueError("voltage_data must use int16 samples")

        total_channels = int(self.current_arr.shape[0])
        total_samples = int(self.current_arr.shape[1])
        if total_channels <= 0 or total_samples <= 0:
            raise ValueError("recording arrays must be non-empty")
        if total_channels > MAX_RECORDING_CHANNELS:
            raise ValueError(f"recording has too many channels: {total_channels}")
        if total_samples > MAX_RECORDING_SAMPLES_PER_CHANNEL:
            raise ValueError(f"recording has too many samples per channel: {total_samples}")

        try:
            sample_rate_hz = float(self.attrs["sample_rate_hz"])
            current_scale = float(self.attrs["current_scale"])
            current_units = str(self.attrs["current_units"])
            duration_sec = float(self.attrs["duration_sec"])
        except KeyError as exc:
            raise ValueError(f"missing required recording attribute: {exc.args[0]}") from exc

        if not math.isfinite(sample_rate_hz) or sample_rate_hz <= 0:
            raise ValueError("sample_rate_hz must be a positive finite number")
        if not math.isfinite(current_scale) or current_scale <= 0:
            raise ValueError("current_scale must be a positive finite number")
        if not current_units:
            raise ValueError("current_units must be a non-empty string")
        if not math.isfinite(duration_sec) or duration_sec <= 0:
            raise ValueError("duration_sec must be a positive finite number")

    def _normalize_request(self, query: dict[str, list[str]]) -> WindowRequest:
        start = parse_int_param(query.get("start", ["0"])[0], "start")
        end = parse_int_param(query.get("end", [str(self.total_samples)])[0], "end")
        width_px = max(1, parse_int_param(query.get("width_px", [str(DEFAULT_WIDTH_PX)])[0], "width_px"))
        if width_px > MAX_WIDTH_PX:
            raise ValueError(f"width_px too large: {width_px} (max {MAX_WIDTH_PX})")
        start, end = clamp_window(start, end, self.total_samples)
        channels = tuple(parse_channel_list(query.get("channels", [None])[0], self.total_channels))
        return WindowRequest(start=start, end=end, width_px=width_px, channels=channels)

    def _enforce_response_budget(self, request: WindowRequest, mode: str, response_format: str) -> None:
        point_count = estimate_trace_point_count(
            mode,
            request.end - request.start,
            request.width_px,
            len(request.channels),
        )
        if response_format == "json" and point_count > MAX_JSON_TRACE_POINTS:
            raise ValueError(
                "requested response is too large for JSON; reduce width, channels, or window span, or use format=binary"
            )
        if response_format == "binary" and point_count * np.dtype(np.int16).itemsize > MAX_BINARY_RESPONSE_BYTES:
            raise ValueError("requested response is too large; reduce width, channels, or window span")

    def _read_channel_slice(self, channel: int, start: int, end: int) -> np.ndarray:
        return np.asarray(self.current_arr[channel, start:end], dtype=np.int16)

    def _overview_bucket_counts(self) -> list[int]:
        if self.total_samples <= 0:
            return [1]

        target = min(
            self.total_samples,
            max(OVERVIEW_PYRAMID_MIN_BUCKETS, DEFAULT_WIDTH_PX * OVERVIEW_PYRAMID_MULTIPLIER),
        )
        finest = 1
        while finest < target:
            finest <<= 1
        finest = min(finest, self.total_samples)

        counts: list[int] = []
        current = finest
        minimum = min(self.total_samples, OVERVIEW_PYRAMID_MIN_BUCKETS)
        while current >= 1:
            counts.append(current)
            if current <= minimum:
                break
            current = max(1, current // 2)
        return sorted(set(counts))

    def _build_overview_levels(self) -> tuple[OverviewLevel, ...]:
        bucket_counts = self._overview_bucket_counts()
        finest_bucket_count = bucket_counts[-1]
        mins_by_count: dict[int, list[np.ndarray]] = {count: [] for count in bucket_counts}
        maxs_by_count: dict[int, list[np.ndarray]] = {count: [] for count in bucket_counts}

        for channel in range(self.total_channels):
            data = self._read_channel_slice(channel, 0, self.total_samples)
            finest_mins, finest_maxs = reduce_to_envelope(data, finest_bucket_count)
            mins_by_count[finest_bucket_count].append(finest_mins)
            maxs_by_count[finest_bucket_count].append(finest_maxs)

            previous_mins = finest_mins
            previous_maxs = finest_maxs
            previous_count = finest_bucket_count
            for bucket_count in reversed(bucket_counts[:-1]):
                if bucket_count == previous_count:
                    reduced_mins = previous_mins
                    reduced_maxs = previous_maxs
                else:
                    reduced_mins, reduced_maxs = reduce_envelope_pair(previous_mins, previous_maxs, bucket_count)
                mins_by_count[bucket_count].append(reduced_mins)
                maxs_by_count[bucket_count].append(reduced_maxs)
                previous_mins = reduced_mins
                previous_maxs = reduced_maxs
                previous_count = bucket_count

        levels = [
            OverviewLevel(
                bucket_count=count,
                mins_by_channel=tuple(mins_by_count[count]),
                maxs_by_channel=tuple(maxs_by_count[count]),
            )
            for count in bucket_counts
        ]
        return tuple(levels)

    def _select_overview_level(self, width_px: int) -> OverviewLevel:
        for level in self._overview_levels:
            if level.bucket_count >= width_px:
                return level
        return self._overview_levels[-1]

    def _build_pyramid_payload(
        self,
        request: WindowRequest,
        *,
        source: str,
        cache_status: str | None = None,
    ) -> dict[str, Any]:
        level = self._select_overview_level(request.width_px)
        traces = []
        total_span = max(1, self.total_samples)
        start_index = max(0, math.floor((request.start / total_span) * level.bucket_count))
        end_index = min(level.bucket_count, math.ceil((request.end / total_span) * level.bucket_count))
        end_index = max(start_index + 1, end_index)

        for channel in request.channels:
            level_mins = level.mins_by_channel[channel][start_index:end_index]
            level_maxs = level.maxs_by_channel[channel][start_index:end_index]
            bucket_count = max(1, min(request.width_px, level_mins.shape[0]))
            mins, maxs = reduce_envelope_pair(level_mins, level_maxs, bucket_count)
            traces.append(
                {
                    "channel": channel,
                    "mins": mins,
                    "maxs": maxs,
                    "min_count": int(mins.min()) if mins.size else 0,
                    "max_count": int(maxs.max()) if maxs.size else 0,
                }
            )

        returned_bucket_count = traces[0]["mins"].shape[0] if traces else 0

        response = {
            "mode": "envelope",
            "source": source,
            "start": request.start,
            "end": request.end,
            "sample_count": request.end - request.start,
            "seconds": round((request.end - request.start) / self.sample_rate_hz, 3),
            "samples_per_pixel": round(samples_per_pixel(request.end - request.start, request.width_px), 3),
            "width_px": request.width_px,
            "bucket_count": returned_bucket_count,
            "channels": request.channels,
            "current_scale": self.current_scale,
            "current_units": self.current_units,
            "traces": traces,
        }
        if cache_status is not None:
            response["cache"] = cache_status
        return response

    def _detail_strategy(self, request: WindowRequest) -> tuple[str, str]:
        sample_count = request.end - request.start
        if sample_count > MAX_DETAIL_SLICE_SAMPLES_PER_CHANNEL:
            return "pyramid", "envelope"
        return "slice", detail_mode_for_window(sample_count, request.width_px)

    def _build_detail_payload(self, request: WindowRequest, response_format: str) -> dict[str, Any]:
        sample_count = request.end - request.start
        source, render_mode = self._detail_strategy(request)
        self._enforce_response_budget(request, render_mode, response_format)
        if source == "pyramid":
            return self._build_pyramid_payload(request, source="pyramid")

        traces = []
        for channel in request.channels:
            data = self._read_channel_slice(channel, request.start, request.end)
            if render_mode == "raw":
                traces.append(
                    {
                        "channel": channel,
                        "values": data,
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
                    "mins": mins,
                    "maxs": maxs,
                    "min_count": int(mins.min()) if mins.size else 0,
                    "max_count": int(maxs.max()) if maxs.size else 0,
                }
            )

        response = {
            "mode": render_mode,
            "source": "slice",
            "start": request.start,
            "end": request.end,
            "sample_count": sample_count,
            "seconds": round(sample_count / self.sample_rate_hz, 3),
            "samples_per_pixel": round(samples_per_pixel(sample_count, request.width_px), 3),
            "width_px": request.width_px,
            "channels": request.channels,
            "current_scale": self.current_scale,
            "current_units": self.current_units,
            "traces": traces,
        }
        if render_mode == "envelope":
            response["bucket_count"] = max(1, min(request.width_px, sample_count))
        return response

    def overview(self, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self._normalize_request(query)
        self._enforce_response_budget(request, "envelope", "json")
        cache_key = ("overview", request.start, request.end, request.width_px, request.channels)
        with self._cache_lock:
            cached = self._overview_cache.get(cache_key)
        if cached is not None:
            return jsonify_trace_payload({**cached, "cache": "hit"})

        response = self._build_pyramid_payload(request, source="pyramid", cache_status="miss")
        with self._cache_lock:
            self._overview_cache.set(cache_key, response)
        return jsonify_trace_payload(response)

    def detail(self, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self._normalize_request(query)
        return jsonify_trace_payload(self._build_detail_payload(request, response_format="json"))

    def overview_binary(self, query: dict[str, list[str]]) -> bytes:
        request = self._normalize_request(query)
        self._enforce_response_budget(request, "envelope", "binary")
        cache_key = ("overview", request.start, request.end, request.width_px, request.channels)
        with self._cache_lock:
            cached = self._overview_cache.get(cache_key)
        if cached is not None:
            return encode_trace_payload({**cached, "cache": "hit"})

        response = self._build_pyramid_payload(request, source="pyramid", cache_status="miss")
        with self._cache_lock:
            self._overview_cache.set(cache_key, response)
        return encode_trace_payload(response)

    def detail_binary(self, query: dict[str, list[str]]) -> bytes:
        request = self._normalize_request(query)
        return encode_trace_payload(self._build_detail_payload(request, response_format="binary"))


class TraceViewerHandler(BaseHTTPRequestHandler):
    server_version = "TraceViewer/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/api/metadata":
            return self._handle_json(lambda: self.server.data_service.metadata())
        if parsed.path == "/api/overview":
            if query.get("format", ["json"])[0] == "binary":
                return self._handle_binary(lambda: self.server.data_service.overview_binary(query))
            return self._handle_json(lambda: self.server.data_service.overview(query))
        if parsed.path == "/api/detail":
            if query.get("format", ["json"])[0] == "binary":
                return self._handle_detail(lambda: self.server.data_service.detail_binary(query), binary=True)
            return self._handle_detail(lambda: self.server.data_service.detail(query), binary=False)
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

    def _handle_binary(self, callback: Any) -> None:
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
        self._write_binary(payload)

    def _handle_detail(self, callback: Any, *, binary: bool) -> None:
        if not self.server.detail_slots.acquire(timeout=DETAIL_SLOT_ACQUIRE_TIMEOUT_SEC):
            self._write_json(
                {"error": "server is busy processing other detail requests"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        try:
            if binary:
                self._handle_binary(callback)
            else:
                self._handle_json(callback)
        finally:
            self.server.detail_slots.release()

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

    def _write_binary(self, payload: bytes, status: HTTPStatus = HTTPStatus.OK) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", TRACE_BINARY_CONTENT_TYPE)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
        except OSError as exc:
            if is_client_disconnect_error(exc):
                return
            raise

    def _serve_static(self, raw_path: str) -> None:
        candidate = resolve_static_path(raw_path)
        if candidate is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
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
    daemon_threads = True
    block_on_close = False
    request_queue_size = 64

    def __init__(self, server_address: tuple[str, int], data_service: TraceDataService):
        super().__init__(server_address, TraceViewerHandler)
        self.data_service = data_service
        self.detail_slots = threading.BoundedSemaphore(MAX_CONCURRENT_DETAIL_REQUESTS)

    def get_request(self) -> tuple[Any, Any]:
        request, client_address = super().get_request()
        request.settimeout(REQUEST_SOCKET_TIMEOUT_SEC)
        return request, client_address


def run_server(host: str, port: int, input_path: Path, generate_if_missing: bool, allow_remote: bool) -> None:
    validate_bind_host(host, allow_remote)
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
    run_server(args.host, args.port, input_path, args.generate_if_missing, args.allow_remote)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
