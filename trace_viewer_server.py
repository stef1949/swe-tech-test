from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import math
import mimetypes
import struct
import threading
import time
import tempfile
import zipfile
import zlib
from collections import Counter, OrderedDict
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
DEFAULT_DETAIL_SECONDS = 5.0
DEFAULT_VIEWPORT_PX = 1200
DETAIL_MAX_SAMPLES_PER_PIXEL = 2.5
OVERVIEW_CACHE_SIZE = 64
TRACE_BINARY_MAGIC = b"TVB1"
TRACE_BINARY_CONTENT_TYPE = "application/vnd.nanopore-trace.v1+binary"
LEGACY_TRACE_BINARY_CONTENT_TYPE = "application/vnd.traceviewer.binary"
PROBLEM_JSON_CONTENT_TYPE = "application/problem+json; charset=utf-8"
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
OVERVIEW_PYRAMID_MIN_BUCKETS = 1024
OVERVIEW_PYRAMID_MULTIPLIER = 64
OVERVIEW_TILE_BUCKETS = 1024
PYRAMID_VIEWPORT_OVERSAMPLE = 1.0
CLIENT_DISCONNECT_ERRNOS = {32, 54, 104, 10053, 10054}
MAX_VIEWPORT_PX = 4096
MAX_WIDTH_PX = MAX_VIEWPORT_PX
MAX_CHANNEL_QUERY_LENGTH = 512
MAX_CHANNEL_TOKENS = 64
MAX_JSON_TRACE_POINTS = 250_000
MAX_BINARY_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_DETAIL_SLICE_SAMPLES_PER_CHANNEL = 200_000
MAX_RECORDING_CHANNELS = 256
MAX_RECORDING_SAMPLES_PER_CHANNEL = 20_000_000
MAX_CONCURRENT_DETAIL_REQUESTS = 2
DETAIL_SLOT_ACQUIRE_TIMEOUT_SEC = 0.2
REQUEST_SOCKET_TIMEOUT_SEC = 10.0
CACHE_CONTROL_STATIC = "public, max-age=300"
CACHE_CONTROL_METADATA = "public, max-age=300, stale-while-revalidate=3600"
CACHE_CONTROL_OVERVIEW = "public, max-age=31536000, immutable"
CACHE_CONTROL_TILE = "public, max-age=31536000, immutable"
CACHE_CONTROL_DETAIL = "private, max-age=30, stale-while-revalidate=300"
CACHE_CONTROL_HEALTH = "no-store"
PROBLEM_BASE_URL = "https://traceviewer.local/problems"


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
    parser.add_argument(
        "--pyramid-sidecar",
        type=Path,
        default=None,
        help="Optional .npz sidecar file for precomputed envelope pyramid data.",
    )
    parser.add_argument(
        "--build-pyramid-only",
        action="store_true",
        help="Build or refresh the pyramid sidecar and exit.",
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


def normalize_recording_id(raw_value: str) -> str:
    normalized = []
    for char in raw_value.strip().lower():
        if char.isalnum() or char in {"-", "_", "."}:
            normalized.append(char)
        else:
            normalized.append("-")
    compact = "".join(normalized).strip("-")
    while "--" in compact:
        compact = compact.replace("--", "-")
    return compact or "recording"


def default_pyramid_sidecar_path(input_path: Path) -> Path:
    return input_path.parent / f"{input_path.name}.trace-pyramid.npz"


def is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError) and exc.errno in CLIENT_DISCONNECT_ERRNOS:
        return True
    return False


def parse_strict_int(raw_value: str, name: str) -> int:
    text = raw_value.strip()
    if not text or text in {"+", "-"}:
        raise ValueError(f"invalid {name}: {raw_value!r}")
    digits = text[1:] if text[0] in {"+", "-"} else text
    if not digits.isdigit():
        raise ValueError(f"invalid {name}: {raw_value!r}")
    return int(text)


def parse_positive_int(raw_value: str, name: str) -> int:
    value = parse_strict_int(raw_value, name)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


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


def parse_channel_list(raw_value: str | None, total_channels: int) -> list[int]:
    if not raw_value:
        return list(range(min(DEFAULT_VISIBLE_CHANNELS, total_channels)))
    if len(raw_value) > MAX_CHANNEL_QUERY_LENGTH:
        raise ValueError("channels parameter is too long")

    requested: set[int] = set()
    token_count = 0
    for part in raw_value.split(","):
        token = part.strip()
        if not token:
            continue
        token_count += 1
        if token_count > MAX_CHANNEL_TOKENS:
            raise ValueError("too many channels requested")
        channel = parse_strict_int(token, "channels")
        if channel < 0 or channel >= total_channels:
            raise ValueError(f"channel out of range: {channel}")
        requested.add(channel)
    if not requested:
        raise ValueError("at least one channel must be selected")
    return sorted(requested)


def samples_per_pixel(sample_count: int, viewport_px: int) -> float:
    return sample_count / max(1, viewport_px)


def detail_mode_for_window(sample_count: int, viewport_px: int) -> str:
    if samples_per_pixel(sample_count, viewport_px) <= DETAIL_MAX_SAMPLES_PER_PIXEL:
        return "raw"
    return "envelope"


def bucket_bounds(sample_count: int, bucket_count: int) -> np.ndarray:
    return np.linspace(0, sample_count, num=bucket_count + 1, dtype=np.int64)


def reduce_to_envelope(data: np.ndarray, bucket_count: int) -> tuple[np.ndarray, np.ndarray]:
    sample_count = int(data.shape[0])
    if sample_count == 0:
        empty = np.empty(0, dtype=np.int16)
        return empty, empty
    bucket_count = max(1, min(bucket_count, sample_count))
    if bucket_count == sample_count:
        materialized = data.astype(np.int16, copy=False)
        return materialized, materialized

    bounds = bucket_bounds(sample_count, bucket_count)
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

    bounds = bucket_bounds(sample_count, bucket_count)
    starts = bounds[:-1]
    reduced_mins = np.minimum.reduceat(mins, starts)
    reduced_maxs = np.maximum.reduceat(maxs, starts)
    return reduced_mins.astype(np.int16, copy=False), reduced_maxs.astype(np.int16, copy=False)


def estimate_trace_point_count(mode: str, sample_count: int, viewport_px: int, channel_count: int) -> int:
    if mode == "raw":
        return sample_count * channel_count
    bucket_count = max(1, min(viewport_px, sample_count))
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


def quote_etag(value: str) -> str:
    return f'"{value}"'


def matches_if_none_match(header_value: str | None, etag: str) -> bool:
    if not header_value:
        return False
    if header_value.strip() == "*":
        return True
    return etag in {token.strip() for token in header_value.split(",")}


class ApiError(Exception):
    def __init__(
        self,
        *,
        status: HTTPStatus,
        type_name: str,
        title: str,
        detail: str,
        invalid_params: list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.type_name = type_name
        self.title = title
        self.detail = detail
        self.invalid_params = invalid_params or []
        self.headers = headers or {}

    def to_problem(self) -> dict[str, Any]:
        payload = {
            "type": f"{PROBLEM_BASE_URL}/{self.type_name}",
            "title": self.title,
            "status": int(self.status),
            "detail": self.detail,
        }
        if self.invalid_params:
            payload["invalid_params"] = self.invalid_params
        return payload


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

    def size(self) -> int:
        return len(self._items)


class ServerMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started_at = time.time()
        self.request_total: Counter[tuple[str, str]] = Counter()
        self.overview_cache_total: Counter[str] = Counter()
        self.detail_rejections_total = 0
        self.not_modified_total = 0
        self.response_bytes_total = 0

    def record_request(self, endpoint: str, status: HTTPStatus | int) -> None:
        key = (endpoint, str(int(status)))
        with self._lock:
            self.request_total[key] += 1

    def record_overview_cache(self, outcome: str) -> None:
        with self._lock:
            self.overview_cache_total[outcome] += 1

    def record_detail_rejection(self) -> None:
        with self._lock:
            self.detail_rejections_total += 1

    def record_not_modified(self) -> None:
        with self._lock:
            self.not_modified_total += 1

    def record_response_bytes(self, size: int) -> None:
        with self._lock:
            self.response_bytes_total += size

    def snapshot(self, *, detail_slots_available: int, overview_cache_size: int) -> dict[str, Any]:
        with self._lock:
            request_total = {
                f'{endpoint}|{status}': count
                for (endpoint, status), count in sorted(self.request_total.items())
            }
            overview_cache_total = dict(sorted(self.overview_cache_total.items()))
            detail_rejections_total = self.detail_rejections_total
            not_modified_total = self.not_modified_total
            response_bytes_total = self.response_bytes_total

        return {
            "started_at_epoch_sec": round(self.started_at, 3),
            "uptime_sec": round(max(0.0, time.time() - self.started_at), 3),
            "request_total": request_total,
            "overview_cache_total": overview_cache_total,
            "detail_rejections_total": detail_rejections_total,
            "not_modified_total": not_modified_total,
            "response_bytes_total": response_bytes_total,
            "detail_slots_available": detail_slots_available,
            "overview_cache_size": overview_cache_size,
        }

    def prometheus(self, *, detail_slots_available: int, overview_cache_size: int) -> str:
        snapshot = self.snapshot(
            detail_slots_available=detail_slots_available,
            overview_cache_size=overview_cache_size,
        )
        lines = [
            "# HELP traceviewer_uptime_seconds Server uptime in seconds.",
            "# TYPE traceviewer_uptime_seconds gauge",
            f'traceviewer_uptime_seconds {snapshot["uptime_sec"]}',
            "# HELP traceviewer_detail_slots_available Available detail request slots.",
            "# TYPE traceviewer_detail_slots_available gauge",
            f'traceviewer_detail_slots_available {snapshot["detail_slots_available"]}',
            "# HELP traceviewer_overview_cache_size Current overview cache size.",
            "# TYPE traceviewer_overview_cache_size gauge",
            f'traceviewer_overview_cache_size {snapshot["overview_cache_size"]}',
            "# HELP traceviewer_detail_rejections_total Detail requests rejected due to overload.",
            "# TYPE traceviewer_detail_rejections_total counter",
            f'traceviewer_detail_rejections_total {snapshot["detail_rejections_total"]}',
            "# HELP traceviewer_not_modified_total Conditional GET requests satisfied by ETag.",
            "# TYPE traceviewer_not_modified_total counter",
            f'traceviewer_not_modified_total {snapshot["not_modified_total"]}',
            "# HELP traceviewer_response_bytes_total Total response bytes written.",
            "# TYPE traceviewer_response_bytes_total counter",
            f'traceviewer_response_bytes_total {snapshot["response_bytes_total"]}',
            "# HELP traceviewer_requests_total Total API requests by endpoint and status.",
            "# TYPE traceviewer_requests_total counter",
        ]
        for key, count in snapshot["request_total"].items():
            endpoint, status = key.split("|", maxsplit=1)
            lines.append(
                f'traceviewer_requests_total{{endpoint="{endpoint}",status="{status}"}} {count}'
            )
        lines.extend(
            [
                "# HELP traceviewer_overview_cache_total Overview cache outcomes.",
                "# TYPE traceviewer_overview_cache_total counter",
            ]
        )
        for outcome, count in snapshot["overview_cache_total"].items():
            lines.append(f'traceviewer_overview_cache_total{{outcome="{outcome}"}} {count}')
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class WindowRequest:
    start_sample: int
    end_sample: int
    viewport_px: int
    channels: tuple[int, ...]
    representation_requested: str

    @property
    def sample_count(self) -> int:
        return self.end_sample - self.start_sample


@dataclass(frozen=True)
class OverviewRequest:
    viewport_px: int
    channels: tuple[int, ...]


@dataclass(frozen=True)
class TileRequest:
    level_index: int
    tile_index: int
    channels: tuple[int, ...]


@dataclass(frozen=True)
class OverviewLevel:
    level_index: int
    bucket_count: int
    bucket_bounds: np.ndarray
    mins_by_channel: np.ndarray
    maxs_by_channel: np.ndarray

    @property
    def tile_count(self) -> int:
        return max(1, math.ceil(self.bucket_count / OVERVIEW_TILE_BUCKETS))


@dataclass(frozen=True)
class DetailStrategy:
    source: str
    representation_actual: str


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

    materialized = {key: value for key, value in payload.items() if key != "traces"}
    materialized["traces"] = traces
    return materialized


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
    def __init__(self, input_path: Path, *, pyramid_sidecar_path: Path | None = None):
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
        self.current_offset = float(self.attrs.get("current_offset", 0.0))
        self.current_units = str(self.attrs["current_units"])
        self.voltage_scale = float(self.attrs["voltage_scale"])
        self.voltage_offset = float(self.attrs.get("voltage_offset", 0.0))
        self.voltage_units = "mV"
        self.current_dtype_info = np.iinfo(self.current_arr.dtype)
        voltage_counts = np.asarray(self.voltage_arr[:, 0], dtype=np.float32)
        self.channel_voltage_mv = (voltage_counts * self.voltage_scale) + self.voltage_offset
        self.recording_id = normalize_recording_id(str(self.attrs.get("device_id") or input_path.stem))
        self.revision = self._compute_revision()
        self._overview_cache = LRUCache(OVERVIEW_CACHE_SIZE)
        self._cache_lock = threading.Lock()
        self.pyramid_sidecar_path = pyramid_sidecar_path or default_pyramid_sidecar_path(input_path)
        self.pyramid_origin = "startup"
        self._overview_levels = self._load_or_build_overview_levels()
        default_window_samples = int(
            min(
                self.sample_rate_hz * DEFAULT_DETAIL_SECONDS,
                DEFAULT_VIEWPORT_PX * DETAIL_MAX_SAMPLES_PER_PIXEL,
            )
        )
        default_window_start = max(0, (self.total_samples - default_window_samples) // 2)
        self._metadata = self._build_metadata(default_window_start, default_window_samples)

    def _build_metadata(self, default_window_start: int, default_window_samples: int) -> dict[str, Any]:
        pyramid_levels = [
            {
                "level_index": level.level_index,
                "bucket_count": level.bucket_count,
                "tile_bucket_count": OVERVIEW_TILE_BUCKETS,
                "tile_count": level.tile_count,
                "samples_per_bucket_estimate": round(self.total_samples / level.bucket_count, 6),
            }
            for level in self._overview_levels
        ]
        api_base = f"/v1/recordings/{self.recording_id}"
        revision_base = f"{api_base}/revisions/{self.revision}"
        return {
            "recording_id": self.recording_id,
            "revision": self.revision,
            "device_id": self.attrs.get("device_id"),
            "api_base": api_base,
            "revisioned_api_base": revision_base,
            "channels": self.total_channels,
            "channel_count": self.total_channels,
            "total_samples": self.total_samples,
            "sample_rate_hz": self.sample_rate_hz,
            "duration_sec": int(self.attrs.get("duration_sec")),
            "current_scale": self.current_scale,
            "current_offset": self.current_offset,
            "current_units": self.current_units,
            "current_count_min": int(self.current_dtype_info.min),
            "current_count_max": int(self.current_dtype_info.max),
            "voltage_scale": self.voltage_scale,
            "voltage_offset": self.voltage_offset,
            "voltage_units": self.voltage_units,
            "channel_voltage_mv": [round(float(value), 4) for value in self.channel_voltage_mv],
            "voltage_range_mv": {
                "min": round(float(self.channel_voltage_mv.min()), 4),
                "max": round(float(self.channel_voltage_mv.max()), 4),
            },
            "default_channels": list(range(min(DEFAULT_VISIBLE_CHANNELS, self.total_channels))),
            "default_window": {
                "start": default_window_start,
                "end": min(self.total_samples, default_window_start + default_window_samples),
            },
            "detail_threshold": {
                "samples_per_pixel": DETAIL_MAX_SAMPLES_PER_PIXEL,
                "seconds": round((DEFAULT_VIEWPORT_PX * DETAIL_MAX_SAMPLES_PER_PIXEL) / self.sample_rate_hz, 3),
            },
            "limits": {
                "max_viewport_px": MAX_VIEWPORT_PX,
                "max_channel_tokens": MAX_CHANNEL_TOKENS,
                "max_json_trace_points": MAX_JSON_TRACE_POINTS,
                "max_binary_response_bytes": MAX_BINARY_RESPONSE_BYTES,
                "raw_max_samples_per_pixel": DETAIL_MAX_SAMPLES_PER_PIXEL,
                "slice_to_pyramid_threshold_samples": MAX_DETAIL_SLICE_SAMPLES_PER_CHANNEL,
            },
            "pyramid": {
                "origin": self.pyramid_origin,
                "sidecar_path": str(self.pyramid_sidecar_path),
                "levels": pyramid_levels,
            },
            "representations": ["raw", "envelope", "auto"],
            "links": {
                "metadata": f"{api_base}/metadata",
                "overview": f"{revision_base}/overview",
                "detail": f"{revision_base}/detail",
                "envelope_tile_template": f"{revision_base}/envelope-tiles/{{level_index}}/{{tile_index}}",
            },
        }

    def metadata(self) -> dict[str, Any]:
        return self._metadata

    def metadata_etag(self) -> str:
        return self._etag_for("metadata", self.recording_id, self.revision)

    def overview_etag(self, request: OverviewRequest, *, response_format: str) -> str:
        channels_key = ",".join(str(channel) for channel in request.channels)
        return self._etag_for("overview", self.revision, request.viewport_px, channels_key, response_format)

    def detail_etag(self, request: WindowRequest, *, response_format: str) -> str:
        strategy = self.resolve_detail_strategy(request, response_format=response_format)
        channels_key = ",".join(str(channel) for channel in request.channels)
        return self._etag_for(
            "detail",
            self.revision,
            request.start_sample,
            request.end_sample,
            request.viewport_px,
            request.representation_requested,
            strategy.representation_actual,
            strategy.source,
            channels_key,
            response_format,
        )

    def tile_etag(self, request: TileRequest, *, response_format: str) -> str:
        channels_key = ",".join(str(channel) for channel in request.channels)
        return self._etag_for(
            "tile",
            self.revision,
            request.level_index,
            request.tile_index,
            channels_key,
            response_format,
        )

    def assert_recording(self, recording_id: str) -> None:
        if recording_id in {"active", "default", self.recording_id}:
            return
        raise ApiError(
            status=HTTPStatus.NOT_FOUND,
            type_name="recording-not-found",
            title="Recording not found",
            detail=f"unknown recording id: {recording_id}",
        )

    def assert_revision(self, revision: str) -> None:
        if revision != self.revision:
            raise ApiError(
                status=HTTPStatus.NOT_FOUND,
                type_name="revision-not-found",
                title="Revision not found",
                detail=f"unknown recording revision: {revision}",
            )

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
            current_offset = float(self.attrs.get("current_offset", 0.0))
            current_units = str(self.attrs["current_units"])
            voltage_scale = float(self.attrs["voltage_scale"])
            voltage_offset = float(self.attrs.get("voltage_offset", 0.0))
            duration_sec = float(self.attrs["duration_sec"])
        except KeyError as exc:
            raise ValueError(f"missing required recording attribute: {exc.args[0]}") from exc

        if not math.isfinite(sample_rate_hz) or sample_rate_hz <= 0:
            raise ValueError("sample_rate_hz must be a positive finite number")
        if not math.isfinite(current_scale) or current_scale <= 0:
            raise ValueError("current_scale must be a positive finite number")
        if not math.isfinite(current_offset):
            raise ValueError("current_offset must be a finite number")
        if not current_units:
            raise ValueError("current_units must be a non-empty string")
        if not math.isfinite(voltage_scale) or voltage_scale <= 0:
            raise ValueError("voltage_scale must be a positive finite number")
        if not math.isfinite(voltage_offset):
            raise ValueError("voltage_offset must be a finite number")
        if not math.isfinite(duration_sec) or duration_sec <= 0:
            raise ValueError("duration_sec must be a positive finite number")

    def _compute_revision(self) -> str:
        hasher = hashlib.sha256()
        hasher.update(str(self.input_path.resolve()).encode("utf-8"))
        hasher.update(json.dumps(self.attrs, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        hasher.update(str(self.current_arr.shape).encode("utf-8"))
        hasher.update(str(self.current_arr.dtype).encode("utf-8"))
        hasher.update(str(self.voltage_arr.shape).encode("utf-8"))
        hasher.update(str(self.voltage_arr.dtype).encode("utf-8"))
        for metadata_path in (
            self.input_path / "zarr.json",
            self.input_path / "current_data" / "zarr.json",
            self.input_path / "voltage_data" / "zarr.json",
        ):
            if metadata_path.exists():
                hasher.update(metadata_path.read_bytes())
        return hasher.hexdigest()[:16]

    def _load_or_build_overview_levels(self) -> tuple[OverviewLevel, ...]:
        sidecar_levels = self._load_pyramid_sidecar()
        if sidecar_levels is not None:
            self.pyramid_origin = "sidecar"
            return sidecar_levels

        built = self._build_overview_levels()
        self._write_pyramid_sidecar(built)
        self.pyramid_origin = "startup"
        return built

    def _load_pyramid_sidecar(self) -> tuple[OverviewLevel, ...] | None:
        if not self.pyramid_sidecar_path.exists():
            return None
        try:
            with np.load(self.pyramid_sidecar_path, allow_pickle=False) as archive:
                stored_revision = str(archive["revision"].item())
                if stored_revision != self.revision:
                    return None
                level_count = int(archive["level_count"].item())
                levels: list[OverviewLevel] = []
                for index in range(level_count):
                    prefix = f"level_{index}"
                    levels.append(
                        OverviewLevel(
                            level_index=index,
                            bucket_count=int(archive[f"{prefix}_bucket_count"].item()),
                            bucket_bounds=np.asarray(archive[f"{prefix}_bounds"], dtype=np.int64),
                            mins_by_channel=np.asarray(archive[f"{prefix}_mins"], dtype=np.int16),
                            maxs_by_channel=np.asarray(archive[f"{prefix}_maxs"], dtype=np.int16),
                        )
                    )
        except (OSError, KeyError, ValueError, EOFError, zipfile.BadZipFile, zlib.error):
            try:
                self.pyramid_sidecar_path.unlink(missing_ok=True)
            except OSError:
                pass
            return None
        return tuple(levels)

    def _write_pyramid_sidecar(self, levels: tuple[OverviewLevel, ...]) -> None:
        payload: dict[str, Any] = {
            "revision": np.array(self.revision),
            "level_count": np.array(len(levels), dtype=np.int32),
        }
        for level in levels:
            prefix = f"level_{level.level_index}"
            payload[f"{prefix}_bucket_count"] = np.array(level.bucket_count, dtype=np.int32)
            payload[f"{prefix}_bounds"] = level.bucket_bounds.astype(np.int64, copy=False)
            payload[f"{prefix}_mins"] = level.mins_by_channel.astype(np.int16, copy=False)
            payload[f"{prefix}_maxs"] = level.maxs_by_channel.astype(np.int16, copy=False)

        try:
            self.pyramid_sidecar_path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_path_raw = tempfile.mkstemp(
                prefix=f"{self.pyramid_sidecar_path.stem}.",
                suffix=".tmp",
                dir=self.pyramid_sidecar_path.parent,
            )
            temp_path = Path(temp_path_raw)
            try:
                with open(fd, "wb", closefd=True) as handle:
                    np.savez_compressed(handle, **payload)
                temp_path.replace(self.pyramid_sidecar_path)
            finally:
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError:
                        pass
        except OSError:
            return

    def _read_channel_slice(self, channel: int, start_sample: int, end_sample: int) -> np.ndarray:
        return np.asarray(self.current_arr[channel, start_sample:end_sample], dtype=np.int16)

    def _overview_bucket_counts(self) -> list[int]:
        if self.total_samples <= 0:
            return [1]

        target = min(
            self.total_samples,
            max(OVERVIEW_PYRAMID_MIN_BUCKETS, DEFAULT_VIEWPORT_PX * OVERVIEW_PYRAMID_MULTIPLIER),
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

        levels = []
        for index, count in enumerate(bucket_counts):
            levels.append(
                OverviewLevel(
                    level_index=index,
                    bucket_count=count,
                    bucket_bounds=bucket_bounds(self.total_samples, count),
                    mins_by_channel=np.stack(mins_by_count[count]).astype(np.int16, copy=False),
                    maxs_by_channel=np.stack(maxs_by_count[count]).astype(np.int16, copy=False),
                )
            )
        return tuple(levels)

    def _etag_for(self, *parts: Any) -> str:
        joined = "|".join(str(part) for part in parts)
        return quote_etag(hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16])

    def _query_value(self, query: dict[str, list[str]], *names: str) -> str | None:
        for name in names:
            values = query.get(name)
            if values:
                return values[0]
        return None

    def _parse_channels(self, query: dict[str, list[str]]) -> tuple[int, ...]:
        try:
            channels = parse_channel_list(self._query_value(query, "channels"), self.total_channels)
        except ValueError as exc:
            raise ApiError(
                status=HTTPStatus.BAD_REQUEST,
                type_name="invalid-parameter",
                title="Invalid parameter",
                detail=str(exc),
                invalid_params=[{"name": "channels", "reason": str(exc)}],
            ) from exc
        return tuple(channels)

    def parse_overview_request(self, query: dict[str, list[str]]) -> OverviewRequest:
        raw_viewport = self._query_value(query, "viewport_px", "width_px") or str(DEFAULT_VIEWPORT_PX)
        try:
            viewport_px = parse_positive_int(raw_viewport, "viewport_px")
        except ValueError as exc:
            raise ApiError(
                status=HTTPStatus.BAD_REQUEST,
                type_name="invalid-parameter",
                title="Invalid parameter",
                detail=str(exc),
                invalid_params=[{"name": "viewport_px", "reason": str(exc)}],
            ) from exc
        if viewport_px > MAX_VIEWPORT_PX:
            raise ApiError(
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                type_name="viewport-too-large",
                title="Viewport too large",
                detail=f"viewport_px too large: {viewport_px} (max {MAX_VIEWPORT_PX})",
                invalid_params=[{"name": "viewport_px", "reason": "exceeds configured maximum"}],
            )
        return OverviewRequest(viewport_px=viewport_px, channels=self._parse_channels(query))

    def parse_detail_request(self, query: dict[str, list[str]]) -> WindowRequest:
        try:
            start_sample = parse_strict_int(
                self._query_value(query, "start_sample", "start") or "0",
                "start_sample",
            )
            end_sample = parse_strict_int(
                self._query_value(query, "end_sample", "end") or str(self.total_samples),
                "end_sample",
            )
            viewport_px = parse_positive_int(
                self._query_value(query, "viewport_px", "width_px") or str(DEFAULT_VIEWPORT_PX),
                "viewport_px",
            )
        except ValueError as exc:
            detail = str(exc)
            invalid_name = "detail"
            if "start_sample" in detail:
                invalid_name = "start_sample"
            elif "end_sample" in detail:
                invalid_name = "end_sample"
            elif "viewport_px" in detail:
                invalid_name = "viewport_px"
            raise ApiError(
                status=HTTPStatus.BAD_REQUEST,
                type_name="invalid-parameter",
                title="Invalid parameter",
                detail=detail,
                invalid_params=[{"name": invalid_name, "reason": detail}],
            ) from exc

        representation_requested = (self._query_value(query, "representation") or "auto").strip().lower()
        if representation_requested not in {"auto", "raw", "envelope"}:
            raise ApiError(
                status=HTTPStatus.BAD_REQUEST,
                type_name="invalid-parameter",
                title="Invalid parameter",
                detail=f"invalid representation: {representation_requested!r}",
                invalid_params=[{"name": "representation", "reason": "must be one of auto, raw, envelope"}],
            )
        if viewport_px > MAX_VIEWPORT_PX:
            raise ApiError(
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                type_name="viewport-too-large",
                title="Viewport too large",
                detail=f"viewport_px too large: {viewport_px} (max {MAX_VIEWPORT_PX})",
                invalid_params=[{"name": "viewport_px", "reason": "exceeds configured maximum"}],
            )
        if start_sample < 0 or end_sample < 0:
            raise ApiError(
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                type_name="window-out-of-range",
                title="Window out of range",
                detail="sample windows must be non-negative",
                invalid_params=[{"name": "start_sample", "reason": "must be non-negative"}],
            )
        if start_sample >= end_sample:
            raise ApiError(
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                type_name="invalid-window",
                title="Invalid window",
                detail="end_sample must be greater than start_sample",
                invalid_params=[{"name": "end_sample", "reason": "must be greater than start_sample"}],
            )
        if end_sample > self.total_samples:
            raise ApiError(
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                type_name="window-out-of-range",
                title="Window out of range",
                detail=f"end_sample exceeds total_samples ({self.total_samples})",
                invalid_params=[{"name": "end_sample", "reason": "exceeds recording length"}],
            )
        return WindowRequest(
            start_sample=start_sample,
            end_sample=end_sample,
            viewport_px=viewport_px,
            channels=self._parse_channels(query),
            representation_requested=representation_requested,
        )

    def parse_tile_request(self, level_index: int, tile_index: int, query: dict[str, list[str]]) -> TileRequest:
        if level_index < 0 or level_index >= len(self._overview_levels):
            raise ApiError(
                status=HTTPStatus.NOT_FOUND,
                type_name="tile-level-not-found",
                title="Tile level not found",
                detail=f"unknown envelope tile level: {level_index}",
            )
        level = self._overview_levels[level_index]
        if tile_index < 0 or tile_index >= level.tile_count:
            raise ApiError(
                status=HTTPStatus.NOT_FOUND,
                type_name="tile-not-found",
                title="Tile not found",
                detail=f"unknown tile index {tile_index} for level {level_index}",
            )
        return TileRequest(level_index=level_index, tile_index=tile_index, channels=self._parse_channels(query))

    def _fits_response_budget(self, request: WindowRequest, mode: str, response_format: str) -> bool:
        point_count = estimate_trace_point_count(mode, request.sample_count, request.viewport_px, len(request.channels))
        if response_format == "json":
            return point_count <= MAX_JSON_TRACE_POINTS
        return point_count * np.dtype(np.int16).itemsize <= MAX_BINARY_RESPONSE_BYTES

    def _enforce_response_budget(self, request: WindowRequest, mode: str, response_format: str) -> None:
        if self._fits_response_budget(request, mode, response_format):
            return
        if response_format == "json":
            raise ApiError(
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                type_name="response-too-large",
                title="Response too large",
                detail=(
                    "requested response is too large for JSON; reduce viewport_px, channels, or window span, "
                    "or request the binary media type"
                ),
            )
        raise ApiError(
            status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            type_name="response-too-large",
            title="Response too large",
            detail="requested response is too large; reduce viewport_px, channels, or window span",
        )

    def resolve_detail_strategy(self, request: WindowRequest, *, response_format: str) -> DetailStrategy:
        actual = "raw" if samples_per_pixel(request.sample_count, request.viewport_px) <= DETAIL_MAX_SAMPLES_PER_PIXEL else "envelope"
        if request.representation_requested == "raw":
            actual = "raw"
        elif request.representation_requested == "envelope":
            actual = "envelope"

        if actual == "raw" and samples_per_pixel(request.sample_count, request.viewport_px) > DETAIL_MAX_SAMPLES_PER_PIXEL:
            raise ApiError(
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
                type_name="raw-window-too-wide",
                title="Raw representation not allowed",
                detail=(
                    f"raw responses require samples_per_pixel <= {DETAIL_MAX_SAMPLES_PER_PIXEL}; "
                    f"got {samples_per_pixel(request.sample_count, request.viewport_px):.3f}"
                ),
            )

        if actual == "raw" and not self._fits_response_budget(request, "raw", response_format):
            if request.representation_requested == "auto":
                actual = "envelope"
            else:
                self._enforce_response_budget(request, "raw", response_format)

        if actual == "raw":
            return DetailStrategy(source="raw_window", representation_actual="raw")
        if request.sample_count > MAX_DETAIL_SLICE_SAMPLES_PER_CHANNEL:
            return DetailStrategy(source="envelope_pyramid", representation_actual="envelope")
        return DetailStrategy(source="envelope_slice", representation_actual="envelope")

    def _select_overview_level(self, span_samples: int, viewport_px: int) -> OverviewLevel:
        desired_local_buckets = max(1, min(span_samples, math.ceil(viewport_px * PYRAMID_VIEWPORT_OVERSAMPLE)))
        for level in self._overview_levels:
            local_bucket_estimate = math.ceil((span_samples / self.total_samples) * level.bucket_count)
            if local_bucket_estimate >= desired_local_buckets:
                return level
        return self._overview_levels[-1]

    def _window_bucket_slice(self, level: OverviewLevel, start_sample: int, end_sample: int) -> tuple[int, int]:
        start_index = int(np.searchsorted(level.bucket_bounds, start_sample, side="right") - 1)
        start_index = max(0, min(start_index, level.bucket_count - 1))
        end_index = int(np.searchsorted(level.bucket_bounds, end_sample, side="left"))
        end_index = max(start_index + 1, min(end_index, level.bucket_count))
        return start_index, end_index

    def _build_trace_base(
        self,
        *,
        source: str,
        mode: str,
        channels: tuple[int, ...],
        start_sample: int,
        end_sample: int,
        viewport_px: int,
        representation_requested: str,
    ) -> dict[str, Any]:
        sample_count = end_sample - start_sample
        return {
            "recording_id": self.recording_id,
            "revision": self.revision,
            "representation_requested": representation_requested,
            "representation_actual": mode,
            "mode": mode,
            "source": source,
            "start_sample": start_sample,
            "end_sample": end_sample,
            "start": start_sample,
            "end": end_sample,
            "sample_count": sample_count,
            "seconds": round(sample_count / self.sample_rate_hz, 3),
            "samples_per_pixel": round(samples_per_pixel(sample_count, viewport_px), 3),
            "viewport_px": viewport_px,
            "width_px": viewport_px,
            "channels": list(channels),
            "channel_ids": list(channels),
            "current_scale": self.current_scale,
            "current_units": self.current_units,
        }

    def _build_pyramid_payload(
        self,
        request: WindowRequest,
        *,
        source: str,
        representation_requested: str,
        cache_status: str | None = None,
    ) -> dict[str, Any]:
        level = self._select_overview_level(request.sample_count, request.viewport_px)
        start_index, end_index = self._window_bucket_slice(level, request.start_sample, request.end_sample)
        traces = []
        for channel in request.channels:
            level_mins = level.mins_by_channel[channel, start_index:end_index]
            level_maxs = level.maxs_by_channel[channel, start_index:end_index]
            bucket_count = max(1, min(request.viewport_px, level_mins.shape[0]))
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

        response = self._build_trace_base(
            source=source,
            mode="envelope",
            channels=request.channels,
            start_sample=request.start_sample,
            end_sample=request.end_sample,
            viewport_px=request.viewport_px,
            representation_requested=representation_requested,
        )
        response.update(
            {
                "bucket_count": traces[0]["mins"].shape[0] if traces else 0,
                "pyramid_level_index": level.level_index,
                "pyramid_level_bucket_count": level.bucket_count,
                "pyramid_origin": self.pyramid_origin,
                "traces": traces,
            }
        )
        if cache_status is not None:
            response["cache_status"] = cache_status
            response["cache"] = cache_status
        return response

    def _build_detail_payload(self, request: WindowRequest, *, response_format: str) -> dict[str, Any]:
        strategy = self.resolve_detail_strategy(request, response_format=response_format)
        self._enforce_response_budget(request, strategy.representation_actual, response_format)

        if strategy.source == "envelope_pyramid":
            return self._build_pyramid_payload(
                request,
                source="envelope_pyramid",
                representation_requested=request.representation_requested,
            )

        traces = []
        for channel in request.channels:
            data = self._read_channel_slice(channel, request.start_sample, request.end_sample)
            if strategy.representation_actual == "raw":
                traces.append(
                    {
                        "channel": channel,
                        "values": data,
                        "min_count": int(data.min()) if data.size else 0,
                        "max_count": int(data.max()) if data.size else 0,
                    }
                )
                continue

            bucket_count = max(1, min(request.viewport_px, request.sample_count))
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

        response = self._build_trace_base(
            source=strategy.source,
            mode=strategy.representation_actual,
            channels=request.channels,
            start_sample=request.start_sample,
            end_sample=request.end_sample,
            viewport_px=request.viewport_px,
            representation_requested=request.representation_requested,
        )
        response["traces"] = traces
        if strategy.representation_actual == "envelope":
            response["bucket_count"] = max(1, min(request.viewport_px, request.sample_count))
        return response

    def _build_overview_payload(self, request: OverviewRequest, *, cache_status: str | None = None) -> dict[str, Any]:
        overview_request = WindowRequest(
            start_sample=0,
            end_sample=self.total_samples,
            viewport_px=request.viewport_px,
            channels=request.channels,
            representation_requested="envelope",
        )
        payload = self._build_pyramid_payload(
            overview_request,
            source="overview_pyramid",
            representation_requested="envelope",
            cache_status=cache_status,
        )
        payload["overview_scope"] = "full_recording"
        payload["links"] = {
            "detail": f"/v1/recordings/{self.recording_id}/revisions/{self.revision}/detail",
        }
        return payload

    def _build_tile_payload(self, request: TileRequest) -> dict[str, Any]:
        level = self._overview_levels[request.level_index]
        bucket_start = request.tile_index * OVERVIEW_TILE_BUCKETS
        bucket_end = min(level.bucket_count, bucket_start + OVERVIEW_TILE_BUCKETS)
        start_sample = int(level.bucket_bounds[bucket_start])
        end_sample = int(level.bucket_bounds[bucket_end])
        traces = []
        for channel in request.channels:
            mins = level.mins_by_channel[channel, bucket_start:bucket_end]
            maxs = level.maxs_by_channel[channel, bucket_start:bucket_end]
            traces.append(
                {
                    "channel": channel,
                    "mins": mins,
                    "maxs": maxs,
                    "min_count": int(mins.min()) if mins.size else 0,
                    "max_count": int(maxs.max()) if maxs.size else 0,
                }
            )

        response = self._build_trace_base(
            source="envelope_tile",
            mode="envelope",
            channels=request.channels,
            start_sample=start_sample,
            end_sample=end_sample,
            viewport_px=bucket_end - bucket_start,
            representation_requested="envelope",
        )
        response.update(
            {
                "bucket_count": bucket_end - bucket_start,
                "level_index": level.level_index,
                "level_bucket_count": level.bucket_count,
                "tile_index": request.tile_index,
                "tile_count": level.tile_count,
                "tile_bucket_count": OVERVIEW_TILE_BUCKETS,
                "pyramid_origin": self.pyramid_origin,
                "samples_per_bucket_estimate": round(self.total_samples / level.bucket_count, 6),
                "traces": traces,
            }
        )
        return response

    def overview(self, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self.parse_overview_request(query)
        cache_key = ("overview", request.viewport_px, request.channels)
        with self._cache_lock:
            cached = self._overview_cache.get(cache_key)
        if cached is not None:
            return jsonify_trace_payload({**cached, "cache_status": "hit", "cache": "hit"})

        response = self._build_overview_payload(request, cache_status="miss")
        with self._cache_lock:
            self._overview_cache.set(cache_key, response)
        return jsonify_trace_payload(response)

    def overview_binary(self, query: dict[str, list[str]]) -> bytes:
        request = self.parse_overview_request(query)
        cache_key = ("overview", request.viewport_px, request.channels)
        with self._cache_lock:
            cached = self._overview_cache.get(cache_key)
        if cached is not None:
            return encode_trace_payload({**cached, "cache_status": "hit", "cache": "hit"})

        response = self._build_overview_payload(request, cache_status="miss")
        with self._cache_lock:
            self._overview_cache.set(cache_key, response)
        return encode_trace_payload(response)

    def detail(self, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self.parse_detail_request(query)
        return jsonify_trace_payload(self._build_detail_payload(request, response_format="json"))

    def detail_binary(self, query: dict[str, list[str]]) -> bytes:
        request = self.parse_detail_request(query)
        return encode_trace_payload(self._build_detail_payload(request, response_format="binary"))

    def envelope_tile(self, level_index: int, tile_index: int, query: dict[str, list[str]]) -> dict[str, Any]:
        request = self.parse_tile_request(level_index, tile_index, query)
        return jsonify_trace_payload(self._build_tile_payload(request))

    def envelope_tile_binary(self, level_index: int, tile_index: int, query: dict[str, list[str]]) -> bytes:
        request = self.parse_tile_request(level_index, tile_index, query)
        return encode_trace_payload(self._build_tile_payload(request))

    def ready(self) -> dict[str, Any]:
        return {
            "ok": True,
            "recording_id": self.recording_id,
            "revision": self.revision,
            "pyramid_origin": self.pyramid_origin,
            "channels": self.total_channels,
            "total_samples": self.total_samples,
        }

    def metrics_snapshot(self, metrics: ServerMetrics, *, detail_slots_available: int) -> dict[str, Any]:
        snapshot = metrics.snapshot(
            detail_slots_available=detail_slots_available,
            overview_cache_size=self._overview_cache.size(),
        )
        snapshot["recording_id"] = self.recording_id
        snapshot["revision"] = self.revision
        snapshot["pyramid_origin"] = self.pyramid_origin
        return snapshot

    def metrics_prometheus(self, metrics: ServerMetrics, *, detail_slots_available: int) -> str:
        return metrics.prometheus(
            detail_slots_available=detail_slots_available,
            overview_cache_size=self._overview_cache.size(),
        )


class TraceViewerHandler(BaseHTTPRequestHandler):
    server_version = "TraceViewer/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path in {"/livez", "/health"}:
                self._handle_json_endpoint(
                    endpoint="livez",
                    payload_factory=lambda: {"ok": True},
                    etag=None,
                    cache_control=CACHE_CONTROL_HEALTH,
                )
                return
            if parsed.path == "/readyz":
                self._handle_json_endpoint(
                    endpoint="readyz",
                    payload_factory=self.server.data_service.ready,
                    etag=self.server.data_service.metadata_etag(),
                    cache_control=CACHE_CONTROL_HEALTH,
                )
                return
            if parsed.path == "/metrics":
                self._handle_text_endpoint(
                    endpoint="metrics",
                    body_factory=lambda: self.server.data_service.metrics_prometheus(
                        self.server.metrics,
                        detail_slots_available=getattr(self.server.detail_slots, "_value", 0),
                    ),
                    cache_control=CACHE_CONTROL_HEALTH,
                )
                return
            if parsed.path == "/api/metadata":
                self._handle_json_endpoint(
                    endpoint="metadata",
                    payload_factory=self.server.data_service.metadata,
                    etag=self.server.data_service.metadata_etag(),
                    cache_control=CACHE_CONTROL_METADATA,
                )
                return
            if parsed.path == "/api/overview":
                self._handle_overview(query, endpoint="overview", cache_control=CACHE_CONTROL_OVERVIEW)
                return
            if parsed.path == "/api/detail":
                self._handle_detail(query, endpoint="detail", cache_control=CACHE_CONTROL_DETAIL)
                return

            routed = self._match_v1_route(parsed.path)
            if routed is not None:
                route_name, path_params = routed
                if route_name == "metadata":
                    self.server.data_service.assert_recording(path_params["recording_id"])
                    self._handle_json_endpoint(
                        endpoint="metadata",
                        payload_factory=self.server.data_service.metadata,
                        etag=self.server.data_service.metadata_etag(),
                        cache_control=CACHE_CONTROL_METADATA,
                    )
                    return
                if route_name == "overview":
                    self.server.data_service.assert_recording(path_params["recording_id"])
                    self.server.data_service.assert_revision(path_params["revision"])
                    self._handle_overview(query, endpoint="overview", cache_control=CACHE_CONTROL_OVERVIEW)
                    return
                if route_name == "detail":
                    self.server.data_service.assert_recording(path_params["recording_id"])
                    self.server.data_service.assert_revision(path_params["revision"])
                    self._handle_detail(query, endpoint="detail", cache_control=CACHE_CONTROL_DETAIL)
                    return
                if route_name == "tile":
                    self.server.data_service.assert_recording(path_params["recording_id"])
                    self.server.data_service.assert_revision(path_params["revision"])
                    self._handle_tile(
                        level_index=path_params["level_index"],
                        tile_index=path_params["tile_index"],
                        query=query,
                        endpoint="envelope_tile",
                        cache_control=CACHE_CONTROL_TILE,
                    )
                    return

            self._serve_static(parsed.path)
        except ApiError as exc:
            if exc.status == HTTPStatus.SERVICE_UNAVAILABLE:
                self.server.metrics.record_detail_rejection()
            self._write_problem(exc, endpoint=self._classify_endpoint(parsed.path))
        except FileNotFoundError as exc:
            self._write_problem(
                ApiError(
                    status=HTTPStatus.NOT_FOUND,
                    type_name="not-found",
                    title="Not found",
                    detail=str(exc),
                ),
                endpoint=self._classify_endpoint(parsed.path),
            )
        except Exception as exc:  # pragma: no cover
            self._write_problem(
                ApiError(
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    type_name="internal-error",
                    title="Internal error",
                    detail=f"internal error: {exc}",
                ),
                endpoint=self._classify_endpoint(parsed.path),
            )

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _classify_endpoint(self, path: str) -> str:
        if path.endswith("/metadata") or path == "/api/metadata":
            return "metadata"
        if path.endswith("/overview") or path == "/api/overview":
            return "overview"
        if path.endswith("/detail") or path == "/api/detail":
            return "detail"
        if "/envelope-tiles/" in path:
            return "envelope_tile"
        if path in {"/livez", "/health"}:
            return "livez"
        if path == "/readyz":
            return "readyz"
        if path == "/metrics":
            return "metrics"
        return "static"

    def _match_v1_route(self, path: str) -> tuple[str, dict[str, Any]] | None:
        stripped = path.strip("/")
        parts = stripped.split("/") if stripped else []
        if len(parts) == 4 and parts[:2] == ["v1", "recordings"] and parts[3] == "metadata":
            return ("metadata", {"recording_id": parts[2]})
        if len(parts) == 6 and parts[:2] == ["v1", "recordings"] and parts[3] == "revisions":
            if parts[5] == "overview":
                return ("overview", {"recording_id": parts[2], "revision": parts[4]})
            if parts[5] == "detail":
                return ("detail", {"recording_id": parts[2], "revision": parts[4]})
        if len(parts) == 8 and parts[:2] == ["v1", "recordings"] and parts[3] == "revisions":
            if parts[5] == "envelope-tiles":
                try:
                    level_index = parse_strict_int(parts[6], "level_index")
                    tile_index = parse_strict_int(parts[7], "tile_index")
                except ValueError as exc:
                    raise ApiError(
                        status=HTTPStatus.BAD_REQUEST,
                        type_name="invalid-parameter",
                        title="Invalid parameter",
                        detail=str(exc),
                    ) from exc
                return (
                    "tile",
                    {
                        "recording_id": parts[2],
                        "revision": parts[4],
                        "level_index": level_index,
                        "tile_index": tile_index,
                    },
                )
        return None

    def _negotiate_trace_format(self, query: dict[str, list[str]]) -> str:
        raw_format = query.get("format", [None])[0]
        if raw_format is not None:
            lowered = raw_format.strip().lower()
            if lowered == "binary":
                return "binary"
            if lowered == "json":
                return "json"
            raise ApiError(
                status=HTTPStatus.BAD_REQUEST,
                type_name="invalid-parameter",
                title="Invalid parameter",
                detail=f"invalid format: {raw_format!r}",
                invalid_params=[{"name": "format", "reason": "must be json or binary"}],
            )

        accept = self.headers.get("Accept", "*/*")
        if TRACE_BINARY_CONTENT_TYPE in accept or LEGACY_TRACE_BINARY_CONTENT_TYPE in accept:
            return "binary"
        if "*/*" in accept or "application/json" in accept or not accept.strip():
            return "json"
        raise ApiError(
            status=HTTPStatus.NOT_ACCEPTABLE,
            type_name="unsupported-accept",
            title="Unsupported Accept header",
            detail=(
                "trace endpoints support application/json or "
                f"{TRACE_BINARY_CONTENT_TYPE}"
            ),
        )

    def _handle_json_endpoint(
        self,
        *,
        endpoint: str,
        payload_factory: Any,
        etag: str | None,
        cache_control: str,
    ) -> None:
        if etag is not None and matches_if_none_match(self.headers.get("If-None-Match"), etag):
            self.server.metrics.record_not_modified()
            self._write_not_modified(
                endpoint=endpoint,
                headers={
                    "ETag": etag,
                    "Cache-Control": cache_control,
                },
            )
            return
        payload = payload_factory()
        self._write_json(
            payload,
            endpoint=endpoint,
            headers={
                "Cache-Control": cache_control,
                **({"ETag": etag} if etag is not None else {}),
            },
        )

    def _handle_text_endpoint(
        self,
        *,
        endpoint: str,
        body_factory: Any,
        cache_control: str,
    ) -> None:
        body = body_factory().encode("utf-8")
        self._write_bytes(
            body,
            endpoint=endpoint,
            content_type=PROMETHEUS_CONTENT_TYPE,
            headers={"Cache-Control": cache_control},
        )

    def _handle_overview(self, query: dict[str, list[str]], *, endpoint: str, cache_control: str) -> None:
        response_format = self._negotiate_trace_format(query)
        request = self.server.data_service.parse_overview_request(query)
        etag = self.server.data_service.overview_etag(request, response_format=response_format)
        common_headers = {
            "Cache-Control": cache_control,
            "ETag": etag,
            "Vary": "Accept",
        }
        if matches_if_none_match(self.headers.get("If-None-Match"), etag):
            self.server.metrics.record_not_modified()
            self._write_not_modified(endpoint=endpoint, headers=common_headers)
            return

        cache_key = ("overview", request.viewport_px, request.channels)
        with self.server.data_service._cache_lock:
            cached = self.server.data_service._overview_cache.get(cache_key)
        if cached is not None:
            self.server.metrics.record_overview_cache("hit")
            if response_format == "binary":
                self._write_binary(
                    encode_trace_payload({**cached, "cache_status": "hit", "cache": "hit"}),
                    endpoint=endpoint,
                    headers=common_headers,
                )
            else:
                self._write_json(
                    jsonify_trace_payload({**cached, "cache_status": "hit", "cache": "hit"}),
                    endpoint=endpoint,
                    headers=common_headers,
                )
            return

        self.server.metrics.record_overview_cache("miss")
        payload = self.server.data_service._build_overview_payload(request, cache_status="miss")
        with self.server.data_service._cache_lock:
            self.server.data_service._overview_cache.set(cache_key, payload)
        if response_format == "binary":
            self._write_binary(encode_trace_payload(payload), endpoint=endpoint, headers=common_headers)
        else:
            self._write_json(jsonify_trace_payload(payload), endpoint=endpoint, headers=common_headers)

    def _handle_detail(self, query: dict[str, list[str]], *, endpoint: str, cache_control: str) -> None:
        response_format = self._negotiate_trace_format(query)
        request = self.server.data_service.parse_detail_request(query)
        etag = self.server.data_service.detail_etag(request, response_format=response_format)
        common_headers = {
            "Cache-Control": cache_control,
            "ETag": etag,
            "Vary": "Accept",
        }
        if matches_if_none_match(self.headers.get("If-None-Match"), etag):
            self.server.metrics.record_not_modified()
            self._write_not_modified(endpoint=endpoint, headers=common_headers)
            return

        if not self.server.detail_slots.acquire(timeout=DETAIL_SLOT_ACQUIRE_TIMEOUT_SEC):
            raise ApiError(
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                type_name="detail-overloaded",
                title="Server busy",
                detail="server is busy processing other detail requests",
                headers={"Retry-After": "1"},
            )
        try:
            payload = self.server.data_service._build_detail_payload(request, response_format=response_format)
        finally:
            self.server.detail_slots.release()

        if response_format == "binary":
            self._write_binary(encode_trace_payload(payload), endpoint=endpoint, headers=common_headers)
        else:
            self._write_json(jsonify_trace_payload(payload), endpoint=endpoint, headers=common_headers)

    def _handle_tile(
        self,
        *,
        level_index: int,
        tile_index: int,
        query: dict[str, list[str]],
        endpoint: str,
        cache_control: str,
    ) -> None:
        response_format = self._negotiate_trace_format(query)
        request = self.server.data_service.parse_tile_request(level_index, tile_index, query)
        etag = self.server.data_service.tile_etag(request, response_format=response_format)
        headers = {
            "Cache-Control": cache_control,
            "ETag": etag,
            "Vary": "Accept",
        }
        if matches_if_none_match(self.headers.get("If-None-Match"), etag):
            self.server.metrics.record_not_modified()
            self._write_not_modified(endpoint=endpoint, headers=headers)
            return
        payload = self.server.data_service._build_tile_payload(request)
        if response_format == "binary":
            self._write_binary(encode_trace_payload(payload), endpoint=endpoint, headers=headers)
        else:
            self._write_json(jsonify_trace_payload(payload), endpoint=endpoint, headers=headers)

    def _write_json(
        self,
        payload: dict[str, Any],
        *,
        endpoint: str = "unknown",
        status: HTTPStatus = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._write_bytes(body, endpoint=endpoint, status=status, content_type=content_type, headers=headers)

    def _write_problem(self, error: ApiError, *, endpoint: str) -> None:
        headers = {"Cache-Control": CACHE_CONTROL_HEALTH, **error.headers}
        self._write_json(
            error.to_problem(),
            endpoint=endpoint,
            status=error.status,
            headers=headers,
            content_type=PROBLEM_JSON_CONTENT_TYPE,
        )

    def _write_binary(
        self,
        payload: bytes,
        *,
        endpoint: str,
        status: HTTPStatus = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        merged_headers = {"Content-Type": TRACE_BINARY_CONTENT_TYPE, **(headers or {})}
        self._write_bytes(payload, endpoint=endpoint, status=status, content_type=None, headers=merged_headers)

    def _write_not_modified(self, *, endpoint: str, headers: dict[str, str]) -> None:
        try:
            self.send_response(HTTPStatus.NOT_MODIFIED)
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
        except OSError as exc:
            if is_client_disconnect_error(exc):
                return
            raise
        self.server.metrics.record_request(endpoint, HTTPStatus.NOT_MODIFIED)

    def _write_bytes(
        self,
        payload: bytes,
        *,
        endpoint: str,
        status: HTTPStatus = HTTPStatus.OK,
        content_type: str | None,
        headers: dict[str, str] | None = None,
    ) -> None:
        merged_headers = dict(headers or {})
        if content_type is not None:
            merged_headers["Content-Type"] = content_type
        merged_headers["Content-Length"] = str(len(payload))
        try:
            self.send_response(status)
            for name, value in merged_headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(payload)
        except OSError as exc:
            if is_client_disconnect_error(exc):
                return
            raise
        self.server.metrics.record_request(endpoint, status)
        self.server.metrics.record_response_bytes(len(payload))

    def _serve_static(self, raw_path: str) -> None:
        candidate = resolve_static_path(raw_path)
        if candidate is None:
            raise ApiError(
                status=HTTPStatus.NOT_FOUND,
                type_name="not-found",
                title="Not found",
                detail=f"unknown path: {raw_path}",
            )
        body = candidate.read_bytes()
        content_type, _ = mimetypes.guess_type(candidate.name)
        self._write_bytes(
            body,
            endpoint="static",
            status=HTTPStatus.OK,
            content_type=content_type or "application/octet-stream",
            headers={"Cache-Control": CACHE_CONTROL_STATIC},
        )


class TraceViewerServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    request_queue_size = 64

    def __init__(self, server_address: tuple[str, int], data_service: TraceDataService):
        super().__init__(server_address, TraceViewerHandler)
        self.data_service = data_service
        self.metrics = ServerMetrics()
        self.detail_slots = threading.BoundedSemaphore(MAX_CONCURRENT_DETAIL_REQUESTS)

    def get_request(self) -> tuple[Any, Any]:
        request, client_address = super().get_request()
        request.settimeout(REQUEST_SOCKET_TIMEOUT_SEC)
        return request, client_address


def run_server(
    host: str,
    port: int,
    input_path: Path,
    generate_if_missing: bool,
    allow_remote: bool,
    *,
    pyramid_sidecar_path: Path | None = None,
    build_pyramid_only: bool = False,
) -> None:
    validate_bind_host(host, allow_remote)
    ensure_recording_exists(input_path, generate_if_missing)
    data_service = TraceDataService(input_path, pyramid_sidecar_path=pyramid_sidecar_path)
    if build_pyramid_only:
        print(f"Wrote pyramid sidecar to {data_service.pyramid_sidecar_path}")
        print(f"Recording revision: {data_service.revision}")
        return

    httpd = TraceViewerServer((host, port), data_service)
    print(f"Serving trace viewer at http://{host}:{port}")
    print(f"Recording id: {data_service.recording_id}")
    print(f"Recording revision: {data_service.revision}")
    print(f"Reading from {input_path}")
    print(f"Pyramid source: {data_service.pyramid_origin} ({data_service.pyramid_sidecar_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input.resolve()
    run_server(
        args.host,
        args.port,
        input_path,
        args.generate_if_missing,
        args.allow_remote,
        pyramid_sidecar_path=args.pyramid_sidecar,
        build_pyramid_only=args.build_pyramid_only,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
