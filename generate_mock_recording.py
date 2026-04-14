"""
Generate the mock recording zarr file used by this exercise.

Produces a 48-channel x 2.5 kHz x 1.5 h synthetic nanopore recording at
./mock48_2500hz_1.5h.zarr, matching the on-disk layout that Portal's
internal library uses to write real recordings.

Usage:
    python generate_mock_recording.py [output_path]

Determinism:  the script seeds random/numpy with a fixed value so the
output is byte-stable across runs on the same machine.
"""
from __future__ import annotations

import math
import os
import random
import sys
import time
from pathlib import Path
from typing import List

import numpy as np
import zarr
from zarr.codecs import BloscCodec, BloscShuffle


# ---------------------------------------------------------------------------
    # Chunking / codec layout
# ---------------------------------------------------------------------------
CURRENT_SAMPLES_PER_LOGICAL_CHUNK = 50_000
VOLTAGE_SAMPLES_PER_LOGICAL_CHUNK = 300_000
LOGICAL_CHUNKS_PER_SHARD_TIME     = 32

CURRENT_SHARD_SAMPLES = CURRENT_SAMPLES_PER_LOGICAL_CHUNK * LOGICAL_CHUNKS_PER_SHARD_TIME  # 1_600_000
VOLTAGE_SHARD_SAMPLES = VOLTAGE_SAMPLES_PER_LOGICAL_CHUNK * LOGICAL_CHUNKS_PER_SHARD_TIME  # 9_600_000


# ---------------------------------------------------------------------------
# Recording configuration
# ---------------------------------------------------------------------------
N_CHANNELS         = 48
SAMPLE_RATE_HZ     = 2500.0
DURATION_SEC       = int(1.5 * 3600)        # 5400 s
N_SAMPLES          = int(SAMPLE_RATE_HZ * DURATION_SEC)   # 13_500_000

HOLD_VOLTAGE_MV    = 180.0                   # measurement potential
MEMBRANE_NOISE_NA  = 0.002                   # SD of additive current noise (pA)
LEAKY_FRACTION     = 0.20                    # fraction of channels with a leak
LEAK_NS            = 150.0
OFFSET_MV_SIGMA    = 2.5                     # per-channel voltage offset SD

CURRENT_RANGE_NA   = 2.0
CURRENT_STEP_PA    = (CURRENT_RANGE_NA * 1e-9) / 32768.0 * 1e12   # pA / int16 unit
VOLTAGE_STEP_MV    = 0.0625                                       # mV / int16 unit

# The per-chunk generation loop
CHUNK_SECONDS      = 120                                            # -> 300_000 samples
SAMPLES_PER_CHUNK  = int(CHUNK_SECONDS * SAMPLE_RATE_HZ)
assert SAMPLES_PER_CHUNK % VOLTAGE_SAMPLES_PER_LOGICAL_CHUNK == 0
assert SAMPLES_PER_CHUNK % CURRENT_SAMPLES_PER_LOGICAL_CHUNK == 0
N_CHUNKS           = N_SAMPLES // SAMPLES_PER_CHUNK                # 45


# ---------------------------------------------------------------------------
# Pore state machine
# ---------------------------------------------------------------------------
def _matrix_exp(m: np.ndarray) -> np.ndarray:
    y = np.zeros(m.shape)
    for k in range(5):
        y += np.linalg.matrix_power(m, k) / math.factorial(k)
    return y


class PoreState:
    def __init__(self, name: str, g: float, v: float):
        self.name, self.g, self.sd = name, g, v ** 0.5

    def get_sample(self) -> float:
        return random.gauss(self.g, self.sd)


class PoreSimulator:
    def __init__(self, states: List[PoreState]):
        self.states = states
        self.state_indices = {s.name: i for i, s in enumerate(states)}
        self.n_state = len(states)
        self.state_i = 0
        self.q_matrix = np.zeros((self.n_state, self.n_state))
        self.set_sample_rate(1000.0)

    def set_transition_rate(self, frm: str, to: str, rate: float):
        self.q_matrix[self.state_indices[frm], self.state_indices[to]] = rate

    def set_sample_rate(self, sample_rate_hz: float):
        for i in range(self.n_state):
            self.q_matrix[i, i] = -np.sum(self.q_matrix[i, :])
        self.p_matrix = _matrix_exp(self.q_matrix / sample_rate_hz)
        self.c_matrix = np.cumsum(self.p_matrix, axis=1)

    def set_initial_state(self, name: str):
        self.state_i = self.state_indices[name]

    def generate_sample(self) -> float:
        s = self.states[self.state_i].get_sample()
        p = random.random()
        for j in range(self.n_state):
            if p < self.c_matrix[self.state_i, j]:
                self.state_i = j
                break
        return s


def make_pore_simulator(sample_rate_hz: float,
                        pore_insertion_probability: float = 0.6,
                        conductance_sd_percent: float = 1.2) -> PoreSimulator:
    sim = PoreSimulator([
        PoreState("uninserted", g=0.0, v=0.0),
        PoreState("closed",     g=0.0, v=0.0001),
        PoreState("blocked",    g=0.5, v=0.0025),
        PoreState("open",       g=3.5, v=0.006),
    ])
    p = 1 + 0.01 * random.gauss(0, conductance_sd_percent)
    for s in sim.states:
        s.g *= p
    insertion_rate = 0.015 if random.random() < pore_insertion_probability else 0.001
    sim.set_transition_rate("uninserted", "closed", insertion_rate)
    sim.set_transition_rate("closed",     "open",    0.06)
    sim.set_transition_rate("open",       "closed",  0.02)
    sim.set_transition_rate("open",       "blocked", 0.2)
    sim.set_transition_rate("blocked",    "open",    0.3)
    sim.set_initial_state("uninserted")
    sim.set_sample_rate(sample_rate_hz)
    return sim


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(out_path: Path):
    if out_path.exists():
        raise SystemExit(f"refusing to overwrite existing file: {out_path}")

    random.seed(20260413)
    np.random.seed(20260413)

    # ---- Per-channel setup ----
    # Build each channel's simulator using the same construction path as the
    # scalar version (conductance variation, insertion rate, initial state),
    # then stack their state into batched arrays for the vectorised inner loop.
    pore_simulators = [make_pore_simulator(SAMPLE_RATE_HZ) for _ in range(N_CHANNELS)]
    leaks_ns    = np.array([LEAK_NS if random.random() < LEAKY_FRACTION else 0.0
                            for _ in range(N_CHANNELS)])
    offsets_mv  = np.array([random.gauss(0.0, OFFSET_MV_SIGMA)
                            for _ in range(N_CHANNELS)])
    applied_voltage_v = (HOLD_VOLTAGE_MV - offsets_mv) / 1000.0    # (48,)
    voltage_int16     = np.clip(
        (HOLD_VOLTAGE_MV + offsets_mv) / VOLTAGE_STEP_MV, -32768, 32767
    ).astype(np.int16)

    # Batched transition/emission state.  All simulators share n_state=4.
    n_state = pore_simulators[0].n_state
    cum_mat = np.stack([s.c_matrix for s in pore_simulators], axis=0)   # (48, 4, 4)
    state_g = np.stack([np.array([st.g  for st in s.states]) for s in pore_simulators])
    state_sd = np.stack([np.array([st.sd for st in s.states]) for s in pore_simulators])
    state_idx = np.array([s.state_i for s in pore_simulators], dtype=np.int64)   # (48,)
    ch_idx = np.arange(N_CHANNELS)
    rng = np.random.default_rng(20260413)

    # Create zarr v3 group + arrays with explicit sharded layout.
    store = zarr.storage.LocalStore(str(out_path))
    root = zarr.create_group(store=store, zarr_format=3)
    root.attrs.update({
        "device_id":        "mock-48ch-001",
        "number_of_channels": N_CHANNELS,
        "sample_rate_hz":   SAMPLE_RATE_HZ,
        "current_units":    "pA",
        "current_range":    CURRENT_RANGE_NA,
        "current_scale":    CURRENT_STEP_PA,
        "current_offset":   0,
        "voltage_scale":    VOLTAGE_STEP_MV,
        "voltage_offset":   0,
        "duration_sec":     DURATION_SEC,
    })

    # ----- current_data: blosc zstd clevel 3 + bitshuffle -----
    # In zarr-python 3.x, `chunks` = inner (logical) chunk and `shards` = outer
    # shard shape. Setting both triggers the ShardingCodec implicitly, with the
    # given compressor(s) applied to each inner chunk.
    current_arr = root.create_array(
        name="current_data",
        shape=(N_CHANNELS, N_SAMPLES),
        dtype="int16",
        chunks=(1, CURRENT_SAMPLES_PER_LOGICAL_CHUNK),   # (1, 50_000)
        shards=(1, CURRENT_SHARD_SAMPLES),               # (1, 1_600_000)
        compressors=BloscCodec(cname="zstd", clevel=3,
                               shuffle=BloscShuffle.bitshuffle, typesize=2),
    )

    # ----- voltage_data: blosc zstd clevel 1 + shuffle -----
    voltage_arr = root.create_array(
        name="voltage_data",
        shape=(N_CHANNELS, N_SAMPLES),
        dtype="int16",
        chunks=(1, VOLTAGE_SAMPLES_PER_LOGICAL_CHUNK),   # (1, 300_000)
        shards=(1, VOLTAGE_SHARD_SAMPLES),               # (1, 9_600_000)
        compressors=BloscCodec(cname="zstd", clevel=1,
                               shuffle=BloscShuffle.shuffle, typesize=2),
    )

    # Voltage is constant per channel -- fill the whole array once.
    voltage_arr[:] = np.broadcast_to(voltage_int16[:, None],
                                     (N_CHANNELS, N_SAMPLES))

    # ----- Generate current in time chunks -----
    print(f"writing {N_CHUNKS} x {CHUNK_SECONDS}s chunks "
          f"({N_CHANNELS} ch, {SAMPLE_RATE_HZ:.0f} Hz, {DURATION_SEC}s) -> {out_path}",
          flush=True)
    t0 = time.monotonic()
    for c in range(N_CHUNKS):
        start = c * SAMPLES_PER_CHUNK
        end   = start + SAMPLES_PER_CHUNK

        # ---- Vectorised CTMC sampling across all 48 channels ----
        # Time is still sequential (state_{k+1} depends on state_k), but the
        # 48 channels advance in lockstep using numpy ops.
        g = np.empty((N_CHANNELS, SAMPLES_PER_CHUNK), dtype=np.float64)
        u_all = rng.random((SAMPLES_PER_CHUNK, N_CHANNELS))    # transitions
        e_all = rng.standard_normal((SAMPLES_PER_CHUNK, N_CHANNELS))  # emission noise
        for k in range(SAMPLES_PER_CHUNK):
            # transition: next state is the first j where u < c_matrix[ch, state, j]
            cur_cum = cum_mat[ch_idx, state_idx]                # (48, n_state)
            state_idx = (u_all[k, :, None] < cur_cum).argmax(axis=1)
            # emission: Gaussian with per-(channel, state) mean and sd
            mu = state_g[ch_idx, state_idx]                     # (48,)
            sd = state_sd[ch_idx, state_idx]                    # (48,)
            g[:, k] = mu + sd * e_all[k]

        # I_nA = (g + leak) * V_applied  + membrane noise
        i_na = (g + leaks_ns[:, None]) * applied_voltage_v[:, None]
        i_na += rng.normal(0.0, MEMBRANE_NOISE_NA,
                           size=(N_CHANNELS, SAMPLES_PER_CHUNK))
        current_int16 = np.clip(i_na * 1000.0 / CURRENT_STEP_PA,
                                -32768, 32767).astype(np.int16)
        current_arr[:, start:end] = current_int16

        if c == 0 or (c + 1) % 5 == 0 or c == N_CHUNKS - 1:
            elapsed = time.monotonic() - t0
            rate = (c + 1) / elapsed if elapsed else 0.0
            eta  = (N_CHUNKS - (c + 1)) / rate if rate else 0.0
            print(f"  chunk {c+1:>3}/{N_CHUNKS}  "
                  f"({elapsed:6.1f}s elapsed, ~{eta:5.1f}s remaining)",
                  flush=True)

    size_mb = sum(p.stat().st_size for p in out_path.rglob("*") if p.is_file()) / 1e6
    print(f"done in {time.monotonic() - t0:.1f}s -> {out_path} ({size_mb:.0f} MB)")


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else \
          Path(__file__).parent / "mock48_2500hz_1.5h.zarr"
    main(out)
