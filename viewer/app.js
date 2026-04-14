const state = {
  metadata: null,
  channels: [],
  viewStart: 0,
  viewEnd: 0,
  overviewData: null,
  detailData: null,
  pendingOverview: null,
  pendingDetail: null,
  detailRequestId: 0,
  overviewRequestId: 0,
  detailRefreshTimer: null,
  drag: null,
};

const overviewCanvas = document.getElementById("overviewCanvas");
const detailCanvas = document.getElementById("detailCanvas");
const overviewBrush = document.getElementById("overviewBrush");
const detailOverlay = document.getElementById("detailOverlay");
const statusBadge = document.getElementById("statusBadge");
const windowLabel = document.getElementById("windowLabel");
const modeLabel = document.getElementById("modeLabel");
const densityLabel = document.getElementById("densityLabel");
const channelCountLabel = document.getElementById("channelCountLabel");
const deviceId = document.getElementById("deviceId");
const durationLabel = document.getElementById("durationLabel");
const sampleRateLabel = document.getElementById("sampleRateLabel");
const centerInput = document.getElementById("centerInput");
const windowInput = document.getElementById("windowInput");
const jumpBtn = document.getElementById("jumpBtn");
const applyChannelsBtn = document.getElementById("applyChannelsBtn");
const channelGrid = document.getElementById("channelGrid");

const COLORS = [
  "#f5f5f5",
  "#e2e2e2",
  "#cecece",
  "#b8b8b8",
  "#a2a2a2",
  "#8c8c8c",
  "#767676",
  "#606060",
];
const DRAG_REFRESH_INTERVAL_MS = 120;
const DETAIL_BUFFER_MARGIN_RATIO = 1;
const DETAIL_BUFFER_REFRESH_MARGIN_RATIO = 0.3;

function setStatus(kind, label) {
  statusBadge.textContent = label;
  statusBadge.className = `status-badge ${kind}`;
  detailOverlay.classList.toggle("hidden", kind !== "loading");
}

function cancelPendingDetailRequest() {
  if (!state.pendingDetail) {
    return;
  }
  state.pendingDetail.abort();
  state.pendingDetail = null;
}

function cancelScheduledDetailRefresh() {
  if (state.detailRefreshTimer === null) {
    return;
  }
  window.clearTimeout(state.detailRefreshTimer);
  state.detailRefreshTimer = null;
}

function clearDetailPreview() {
  detailCanvas.style.transform = "";
  detailCanvas.style.transformOrigin = "";
  detailCanvas.style.transition = "";
  detailCanvas.parentElement.classList.remove("is-dragging");
}

function setDetailPreviewTransform(offsetPx, scaleX = 1) {
  detailCanvas.style.transformOrigin = "0 0";
  detailCanvas.style.transition = "none";
  detailCanvas.style.transform = `translate3d(${offsetPx}px, 0, 0) scaleX(${scaleX})`;
}

function setDetailPreviewOffset(offsetPx) {
  setDetailPreviewTransform(offsetPx, 1);
  detailCanvas.parentElement.classList.add("is-dragging");
}

function secondsToSamples(seconds) {
  return Math.round(seconds * state.metadata.sample_rate_hz);
}

function samplesToSeconds(samples) {
  return samples / state.metadata.sample_rate_hz;
}

function clampWindow(start, end) {
  const total = state.metadata.total_samples;
  const roundedStart = Math.round(start);
  const roundedEnd = Math.round(end);
  const span = Math.max(1, roundedEnd - roundedStart);
  let nextStart = Math.max(0, Math.min(roundedStart, total - 1));
  let nextEnd = Math.min(total, nextStart + span);
  if (nextEnd - nextStart < span && nextEnd === total) {
    nextStart = Math.max(0, nextEnd - span);
  }
  return { start: Math.round(nextStart), end: Math.round(nextEnd) };
}

function detailDensityThreshold() {
  return state.metadata?.detail_threshold?.samples_per_pixel ?? 2.5;
}

function detailModeForSpan(sampleCount, widthPx = detailCanvas.width) {
  return sampleCount / Math.max(1, widthPx) <= detailDensityThreshold() ? "raw" : "envelope";
}

function detailChannelsMatch() {
  if (!state.detailData || !Array.isArray(state.detailData.channels)) {
    return false;
  }
  if (state.detailData.channels.length !== state.channels.length) {
    return false;
  }
  return state.detailData.channels.every((channel, index) => channel === state.channels[index]);
}

function hasBufferedDetailWindow(start = state.viewStart, end = state.viewEnd) {
  if (!state.detailData || !detailChannelsMatch()) {
    return false;
  }
  return state.detailData.start <= start && state.detailData.end >= end;
}

function buildDetailRequestWindow(start = state.viewStart, end = state.viewEnd) {
  const totalSamples = state.metadata.total_samples;
  const visibleSpan = Math.max(1, end - start);
  const margin = Math.max(1, Math.round(visibleSpan * DETAIL_BUFFER_MARGIN_RATIO));
  const desiredSpan = Math.min(totalSamples, visibleSpan + margin * 2);
  let requestStart = Math.max(0, start - margin);
  let requestEnd = Math.min(totalSamples, end + margin);

  if (requestEnd - requestStart < desiredSpan) {
    if (requestStart === 0) {
      requestEnd = Math.min(totalSamples, requestStart + desiredSpan);
    } else if (requestEnd === totalSamples) {
      requestStart = Math.max(0, requestEnd - desiredSpan);
    }
  }

  const requestSpan = Math.max(1, requestEnd - requestStart);
  const widthPx = Math.max(
    detailCanvas.width,
    Math.round(detailCanvas.width * (requestSpan / visibleSpan)),
  );

  return {
    start: requestStart,
    end: requestEnd,
    widthPx,
  };
}

function detailBufferNeedsRefresh(start = state.viewStart, end = state.viewEnd) {
  if (!hasBufferedDetailWindow(start, end)) {
    return true;
  }

  const visibleSpan = Math.max(1, end - start);
  const desiredMode = detailModeForSpan(visibleSpan, detailCanvas.width);
  if (state.detailData.mode !== desiredMode) {
    return true;
  }

  const refreshMargin = Math.max(1, Math.round(visibleSpan * DETAIL_BUFFER_REFRESH_MARGIN_RATIO));
  return (start - state.detailData.start) < refreshMargin || (state.detailData.end - end) < refreshMargin;
}

function selectedChannelsFromUI() {
  const checked = [...channelGrid.querySelectorAll("input:checked")];
  return checked.map((input) => Number(input.value)).sort((a, b) => a - b);
}

function syncInputs() {
  const centerSeconds = samplesToSeconds((state.viewStart + state.viewEnd) / 2);
  const windowSeconds = samplesToSeconds(state.viewEnd - state.viewStart);
  centerInput.value = centerSeconds.toFixed(2);
  windowInput.value = windowSeconds.toFixed(2);
}

function updateLabels() {
  const sampleCount = state.viewEnd - state.viewStart;
  const windowSeconds = samplesToSeconds(sampleCount);
  windowLabel.textContent = `${windowSeconds.toFixed(2)} s`;
  channelCountLabel.textContent = String(state.channels.length);
  if (state.detailData) {
    modeLabel.textContent = state.detailData.mode;
    densityLabel.textContent = `${state.detailData.samples_per_pixel.toFixed(2)} spp`;
  }
  updateBrush();
}

function updateBrush() {
  if (!state.metadata) {
    return;
  }
  const total = state.metadata.total_samples;
  const left = (state.viewStart / total) * 100;
  const width = ((state.viewEnd - state.viewStart) / total) * 100;
  overviewBrush.style.left = `${left}%`;
  overviewBrush.style.width = `${Math.max(width, 0.25)}%`;
}

async function fetchJson(path, controller) {
  const response = await fetch(path, { signal: controller.signal });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `Request failed with ${response.status}`);
  }
  return response.json();
}

function queryString(params) {
  return new URLSearchParams(params).toString();
}

async function loadMetadata() {
  const metadata = await fetchJson("/api/metadata", new AbortController());
  state.metadata = metadata;
  state.channels = metadata.default_channels;
  state.viewStart = metadata.default_window.start;
  state.viewEnd = metadata.default_window.end;
  deviceId.textContent = metadata.device_id;
  durationLabel.textContent = `${metadata.duration_sec}s`;
  sampleRateLabel.textContent = `${metadata.sample_rate_hz} Hz`;
  buildChannelGrid(metadata.channels, state.channels);
  syncInputs();
}

function buildChannelGrid(channelCount, selectedChannels) {
  channelGrid.innerHTML = "";
  for (let channel = 0; channel < channelCount; channel += 1) {
    const label = document.createElement("label");
    label.className = "channel-option";
    label.innerHTML = `
      <input type="checkbox" value="${channel}" ${selectedChannels.includes(channel) ? "checked" : ""}>
      <span class="channel-label">Ch ${channel}</span>
    `;
    channelGrid.appendChild(label);
  }
}

async function refreshOverview() {
  if (state.pendingOverview) {
    state.pendingOverview.abort();
  }
  const requestId = ++state.overviewRequestId;
  const controller = new AbortController();
  state.pendingOverview = controller;
  const params = queryString({
    start: "0",
    end: String(state.metadata.total_samples),
    width_px: String(overviewCanvas.width),
    channels: state.channels.join(","),
  });
  const data = await fetchJson(`/api/overview?${params}`, controller);
  if (requestId !== state.overviewRequestId) {
    return;
  }
  state.overviewData = data;
  renderOverview();
}

async function refreshDetail() {
  cancelPendingDetailRequest();
  cancelScheduledDetailRefresh();
  const requestId = ++state.detailRequestId;
  const controller = new AbortController();
  state.pendingDetail = controller;
  state.lastDetailRequestAt = performance.now();
  setStatus("loading", "Loading");
  const requestWindow = buildDetailRequestWindow();
  const params = queryString({
    start: String(Math.round(requestWindow.start)),
    end: String(Math.round(requestWindow.end)),
    width_px: String(requestWindow.widthPx),
    channels: state.channels.join(","),
  });
  try {
    const data = await fetchJson(`/api/detail?${params}`, controller);
    if (requestId !== state.detailRequestId) {
      return;
    }
    state.pendingDetail = null;
    state.detailData = data;
    renderDetail();
    if (state.drag) {
      state.drag.baseStart = state.viewStart;
      state.drag.baseEnd = state.viewEnd;
      state.drag.startX = state.drag.latestX;
      detailCanvas.parentElement.classList.add("is-dragging");
    }
    updateLabels();
    setStatus("idle", "Ready");
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }
    state.pendingDetail = null;
    clearDetailPreview();
    setStatus("error", "Error");
    console.error(error);
  }
}

function scheduleDetailRefresh(delayMs = 60) {
  cancelScheduledDetailRefresh();
  state.detailRefreshTimer = window.setTimeout(() => {
    state.detailRefreshTimer = null;
    refreshDetail();
  }, delayMs);
}

function scheduleThrottledDetailRefresh(intervalMs = DRAG_REFRESH_INTERVAL_MS) {
  const now = performance.now();
  const lastRequestAt = state.lastDetailRequestAt ?? 0;
  const delayMs = Math.max(0, intervalMs - (now - lastRequestAt));

  if (state.detailRefreshTimer !== null) {
    return;
  }

  state.detailRefreshTimer = window.setTimeout(() => {
    state.detailRefreshTimer = null;
    refreshDetail();
  }, delayMs);
}

function channelColor(index) {
  return COLORS[index % COLORS.length];
}

function clearCanvas(ctx, canvas) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#020202";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
}

function drawGrid(ctx, canvas, rows) {
  ctx.save();
  ctx.strokeStyle = "rgba(255, 255, 255, 0.10)";
  ctx.lineWidth = 1;
  for (let i = 1; i < rows; i += 1) {
    const y = (canvas.height / rows) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(canvas.width, y);
    ctx.stroke();
  }
  for (let i = 1; i < 6; i += 1) {
    const x = (canvas.width / 6) * i;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, canvas.height);
    ctx.stroke();
  }
  ctx.restore();
}

function rowScale(minValue, maxValue) {
  const fallbackMin = state.metadata.current_count_min ?? -32768;
  const fallbackMax = state.metadata.current_count_max ?? 32767;
  const traceMin = Number.isFinite(minValue) ? minValue : fallbackMin;
  const traceMax = Number.isFinite(maxValue) ? maxValue : fallbackMax;
  const dataMin = Math.min(traceMin, traceMax);
  const dataMax = Math.max(traceMin, traceMax);
  const rawSpan = dataMax - dataMin;
  const padding = rawSpan === 0
    ? Math.max(1, Math.round(Math.abs(dataMax || dataMin || 1) * 0.05))
    : Math.max(1, Math.round(rawSpan * 0.08));
  const minCount = dataMin - padding;
  const maxCount = dataMax + padding;
  const span = Math.max(1, maxCount - minCount);

  return {
    dataMin,
    dataMax,
    minCount,
    maxCount,
    span,
    centerCount: (maxCount + minCount) / 2,
  };
}

function drawTraceLabel(ctx, rowTop, trace, scale, unitScale) {
  ctx.fillStyle = "rgba(255, 255, 255, 0.88)";
  ctx.font = '12px "IBM Plex Mono", monospace';
  ctx.fillText(
    `Ch ${trace.channel}  ${(scale.dataMin * unitScale).toFixed(1)} to ${(scale.dataMax * unitScale).toFixed(1)} ${state.metadata.current_units}`,
    14,
    rowTop + 18,
  );
}

function drawOverviewTraceRow(ctx, rowIndex, rowCount, trace, mode, color, canvas, unitScale) {
  const rowHeight = canvas.height / rowCount;
  const rowTop = rowHeight * rowIndex;
  const paddingY = 18;
  const availableHeight = rowHeight - paddingY * 2;
  const scale = rowScale(trace.min_count, trace.max_count);
  const centerY = rowTop + rowHeight / 2;

  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2;

  if (mode === "raw") {
    ctx.beginPath();
    const values = trace.values;
    for (let index = 0; index < values.length; index += 1) {
      const x = values.length <= 1 ? 0 : (index / (values.length - 1)) * canvas.width;
      const normalized = (values[index] - scale.centerCount) / scale.span;
      const y = centerY - normalized * availableHeight;
      if (index === 0) {
        ctx.moveTo(x, y);
      } else {
        ctx.lineTo(x, y);
      }
    }
    ctx.stroke();
  } else {
    const count = trace.mins.length;
    ctx.beginPath();
    for (let index = 0; index < count; index += 1) {
      const x = count <= 1 ? 0 : (index / (count - 1)) * canvas.width;
      const minNorm = (trace.mins[index] - scale.centerCount) / scale.span;
      const maxNorm = (trace.maxs[index] - scale.centerCount) / scale.span;
      const yMin = centerY - minNorm * availableHeight;
      const yMax = centerY - maxNorm * availableHeight;
      ctx.moveTo(x, yMin);
      ctx.lineTo(x, yMax);
    }
    ctx.stroke();
  }

  drawTraceLabel(ctx, rowTop, trace, scale, unitScale);
  ctx.restore();
}

function rawSliceBounds(trace, visibleStart, visibleEnd) {
  const startIndex = Math.max(0, visibleStart - state.detailData.start);
  const endIndex = Math.min(trace.values.length, visibleEnd - state.detailData.start);
  return {
    startIndex,
    endIndex: Math.max(startIndex + 1, endIndex),
  };
}

function envelopeSliceBounds(trace, visibleStart, visibleEnd) {
  const totalSpan = Math.max(1, state.detailData.end - state.detailData.start);
  const bucketCount = trace.mins.length;
  const startRatio = (visibleStart - state.detailData.start) / totalSpan;
  const endRatio = (visibleEnd - state.detailData.start) / totalSpan;
  const startIndex = Math.max(0, Math.floor(startRatio * bucketCount));
  const endIndex = Math.min(bucketCount, Math.ceil(endRatio * bucketCount));
  return {
    startIndex,
    endIndex: Math.max(startIndex + 1, endIndex),
  };
}

function traceRangeFromRaw(trace, bounds) {
  let minCount = Number.POSITIVE_INFINITY;
  let maxCount = Number.NEGATIVE_INFINITY;
  for (let index = bounds.startIndex; index < bounds.endIndex; index += 1) {
    const value = trace.values[index];
    if (value < minCount) {
      minCount = value;
    }
    if (value > maxCount) {
      maxCount = value;
    }
  }
  if (!Number.isFinite(minCount) || !Number.isFinite(maxCount)) {
    return { minCount: trace.min_count, maxCount: trace.max_count };
  }
  return { minCount, maxCount };
}

function traceRangeFromEnvelope(trace, bounds) {
  let minCount = Number.POSITIVE_INFINITY;
  let maxCount = Number.NEGATIVE_INFINITY;
  for (let index = bounds.startIndex; index < bounds.endIndex; index += 1) {
    const minValue = trace.mins[index];
    const maxValue = trace.maxs[index];
    if (minValue < minCount) {
      minCount = minValue;
    }
    if (maxValue > maxCount) {
      maxCount = maxValue;
    }
  }
  if (!Number.isFinite(minCount) || !Number.isFinite(maxCount)) {
    return { minCount: trace.min_count, maxCount: trace.max_count };
  }
  return { minCount, maxCount };
}

function canRenderVisibleDetailFromBuffer(start = state.viewStart, end = state.viewEnd) {
  if (!hasBufferedDetailWindow(start, end)) {
    return false;
  }
  return state.detailData.mode === detailModeForSpan(end - start, detailCanvas.width);
}

function drawDetailTraceRow(ctx, rowIndex, rowCount, trace, color, canvas, unitScale) {
  const rowHeight = canvas.height / rowCount;
  const rowTop = rowHeight * rowIndex;
  const paddingY = 18;
  const availableHeight = rowHeight - paddingY * 2;
  const centerY = rowTop + rowHeight / 2;
  const visibleStart = state.viewStart;
  const visibleEnd = state.viewEnd;
  const mode = state.detailData.mode;
  const bounds = mode === "raw"
    ? rawSliceBounds(trace, visibleStart, visibleEnd)
    : envelopeSliceBounds(trace, visibleStart, visibleEnd);
  const range = mode === "raw"
    ? traceRangeFromRaw(trace, bounds)
    : traceRangeFromEnvelope(trace, bounds);
  const scale = rowScale(range.minCount, range.maxCount);

  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2;

  if (mode === "raw") {
    const visibleCount = Math.max(1, bounds.endIndex - bounds.startIndex);
    ctx.beginPath();
    for (let index = bounds.startIndex; index < bounds.endIndex; index += 1) {
      const relativeIndex = index - bounds.startIndex;
      const x = visibleCount <= 1 ? 0 : (relativeIndex / (visibleCount - 1)) * canvas.width;
      const normalized = (trace.values[index] - scale.centerCount) / scale.span;
      const y = centerY - normalized * availableHeight;
      if (relativeIndex === 0) {
        ctx.moveTo(x, y);
      } else {
        ctx.lineTo(x, y);
      }
    }
    ctx.stroke();
  } else {
    const visibleCount = Math.max(1, bounds.endIndex - bounds.startIndex);
    ctx.beginPath();
    for (let index = bounds.startIndex; index < bounds.endIndex; index += 1) {
      const relativeIndex = index - bounds.startIndex;
      const x = visibleCount <= 1 ? 0 : (relativeIndex / (visibleCount - 1)) * canvas.width;
      const minNorm = (trace.mins[index] - scale.centerCount) / scale.span;
      const maxNorm = (trace.maxs[index] - scale.centerCount) / scale.span;
      const yMin = centerY - minNorm * availableHeight;
      const yMax = centerY - maxNorm * availableHeight;
      ctx.moveTo(x, yMin);
      ctx.lineTo(x, yMax);
    }
    ctx.stroke();
  }

  drawTraceLabel(ctx, rowTop, trace, scale, unitScale);
  ctx.restore();
}

function renderDetailFromBufferIfAvailable() {
  if (!canRenderVisibleDetailFromBuffer()) {
    return false;
  }
  renderDetail();
  return true;
}

function renderOverview() {
  if (!state.overviewData) {
    return;
  }
  const ctx = overviewCanvas.getContext("2d");
  clearCanvas(ctx, overviewCanvas);
  drawGrid(ctx, overviewCanvas, state.overviewData.traces.length);
  state.overviewData.traces.forEach((trace, index) => {
    drawOverviewTraceRow(
      ctx,
      index,
      state.overviewData.traces.length,
      trace,
      "envelope",
      channelColor(index),
      overviewCanvas,
      state.metadata.current_scale,
    );
  });
  updateBrush();
}

function renderDetail() {
  if (!canRenderVisibleDetailFromBuffer()) {
    return;
  }
  clearDetailPreview();
  const ctx = detailCanvas.getContext("2d");
  clearCanvas(ctx, detailCanvas);
  drawGrid(ctx, detailCanvas, state.detailData.traces.length);
  state.detailData.traces.forEach((trace, index) => {
    drawDetailTraceRow(
      ctx,
      index,
      state.detailData.traces.length,
      trace,
      channelColor(index),
      detailCanvas,
      state.metadata.current_scale,
    );
  });
}

function beginDrag(event) {
  cancelScheduledDetailRefresh();
  state.drag = {
    pointerId: event.pointerId,
    startX: event.clientX,
    latestX: event.clientX,
    baseStart: state.viewStart,
    baseEnd: state.viewEnd,
    widthPx: detailCanvas.getBoundingClientRect().width,
    frameId: null,
  };
  if (state.detailRefreshTimer !== null) {
    window.clearTimeout(state.detailRefreshTimer);
    state.detailRefreshTimer = null;
  }
  detailCanvas.setPointerCapture(event.pointerId);
  detailCanvas.parentElement.classList.add("is-dragging");
}

function applyDragPreview() {
  if (!state.drag) {
    return;
  }
  state.drag.frameId = null;
  const deltaPx = state.drag.latestX - state.drag.startX;
  const span = state.drag.baseEnd - state.drag.baseStart;
  const shiftSamples = Math.round((deltaPx / state.drag.widthPx) * span);
  const next = clampWindow(
    state.drag.baseStart - shiftSamples,
    state.drag.baseEnd - shiftSamples,
  );
  state.viewStart = next.start;
  state.viewEnd = next.end;
  renderDetailFromBufferIfAvailable();
  detailCanvas.parentElement.classList.add("is-dragging");
  updateLabels();
  if (detailBufferNeedsRefresh(next.start, next.end)) {
    cancelPendingDetailRequest();
    scheduleThrottledDetailRefresh();
  }
}

function updateDrag(event) {
  if (!state.drag) {
    return;
  }
  state.drag.latestX = event.clientX;
  if (state.drag.frameId !== null) {
    return;
  }
  state.drag.frameId = window.requestAnimationFrame(applyDragPreview);
}

function endDrag() {
  if (!state.drag) {
    return;
  }
  if (state.drag.frameId !== null) {
    window.cancelAnimationFrame(state.drag.frameId);
    applyDragPreview();
  }
  state.drag = null;
  syncInputs();
  scheduleDetailRefresh(0);
}

function setWindow(centerSamples, spanSamples) {
  const roundedCenter = Math.round(centerSamples);
  const half = Math.max(1, Math.round(spanSamples / 2));
  const next = clampWindow(roundedCenter - half, roundedCenter + half);
  state.viewStart = next.start;
  state.viewEnd = next.end;
  syncInputs();
  updateLabels();
  renderDetailFromBufferIfAvailable();
  if (detailBufferNeedsRefresh()) {
    scheduleDetailRefresh();
  }
}

function pan(factor) {
  const span = state.viewEnd - state.viewStart;
  const shift = Math.max(1, Math.round(span * factor));
  setWindow(Math.round((state.viewStart + state.viewEnd) / 2) + shift, span);
}

function zoom(factor, anchorRatio = 0.5) {
  cancelPendingDetailRequest();
  const oldStart = state.viewStart;
  const oldEnd = state.viewEnd;
  const oldSpan = oldEnd - oldStart;
  const span = state.viewEnd - state.viewStart;
  const nextSpan = Math.max(secondsToSamples(0.2), Math.round(span * factor));
  const anchorSample = state.viewStart + span * anchorRatio;
  const start = anchorSample - nextSpan * anchorRatio;
  const next = clampWindow(Math.round(start), Math.round(start + nextSpan));
  state.viewStart = next.start;
  state.viewEnd = next.end;
  const newSpan = Math.max(1, next.end - next.start);
  const renderedFromBuffer = renderDetailFromBufferIfAvailable();
  if (!renderedFromBuffer && newSpan < oldSpan) {
    const displayWidthPx = detailCanvas.getBoundingClientRect().width;
    const scaleX = oldSpan / newSpan;
    const offsetPx = ((oldStart - next.start) / newSpan) * displayWidthPx;
    setDetailPreviewTransform(offsetPx, scaleX);
  } else if (!renderedFromBuffer) {
    clearDetailPreview();
  }
  syncInputs();
  updateLabels();
  if (detailBufferNeedsRefresh()) {
    scheduleDetailRefresh(90);
  }
}

function attachControls() {
  document.getElementById("panLeftBtn").addEventListener("click", () => pan(-0.35));
  document.getElementById("panRightBtn").addEventListener("click", () => pan(0.35));
  document.getElementById("zoomInBtn").addEventListener("click", () => zoom(0.6));
  document.getElementById("zoomOutBtn").addEventListener("click", () => zoom(1.6));

  jumpBtn.addEventListener("click", () => {
    const centerSeconds = Number(centerInput.value);
    const windowSeconds = Number(windowInput.value);
    if (!Number.isFinite(centerSeconds) || !Number.isFinite(windowSeconds)) {
      return;
    }
    setWindow(secondsToSamples(centerSeconds), secondsToSamples(windowSeconds));
  });

  applyChannelsBtn.addEventListener("click", async () => {
    const nextChannels = selectedChannelsFromUI();
    if (!nextChannels.length) {
      return;
    }
    state.channels = nextChannels;
    channelCountLabel.textContent = String(state.channels.length);
    await refreshOverview();
    await refreshDetail();
  });

  overviewCanvas.addEventListener("click", (event) => {
    const rect = overviewCanvas.getBoundingClientRect();
    const ratio = (event.clientX - rect.left) / rect.width;
    const centerSamples = Math.round(ratio * state.metadata.total_samples);
    setWindow(centerSamples, state.viewEnd - state.viewStart);
  });

  detailCanvas.addEventListener("wheel", (event) => {
    event.preventDefault();
    const rect = detailCanvas.getBoundingClientRect();
    const anchorRatio = (event.clientX - rect.left) / rect.width;
    zoom(event.deltaY < 0 ? 0.8 : 1.25, anchorRatio);
  });

  detailCanvas.addEventListener("pointerdown", (event) => {
    beginDrag(event);
  });
  detailCanvas.addEventListener("pointermove", (event) => {
    updateDrag(event);
  });
  detailCanvas.addEventListener("pointerup", () => {
    endDrag();
  });
  detailCanvas.addEventListener("pointercancel", () => {
    endDrag();
  });
}

async function boot() {
  try {
    setStatus("loading", "Loading");
    await loadMetadata();
    attachControls();
    await refreshOverview();
    await refreshDetail();
    updateLabels();
    setStatus("idle", "Ready");
  } catch (error) {
    setStatus("error", "Error");
    console.error(error);
  }
}

boot();
