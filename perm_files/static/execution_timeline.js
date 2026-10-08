const dagId = document.body.dataset.dagId;
const selectedRun = document.body.dataset.runTs;
document.getElementById('run-selector')?.addEventListener('change', event => {
  const run = event.target.value;
  location.href = `/timeline?dag=${encodeURIComponent(dagId)}${run ? `&run=${encodeURIComponent(run)}` : ''}`;
});
const scrollPanel = document.getElementById('timeline-scroll');
const detailPanel = document.getElementById('attempt-detail');
const groupWorker = document.getElementById('group-worker');
const autoRefresh = document.getElementById('auto-refresh');
const jobFilter = document.getElementById('job-filter');
const spanLimit = document.getElementById('span-limit');
const statusFilter = document.getElementById('status-filter');
const workerFilter = document.getElementById('worker-filter');
const timeWindow = document.getElementById('time-window');
const retriesOnly = document.getElementById('retries-only');
const zoomRange = document.getElementById('zoom-range');
const errorList = document.getElementById('error-list');
const moreErrors = document.getElementById('more-errors');
let snapshot = null;
let selectedKey = null;
let rows = [];
let rowsLayer = null;
let geometry = null;
let visibleRange = [-1, -1];
let pollTimer = null;
const ROW_HEIGHT = 46;
const OVERSCAN = 12;

const ms = value => value < 1000 ? `${value} ms` : `${(value / 1000).toFixed(2)} s`;
const spanSeconds = value => `${(Math.max(0, value) / 1000).toFixed(2)} s`;
const clock = value => new Date(value).toLocaleTimeString([], {
  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, fractionalSecondDigits: 3,
});
const node = (tag, className, value) => {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (value !== undefined) el.textContent = value;
  return el;
};
const keyOf = span => `${span.job_id}:${span.attempt}:${span.started_at}`;
const workerTone = worker => [...worker].reduce((hash, char) => (hash * 31 + char.charCodeAt(0)) >>> 0, 0) % 6;

document.getElementById('pipeline-search')?.addEventListener('input', event => {
  const query = event.target.value.toLowerCase();
  document.querySelectorAll('.dag-link').forEach(link => {
    link.hidden = !link.textContent.toLowerCase().includes(query);
  });
});
const runTime = document.getElementById('run-time');
if (runTime && Number(runTime.dataset.timestamp)) runTime.textContent = new Date(Number(runTime.dataset.timestamp)).toLocaleString();

function showDetails(span) {
  detailPanel.replaceChildren();
  if (snapshot) {
    const factor = node('p', 'run-insight');
    const strong = node('b', '', `${snapshot.metrics.parallelism_factor.toFixed(2)}×`);
    factor.append('Achieved parallelism ', strong,
      ` · ${snapshot.metrics.peak_parallel} peak concurrent · ${snapshot.metrics.jobs_per_worker.toFixed(2)} average active jobs per worker`);
    factor.title = 'Parallelism = total execution time across attempts ÷ elapsed run time.';
    detailPanel.append(factor);
  }
  if (!span) {
    detailPanel.append(node('p', 'empty', 'Select an attempt to inspect its timing, worker, and outcome.'));
    return;
  }
  detailPanel.append(node('h2', '', `${span.job_id} · attempt #${span.attempt}`));
  const list = node('dl');
  const fields = [
    ['Status', span.status], ['Worker', span.worker],
    ['Queued', clock(span.queued_at)], ['Started', clock(span.started_at)],
    ['Finished', span.ended_at ? clock(span.ended_at) : 'Running'],
    ['Queue wait', ms(span.started_at - span.queued_at)],
    ['Execution', ms((span.ended_at || Date.now()) - span.started_at)],
    ['Parents', span.parents.join(', ') || 'None'],
    ['Critical path', snapshot.critical_path.includes(span.job_id) ? 'Yes' : 'No'],
  ];
  for (const [label, value] of fields) {
    const field = node('div'); field.append(node('dt', '', label), node('dd', '', value)); list.append(field);
  }
  if (span.reason) {
    const field = node('div'); field.append(node('dt', '', 'Result / error'), node('dd', 'error', span.reason)); list.append(field);
  }
  const link = node('a', '', 'Open job in dependency graph ↗');
  link.href = `/dags/${encodeURIComponent(dagId)}?job=${encodeURIComponent(span.job_id)}&run=${encodeURIComponent(selectedRun)}`;
  detailPanel.append(list, link);
}

function renderMetrics(data) {
  const target = document.getElementById('timeline-metrics');
  target.replaceChildren();
  const values = [
    ['Wall clock', ms(data.metrics.wall_ms)], ['Critical path', ms(data.metrics.critical_ms)],
    ['Parallelism factor', `${data.metrics.parallelism_factor.toFixed(2)}×`],
    ['Avg jobs / worker', `${data.metrics.jobs_per_worker.toFixed(2)}×`],
    ['Queue time', ms(data.metrics.queue_ms)],
    ['Attempts', data.metrics.attempts], ['Jobs', data.metrics.jobs],
    ['Workers', data.metrics.workers], ['Peak concurrent', data.metrics.peak_parallel],
    ['Retries', data.metrics.retry_attempts], ['Failed attempts', data.metrics.failed_attempts],
  ];
  for (const [label, value] of values) {
    const card = node('div', 'timeline-metric');
    card.append(node('b', '', String(value)), node('small', '', label)); target.append(card);
  }
}

function makeRow(span, index) {
    const row = node('div', 'timeline-row');
    row.style.top = `${index * ROW_HEIGHT}px`;
    const label = node('div', 'timeline-label');
    const meta = node('div', 'timeline-row-meta');
    meta.append(node('span', 'attempt-chip', `#${span.attempt}`),
      node('span', `worker-chip worker-tone-${workerTone(span.worker)}`, span.worker));
    label.append(node('strong', '', span.job_id), meta);
    const track = node('div', 'timeline-track');
    const start = (span.started_at - geometry.origin) / geometry.width * 100;
    const waitStart = (span.queued_at - geometry.origin) / geometry.width * 100;
    const stop = ((span.ended_at || Date.now()) - geometry.origin) / geometry.width * 100;
    if (start > waitStart) {
      const wait = node('div', 'timeline-wait');
      wait.style.left = `${waitStart}%`; wait.style.width = `${start - waitStart}%`;
      wait.title = `Queue wait: ${ms(span.started_at - span.queued_at)}`; track.append(wait);
    }
    const bar = node('button', 'timeline-bar' + (geometry.critical.has(span.job_id) ? ' critical' : '') +
      (selectedKey === keyOf(span) ? ' selected' : ''));
    bar.type = 'button'; bar.dataset.status = span.status;
    const duration = spanSeconds((span.ended_at || Date.now()) - span.started_at);
    const width = Math.max(0.3, stop - start);
    const trackPixels = Math.max(470, Math.max(700, 1100 * Number(zoomRange.value) / 100) - 230);
    const narrow = trackPixels * width / 100 < 57;
    bar.style.left = `${start}%`; bar.style.width = `${width}%`;
    if (!narrow) bar.append(node('span', 'timeline-duration', duration));
    bar.title = `${span.job_id} · ${span.status} · ${duration}`;
    bar.setAttribute('aria-label', bar.title);
    bar.addEventListener('click', () => { selectedKey = keyOf(span); renderWindow(true); showDetails(span); });
    track.append(bar);
    if (narrow) {
      const outside = node('span', 'timeline-duration-outside', duration);
      outside.style.left = `${Math.min(89, stop + 0.6)}%`;
      track.append(outside);
    }
    row.append(label, track);
    return row;
}

function renderWindow(force = false) {
  if (!rowsLayer) return;
  const first = Math.max(0, Math.floor(Math.max(0, scrollPanel.scrollTop - 32) / ROW_HEIGHT) - OVERSCAN);
  const last = Math.min(rows.length, Math.ceil((scrollPanel.scrollTop + scrollPanel.clientHeight) / ROW_HEIGHT) + OVERSCAN);
  if (!force && first === visibleRange[0] && last === visibleRange[1]) return;
  visibleRange = [first, last];
  const fragment = document.createDocumentFragment();
  for (let index = first; index < last; index++) fragment.append(makeRow(rows[index], index));
  rowsLayer.replaceChildren(fragment);
}

function renderErrors(data) {
  const errors = data.failures || [];
  document.getElementById('error-count').textContent = data.failure_total;
  errorList.replaceChildren();
  if (!errors.length) {
    errorList.append(node('p', 'empty', 'No failed dispatch attempts in this run.'));
  }
  const fragment = document.createDocumentFragment();
  for (const error of errors) {
    const item = node('div', 'error-item');
    const label = node('div');
    const link = node('a', '', `${error.job_id} · attempt #${error.attempt}`);
    link.href = `/dags/${encodeURIComponent(dagId)}?job=${encodeURIComponent(error.job_id)}&run=${encodeURIComponent(selectedRun)}`;
    label.append(link, node('small', '', `${error.worker} · ${error.ended_at ? clock(error.ended_at) : 'Time unavailable'}${error.exit_code === null ? '' : ` · exit ${error.exit_code}`}`));
    item.append(label, node('p', '', error.message)); fragment.append(item);
  }
  errorList.append(fragment);
  moreErrors.hidden = errors.length >= data.failure_total;
  if (!moreErrors.hidden) moreErrors.textContent = `Show more errors (${data.failure_total - errors.length} remaining)`;
}

function render(data) {
  if (snapshot && snapshot.submitted === data.submitted && snapshot.failures?.length > data.failures.length) {
    const known = new Set(data.failures.map(error => `${error.job_id}:${error.attempt}:${error.ended_at}`));
    data.failures.push(...snapshot.failures.filter(error => !known.has(`${error.job_id}:${error.attempt}:${error.ended_at}`)));
  }
  snapshot = data;
  renderMetrics(data);
  renderErrors(data);
  const previousX = scrollPanel.scrollLeft, previousY = scrollPanel.scrollTop;
  scrollPanel.replaceChildren();
  const query = jobFilter.value.trim().toLowerCase();
  const status = statusFilter.value;
  const worker = workerFilter.value;
  const windowMs = Number(timeWindow.value);
  const observedEnd = data.spans.reduce((value, span) => Math.max(value, span.ended_at || Date.now()), 0);
  rows = data.spans.filter(span =>
    (!query || span.job_id.toLowerCase().includes(query)) &&
    (!status || span.status === status || status === 'FAILED' && span.status === 'DEAD') &&
    (!worker || span.worker === worker) &&
    (!retriesOnly.checked || span.attempt > 1) &&
    (!windowMs || span.started_at >= observedEnd - windowMs));
  rows.sort(groupWorker.checked
    ? (a, b) => a.worker.localeCompare(b.worker) || a.started_at - b.started_at
    : (a, b) => a.started_at - b.started_at || a.job_id.localeCompare(b.job_id));
  document.getElementById('row-count').textContent = `${rows.length.toLocaleString()} shown · ${data.span_total.toLocaleString()} total`;
  if (!rows.length) {
    scrollPanel.append(node('p', 'empty timeline-empty', data.span_total ? 'No attempts match these controls.' :
      'No dispatch attempts recorded for this run yet. Waiting jobs have no execution bar.'));
    rowsLayer = null;
    showDetails(null);
    return;
  }
  let origin = Infinity, axisEnd = 0;
  for (const span of data.spans) {
    origin = Math.min(origin, span.queued_at);
    axisEnd = Math.max(axisEnd, span.ended_at || Date.now());
  }
  geometry = {origin, width: Math.max(1, axisEnd - origin), critical: new Set(data.critical_path)};
  const canvas = node('div', 'timeline-canvas');
  canvas.style.width = `${Math.max(700, 1100 * Number(zoomRange.value) / 100)}px`;
  canvas.style.height = `${32 + rows.length * ROW_HEIGHT}px`;
  const ruler = node('div', 'timeline-ruler');
  ruler.append(node('span', '', 'JOB / ATTEMPT'));
  const rulerTrack = node('div', 'timeline-ruler-track');
  for (let i = 0; i <= 5; i++) rulerTrack.append(node('span', '', ms(Math.round(geometry.width * i / 5))));
  ruler.append(rulerTrack);
  rowsLayer = node('div', 'timeline-rows');
  canvas.append(ruler, rowsLayer);
  scrollPanel.append(canvas);
  scrollPanel.scrollLeft = previousX; scrollPanel.scrollTop = previousY;
  visibleRange = [-1, -1];
  renderWindow(true);
  showDetails(data.spans.find(span => keyOf(span) === selectedKey));
}

scrollPanel?.addEventListener('scroll', () => renderWindow());
groupWorker?.addEventListener('change', () => { if (snapshot) render(snapshot); });
jobFilter?.addEventListener('input', () => { if (snapshot) { scrollPanel.scrollTop = 0; render(snapshot); } });
for (const control of [statusFilter, workerFilter, timeWindow, retriesOnly]) {
  control?.addEventListener('change', () => { if (snapshot) { scrollPanel.scrollTop = 0; render(snapshot); } });
}
zoomRange?.addEventListener('input', () => {
  document.getElementById('zoom-label').textContent = `${zoomRange.value}%`;
  if (snapshot) render(snapshot);
});
spanLimit?.addEventListener('change', () => poll());
moreErrors?.addEventListener('click', async () => {
  if (!snapshot) return;
  moreErrors.disabled = true;
  try {
    const offset = snapshot.failures.length;
    const url = `/api/dag/timeline/${encodeURIComponent(dagId)}?run=${encodeURIComponent(selectedRun)}&limit=0&error_offset=${offset}`;
    const response = await fetch(url, {cache: 'no-store'});
    if (!response.ok) throw new Error('Could not load more errors');
    snapshot.failures.push(...(await response.json()).failures);
    renderErrors(snapshot);
  } catch (error) {
    moreErrors.textContent = error.message;
  } finally {
    moreErrors.disabled = false;
  }
});
async function poll() {
  if (!dagId) return;
  if (pollTimer) clearTimeout(pollTimer);
  try {
    const response = await fetch(`/api/dag/timeline/${encodeURIComponent(dagId)}?run=${encodeURIComponent(selectedRun)}&limit=${encodeURIComponent(spanLimit.value)}`, {cache: 'no-store'});
    if (!response.ok) throw new Error('Timeline unavailable');
    const data = await response.json();
    const workers = new Set([...workerFilter.options].map(option => option.value));
    for (const worker of new Set(data.spans.map(span => span.worker))) {
      if (!workers.has(worker)) workerFilter.append(new Option(worker, worker));
    }
    render(data);
  } catch (error) {
    if (!snapshot) scrollPanel.textContent = error.message;
  } finally {
    const delay = snapshot?.spans.some(span => span.status === 'RUNNING') ? 3000 : 15000;
    schedulePoll(delay);
  }
}
function schedulePoll(delay) {
  pollTimer = setTimeout(() => { if (autoRefresh?.checked) poll(); else schedulePoll(delay); }, delay);
}
poll();
