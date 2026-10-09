// The server owns DAG membership and dependency structure. Poll only state.
const selectedId = document.body.dataset.selectedDag;
const historical = document.body.dataset.historical === 'true';
let selectedStatus = document.body.dataset.selectedStatus;
const terminal = status => ['COMPLETED', 'FAILED', 'DEAD', 'CANCELLED'].includes(status);
const refreshDelay = status => document.hidden ? 60000 : terminal(status) ? 30000 : status === 'UNKNOWN' ? 10000 : 3000;
document.getElementById('run-selector')?.addEventListener('change', event => {
  const run = event.target.value;
  location.href = `/dags/${encodeURIComponent(selectedId)}${run ? `?run=${encodeURIComponent(run)}` : ''}`;
});
const cancelDagButton = document.getElementById('cancel-dag');
const cancelJobButton = document.getElementById('cancel-job');
const canCancel = status => ['WAITING', 'PENDING', 'RUNNING'].includes(status);
const isService = job => ['service', 'deploy_payload'].includes(String(job.spec?.job_type || '').toLowerCase());
function serviceAddress(container, job, includeId = false) {
  container.replaceChildren();
  const endpoint = job.service;
  if (!endpoint || job.status !== 'COMPLETED') { container.hidden = true; return; }
  container.hidden = false;
  if (includeId) {
    const chip = document.createElement('span'); chip.className = 'service-chip'; chip.textContent = 'Service ready';
    const id = document.createElement('strong'); id.textContent = job.id;
    container.append(chip, id);
  } else container.append('Ready at ');
  const address = document.createElement('a');
  const url = String(endpoint.url || '');
  if (/^http:\/\/[A-Za-z0-9.\-\[\]:]+:\d+$/.test(url)) {
    address.href = url; address.target = '_blank'; address.rel = 'noopener noreferrer';
  }
  address.textContent = url;
  container.append(address, document.createTextNode(' · Port ' + endpoint.port));
}
const filePanel = document.getElementById('workspace-files');
const sharedPanel = document.getElementById('shared-files');
const runTime = document.querySelector('.run-time');
if (runTime && Number(runTime.dataset.timestamp)) {
  runTime.textContent = new Date(Number(runTime.dataset.timestamp)).toLocaleString();
}
document.querySelectorAll('.detail-tabs [role="tab"]').forEach(button => {
  button.addEventListener('click', () => {
    document.querySelectorAll('.detail-tabs [role="tab"]').forEach(tab => {
      const active = tab === button;
      tab.setAttribute('aria-selected', String(active));
      document.getElementById(tab.getAttribute('aria-controls')).hidden = !active;
    });
  });
});
async function refreshFileList(panel, url, emptyMessage) {
  if (!panel) return;
  try {
    const response = await fetch(url, { cache: 'no-store' });
    if (!response.ok) throw new Error('Files unavailable');
    const { files } = await response.json();
    panel.replaceChildren();
    if (!files.length) {
      const empty = document.createElement('p');
      empty.className = 'empty';
      empty.textContent = emptyMessage;
      panel.append(empty);
    }
    for (const file of files) {
      const row = document.createElement('div');
      row.className = 'workspace-file';
      const link = document.createElement('a');
      link.href = file.url;
      link.textContent = file.name;
      link.title = 'Download ' + file.name;
      const detail = document.createElement('span');
      detail.textContent = `${file.kind} · ${Math.ceil(file.size / 1024)} KB`;
      row.append(link, detail);
      panel.append(row);
    }
  } catch {
    panel.textContent = 'Workspace files are unavailable.';
  }
}
function refreshFiles() {
  const jobs = filePanel ? refreshFileList(filePanel,
    `/api/dag/files/${encodeURIComponent(filePanel.dataset.dagId)}/${encodeURIComponent(filePanel.dataset.jobId)}`,
    'No log or job-scoped artifact for this run yet.') : Promise.resolve();
  const shared = sharedPanel ? refreshFileList(sharedPanel, '/api/dag/shared_files',
    'No unassigned shared files.') : Promise.resolve();
  return Promise.allSettled([jobs, shared]);
}
if (!historical) refreshFiles();
const state = (node, status) => {
  if (!node) return;
  node.textContent = status;
  node.dataset.state = status;
  if (node.hasAttribute('data-status')) node.dataset.status = status;
};

async function refresh() {
  if (historical) return;
  try {
    const params = new URLSearchParams({ view: 'page', page: document.body.dataset.page || '1',
      q: document.body.dataset.query || '', selected: selectedId || '' });
    const response = await fetch(`/api/dag_status?${params}`, { cache: 'no-store' });
    if (!response.ok) throw new Error('Master unavailable');
    const data = await response.json();
    const connection = document.getElementById('connection');
    connection.textContent = 'Master online';
    connection.classList.add('online');
    connection.classList.remove('offline');
    document.getElementById('dag-count').textContent = data.total;
    for (const dag of data.dags) {
      const item = [...document.querySelectorAll('.dag-link')].find(el => el.dataset.dagId === dag.id);
      if (item) {
        state(item.querySelector('.state[data-status]'), dag.status);
        item.querySelector('.status-dot').dataset.status = dag.status;
        item.querySelector('[data-progress]').textContent = `${dag.completed}/${dag.total} jobs`;
      }
      if (dag.id !== selectedId) continue;
      selectedStatus = dag.status;
      state(document.getElementById('dag-status'), dag.status);
      document.getElementById('status-note').hidden = dag.status !== 'UNKNOWN';
      if (cancelDagButton && !cancelDagButton.dataset.pending) {
        cancelDagButton.disabled = !dag.jobs.some(job => canCancel(job.status));
      }
      const count = name => dag.jobs.filter(job => job.status === name).length;
      document.getElementById('completed-count').textContent = count('COMPLETED');
      document.getElementById('running-count').textContent = count('RUNNING');
      document.getElementById('failed-count').textContent = count('FAILED') + count('DEAD');
      document.getElementById('cancelled-count').textContent = count('CANCELLED');
      document.getElementById('waiting-count').textContent = count('WAITING') + count('PENDING');
      const progress = document.querySelector('.progress');
      const percent = dag.total ? Math.round(dag.completed * 100 / dag.total) : 0;
      progress.setAttribute('aria-valuenow', percent);
      document.getElementById('progress-fill').style.width = `${percent}%`;
      for (const job of dag.jobs) {
        const node = [...document.querySelectorAll('.node')].find(el => el.dataset.jobId === job.id);
        if (!node) continue;
        node.dataset.status = job.status;
        node.querySelector('.node-status').textContent = job.status;
        node.querySelector('.node-type').textContent = isService(job) ? 'SERVICE' :
          job.spec?.hitl_message ? 'HUMAN GATE' : 'TASK';
      }
      const deployments = document.getElementById('service-deployments');
      if (deployments) {
        deployments.replaceChildren();
        for (const service of dag.jobs.filter(job => isService(job) && job.service && job.status === 'COMPLETED')) {
          const row = document.createElement('div'); serviceAddress(row, service, true); deployments.append(row);
        }
        deployments.hidden = !deployments.childElementCount;
      }
      const current = document.body.dataset.selectedJob;
      const selected = dag.jobs.find(job => job.id === current);
      if (selected) {
        state(document.getElementById('selected-status'), selected.status);
        document.getElementById('selected-service-chip').hidden = !isService(selected);
        serviceAddress(document.getElementById('selected-service-endpoint'), selected);
        document.getElementById('selected-worker').textContent = selected.worker || 'Unassigned';
        document.getElementById('selected-attempts').textContent = selected.attempts || '—';
        document.getElementById('selected-retries').textContent = selected.retry_count || 0;
        document.getElementById('selected-exit').textContent = selected.exit_code ?? '—';
        document.getElementById('selected-blockers').textContent = selected.blocked_by?.join(', ') ||
          (canCancel(selected.status) ? 'Worker capacity or scheduled delay' : '—');
        document.getElementById('selected-error').textContent = selected.last_error || '—';
        if (cancelJobButton && !cancelJobButton.dataset.pending) {
          cancelJobButton.disabled = !canCancel(selected.status);
        }
      }
    }
  } catch (error) {
    const connection = document.getElementById('connection');
    connection.textContent = 'Master offline';
    connection.classList.remove('online');
    connection.classList.add('offline');
  } finally {
    refreshFiles();
    // Keep a low-rate check after completion so an SDK redeploy or recovered
    // master eventually appears without returning to the old three-second load.
    setTimeout(refresh, refreshDelay(selectedStatus));
  }
}
refresh();

async function requestCancellation(button, url) {
  const feedback = document.getElementById('action-feedback');
  button.dataset.pending = 'true';
  button.disabled = true;
  feedback.classList.remove('error');
  feedback.textContent = 'Sending stop request…';
  try {
    const response = await fetch(url, { method: 'POST' });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Stop request failed');
    feedback.textContent = 'Stop requested. Status will update shortly.';
  } catch (error) {
    feedback.classList.add('error');
    feedback.textContent = error.message;
  } finally {
    delete button.dataset.pending;
  }
}

cancelDagButton?.addEventListener('click', () => requestCancellation(cancelDagButton,
  `/api/dag/cancel/${encodeURIComponent(cancelDagButton.dataset.dagId)}`));
cancelJobButton?.addEventListener('click', () => requestCancellation(cancelJobButton,
  `/api/dag/cancel/${encodeURIComponent(cancelJobButton.dataset.dagId)}/${encodeURIComponent(cancelJobButton.dataset.jobId)}`));

const redeployButton = document.getElementById('redeploy');
redeployButton?.addEventListener('click', async () => {
  redeployButton.disabled = true;
  redeployButton.textContent = 'Deploying…';
  try {
    const response = await fetch('/api/dag/redeploy/' + encodeURIComponent(redeployButton.dataset.dagId),
      { method: 'POST' });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Redeploy failed');
    location.reload();
  } catch (error) {
    redeployButton.textContent = error.message;
    redeployButton.disabled = false;
  }
});
