'use strict';

// ── State ────────────────────────────────────────────────────────────────────

const state = {
  jobs: {},        // id → job object (latest from SSE)
  sseStatus: 'connecting',
  sseAbort: null,
};

// ── Settings ─────────────────────────────────────────────────────────────────

const SETTINGS_KEY = 'az_settings';

function getSettings() {
  try { return JSON.parse(localStorage.getItem(SETTINGS_KEY) || '{}'); } catch { return {}; }
}

function saveSettings() {
  const s = {
    backendURL: document.getElementById('s-backend').value.replace(/\/$/, ''),
    apiKey: document.getElementById('s-apikey').value,
    ncURL: document.getElementById('s-ncurl').value.replace(/\/$/, ''),
    ncToken: document.getElementById('s-nctoken').value,
    isOwner: document.getElementById('s-owner').checked,
  };
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(s));
  toast('Settings saved.');
  closeModal(null, 'settings');
  reconnectSSE();
}

function loadSettingsForm() {
  const s = getSettings();
  document.getElementById('s-backend').value = s.backendURL || '';
  document.getElementById('s-apikey').value = s.apiKey || '';
  document.getElementById('s-ncurl').value = s.ncURL || '';
  document.getElementById('s-nctoken').value = s.ncToken || '';
  document.getElementById('s-owner').checked = !!s.isOwner;
}

// ── API helpers ───────────────────────────────────────────────────────────────

async function apiFetch(path, opts = {}) {
  const { backendURL, apiKey } = getSettings();
  if (!backendURL || !apiKey) throw new Error('Backend not configured. Check Settings.');
  const headers = { 'Authorization': `Bearer ${apiKey}`, ...opts.headers };
  if (opts.body) headers['Content-Type'] = 'application/json';
  return fetch(backendURL + path, { ...opts, headers });
}

// ── SSE ───────────────────────────────────────────────────────────────────────

function reconnectSSE() {
  if (state.sseAbort) { state.sseAbort.abort(); state.sseAbort = null; }
  startSSE();
}

async function startSSE() {
  let delay = 2000;
  let retries = 0;
  while (true) {
    setSSEStatus('connecting', retries);
    await runSSE();
    retries++;
    setSSEStatus('disconnected', retries);
    await sleep(delay);
    delay = Math.min(delay * 2, 30000);
  }
}

async function runSSE() {
  const { backendURL, apiKey } = getSettings();
  if (!backendURL || !apiKey) return;

  state.sseAbort = new AbortController();
  try {
    const resp = await fetch(`${backendURL}/api/jobs/stream`, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
      signal: state.sseAbort.signal,
    });
    if (!resp.ok) { console.warn('SSE HTTP', resp.status); return; }

    setSSEStatus('connected');

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });

      // Parse SSE blocks (separated by \n\n)
      let boundary;
      while ((boundary = buf.indexOf('\n\n')) !== -1) {
        const block = buf.slice(0, boundary);
        buf = buf.slice(boundary + 2);
        for (const line of block.split('\n')) {
          if (line.startsWith('data: ')) {
            try { onJobUpdate(JSON.parse(line.slice(6))); } catch { /* ignore malformed */ }
          }
        }
      }
    }
  } catch (e) {
    if (e.name !== 'AbortError') console.warn('SSE error:', e.message);
  }
}

function onJobUpdate(job) {
  state.jobs[job.id] = job;
  renderJobsView();
}

function setSSEStatus(status, retries) {
  state.sseStatus = status;
  const dot = document.getElementById('sse-dot');
  dot.className = status;
  const retryStr = retries ? ` (retry #${retries})` : '';
  dot.title = `SSE: ${status}${retryStr}`;
}

// ── Modals ────────────────────────────────────────────────────────────────────

function openModal(name) {
  document.getElementById(`modal-${name}`).classList.add('open');
  if (name === 'submit') {
    const s = getSettings();
    document.getElementById('hint-backend').textContent = s.backendURL || '(not set)';
  }
  if (name === 'settings') {
    loadSettingsForm();
  }
}

function closeModal(event, name) {
  if (event) event.stopPropagation();
  document.getElementById(`modal-${name}`).classList.remove('open');
}

// ── Views ─────────────────────────────────────────────────────────────────────

function renderJobsView() {
  const container = document.getElementById('jobs-list');
  const jobs = Object.values(state.jobs).sort(
    (a, b) => new Date(b.created_at) - new Date(a.created_at)
  );

  if (jobs.length === 0) {
    const msg = state.sseStatus === 'connected'
      ? '<p>No jobs yet. Click the + button to submit one.</p>'
      : '<p>Connecting…</p>';
    container.innerHTML = `<div class="empty-state">${msg}</div>`;
    return;
  }
  container.innerHTML = jobs.map(renderJobCard).join('');
}

function renderJobCard(job) {
  const statusLow = job.status.toLowerCase();
  const size = job.size != null ? fmtSize(job.size) : '—';
  const progress = job.chunks_total > 0
    ? `Chunk ${job.chunks_done} / ${job.chunks_total}`
    : '';
  const acquireProgress = job.status === 'ACQUIRING' && job.size
    ? `${fmtSize(job.acquired_bytes)} / ${size}`
    : '';
  const meta = [size, job.stage, progress || acquireProgress].filter(Boolean).join(' · ');

  let progressBar = '';
  if (job.status === 'ACQUIRING' && job.size) {
    const pct = Math.round((job.acquired_bytes / job.size) * 100);
    progressBar = progressBarHTML(pct);
  } else if (job.chunks_total > 0) {
    const pct = Math.round((job.chunks_done / job.chunks_total) * 100);
    progressBar = progressBarHTML(pct);
  }

  let body = '';
  if (job.status === 'DELIVERING' && job.current_chunk) {
    body = renderChunkAction(job);
  } else if (job.status === 'STAGED' && !job.deliver_now) {
    const s = getSettings();
    const moveBtn = (s.isOwner && job.stage === 'vps')
      ? `<button class="btn btn-ghost btn-sm" onclick="moveToGdrive('${esc(job.id)}')">☁ Move to GDrive</button>`
      : '';
    body = `<div class="chunk-action">
      <span class="chunk-label">File staged on server, awaiting delivery.</span>
      <button class="btn btn-primary btn-sm" onclick="startDeliver('${esc(job.id)}')">Start delivery</button>
      ${moveBtn}
    </div>`;
  } else if (job.status === 'STAGED' && job.stage === 'gdrive') {
    const s = getSettings();
    const deliverBtn = s.isOwner
      ? `<button class="btn btn-primary btn-sm" onclick="startDeliver('${esc(job.id)}')">Deliver from GDrive</button>`
      : `<span class="chunk-label">Archived to GDrive. Owner must start delivery.</span>`;
    body = `<div class="chunk-action">${deliverBtn}</div>`;
  } else if (job.status === 'DONE') {
    body = renderDone(job);
  } else if (job.status === 'FAILED') {
    body = `<p class="job-error">Error: ${esc(job.error || 'unknown')}</p>`;
  }

  const deleteBtn = ['DONE', 'FAILED', 'CANCELED'].includes(job.status)
    ? '' // no delete for terminal states (nothing to clean up server-side)
    : `<button class="btn btn-ghost btn-sm" onclick="deleteJob('${esc(job.id)}')" title="Cancel job">✕</button>`;

  return `
    <div class="job-card status-${statusLow}">
      <div class="job-header">
        <span class="job-filename" title="${esc(job.filename)}">${esc(job.filename)}</span>
        <span class="badge ${statusLow}">${job.status}</span>
        ${deleteBtn}
      </div>
      <div class="job-meta">${esc(meta)}</div>
      ${progressBar}
      ${body}
    </div>`;
}

function renderChunkAction(job) {
  const chunk = job.current_chunk;
  const name = chunkName(chunk.idx);
  const sha256Row = chunk.sha256
    ? `<div class="chunk-sha" title="SHA-256 for integrity check">
        <span class="sha-label">SHA-256</span>
        <code class="sha-value" id="sha-${esc(job.id)}-${chunk.idx}">${esc(chunk.sha256)}</code>
        <button class="btn btn-ghost btn-sm" onclick="copySHA('${esc(job.id)}', ${chunk.idx})">Copy</button>
       </div>`
    : '';
  return `
    <div class="chunk-action">
      <span class="chunk-label">Ready: <strong>${esc(name)}</strong> (${fmtSize(chunk.size)})</span>
      <button class="btn btn-primary btn-sm" onclick="downloadChunk('${esc(job.id)}', ${chunk.idx}, '${esc(chunk.url)}')">
        ⬇ Download
      </button>
      <button class="btn btn-success btn-sm" onclick="ackChunk('${esc(job.id)}', ${chunk.idx})">
        ✓ Mark done
      </button>
    </div>
    ${sha256Row}`;
}

function renderDone(job) {
  return `
    <div class="concat-box">
      <p>All chunks downloaded. Reassemble with:</p>
      <div class="concat-cmd">
        <code id="cmd-${esc(job.id)}">${esc(buildConcatCmd(job))}</code>
        <button class="btn btn-ghost btn-sm" onclick="copyCmd('${esc(job.id)}')">Copy</button>
      </div>
    </div>`;
}

function buildConcatCmd(job) {
  const isWin = navigator.userAgent.includes('Windows');
  if (isWin) {
    return `cd %USERPROFILE%\\Downloads\\${job.id}\r\ncopy /b part_*.bin "${job.filename}"`;
  }
  return `cd ~/Downloads/${job.id}\ncat part_*.bin > "${job.filename}" && rm part_*.bin`;
}

function progressBarHTML(pct) {
  return `<div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>`;
}

// ── Actions ───────────────────────────────────────────────────────────────────

async function submitJob() {
  const url = document.getElementById('f-url').value.trim();
  if (!url) { toast('URL is required.', true); return; }

  const s = getSettings();
  if (!s.ncURL || !s.ncToken) { toast('Configure Nextcloud URL and token in Settings.', true); return; }

  const body = {
    url,
    filename: document.getElementById('f-filename').value.trim() || undefined,
    referer: document.getElementById('f-referer').value.trim() || undefined,
    user_agent: document.getElementById('f-ua').value.trim() || undefined,
    stage: document.getElementById('f-stage').value,
    deliver_now: document.getElementById('f-deliver-now').checked,
    nextcloud_url: s.ncURL,
    nextcloud_token: s.ncToken,
  };

  try {
    const resp = await apiFetch('/api/jobs', { method: 'POST', body: JSON.stringify(body) });
    if (!resp.ok) { const t = await resp.text(); toast(`Submit failed: ${t}`, true); return; }
    const job = await resp.json();
    state.jobs[job.id] = job;
    // Clear form
    ['f-url', 'f-filename', 'f-referer', 'f-ua'].forEach(id => document.getElementById(id).value = '');
    toast('Job submitted.');
    closeModal(null, 'submit');
  } catch (e) {
    toast(`Error: ${e.message}`, true);
  }
}

async function downloadChunk(jobId, chunkIdx, chunkURL) {
  const { ncToken } = getSettings();
  const name = chunkName(chunkIdx);
  toast(`Fetching ${name}…`);
  try {
    const resp = await fetch(chunkURL, {
      headers: { 'Authorization': 'Basic ' + btoa(ncToken + ':') },
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

    // Prefer streaming to disk (Chrome/Edge); fall back to buffered blob.
    if ('showSaveFilePicker' in window) {
      try {
        const fh = await window.showSaveFilePicker({ suggestedName: name });
        const writable = await fh.createWritable();
        await resp.body.pipeTo(writable);
        toast(`${name} saved.`);
        return;
      } catch (e) {
        if (e.name === 'AbortError') return; // user cancelled picker
        // Fall through to blob on other errors.
      }
    }

    const blob = await resp.blob();
    const blobURL = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = blobURL; a.download = name;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(blobURL), 10000);
    toast(`${name} downloading.`);
  } catch (e) {
    toast(`Download failed: ${e.message}`, true);
  }
}

async function ackChunk(jobId, chunkIdx) {
  try {
    const resp = await apiFetch(`/api/jobs/${jobId}/chunk-done`, {
      method: 'POST',
      body: JSON.stringify({ idx: chunkIdx }),
    });
    if (!resp.ok) {
      const t = await resp.text();
      toast(`Ack failed: ${t}`, true);
    }
    // SSE will push the updated state; no local update needed.
  } catch (e) {
    toast(`Error: ${e.message}`, true);
  }
}

async function startDeliver(jobId) {
  try {
    const resp = await apiFetch(`/api/jobs/${jobId}/deliver`, { method: 'POST' });
    if (!resp.ok) { const t = await resp.text(); toast(`Deliver failed: ${t}`, true); }
  } catch (e) {
    toast(`Error: ${e.message}`, true);
  }
}

async function moveToGdrive(jobId) {
  if (!confirm('Move this staged file to Google Drive? The VPS scratch copy will be deleted.')) return;
  try {
    const resp = await apiFetch(`/api/jobs/${jobId}/move-to-gdrive`, { method: 'POST' });
    if (!resp.ok) { const t = await resp.text(); toast(`Move failed: ${t}`, true); return; }
    toast('Moving to GDrive…');
  } catch (e) {
    toast(`Error: ${e.message}`, true);
  }
}

async function deleteJob(jobId) {
  if (!confirm('Cancel this job? The goroutine will stop and scratch file will be deleted.')) return;
  try {
    const resp = await apiFetch(`/api/jobs/${jobId}`, { method: 'DELETE' });
    if (resp.ok) {
      delete state.jobs[jobId];
      renderJobsView();
    } else {
      const t = await resp.text();
      toast(`Delete failed: ${t}`, true);
    }
  } catch (e) {
    toast(`Error: ${e.message}`, true);
  }
}

function copyCmd(jobId) {
  const el = document.getElementById(`cmd-${jobId}`);
  if (!el) return;
  navigator.clipboard.writeText(el.textContent).then(() => toast('Copied!'));
}

function copySHA(jobId, chunkIdx) {
  const el = document.getElementById(`sha-${jobId}-${chunkIdx}`);
  if (!el) return;
  navigator.clipboard.writeText(el.textContent.trim()).then(() => toast('SHA-256 copied!'));
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function chunkName(idx) {
  return `part_${String(idx).padStart(4, '0')}.bin`;
}

function fmtSize(bytes) {
  if (bytes == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let n = bytes, i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i > 0 ? n.toFixed(1) : Math.round(n)) + ' ' + units[i];
}

function esc(str) {
  const d = document.createElement('div');
  d.textContent = String(str ?? '');
  return d.innerHTML;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

let toastTimer = null;
function toast(msg, isError = false) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'show' + (isError ? ' error' : '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.className = ''; }, 3500);
}

// ── Boot ──────────────────────────────────────────────────────────────────────

function init() {
  const s = getSettings();
  renderJobsView(); // Start by rendering jobs view unconditionally
  
  if (!s.backendURL || !s.apiKey) {
    openModal('settings');
    toast('Configure your backend URL and API key to get started.');
  } else {
    startSSE();
  }

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('sw.js').catch(console.warn);
  }
}

document.addEventListener('DOMContentLoaded', init);