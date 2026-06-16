'use strict';

// ── State ────────────────────────────────────────────────────────────────────

const state = {
  jobs: {},        // id → job object (latest from SSE)
  rates: {},       // id → { lastBytes, lastTime, ewmaRate } for ACQUIRING ETA
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

function exportSettings() {
  const s = getSettings();
  const blob = new Blob([JSON.stringify(s, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'az-settings.json';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 5000);
  toast('Settings exported.');
}

function importSettingsFile(event) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const s = JSON.parse(e.target.result);
      if (typeof s !== 'object' || s === null) throw new Error();
      localStorage.setItem(SETTINGS_KEY, JSON.stringify(s));
      loadSettingsForm();
      toast('Settings imported.');
      reconnectSSE();
    } catch {
      toast('Invalid settings file.', true);
    }
    event.target.value = '';
  };
  reader.readAsText(file);
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
  const prev = state.jobs[job.id];
  state.jobs[job.id] = job;
  updateRate(prev, job);
  renderJobsView();
}

function setSSEStatus(status, retries) {
  state.sseStatus = status;
  const dot = document.getElementById('sse-dot');
  dot.className = status;
  const retryStr = retries ? ` (retry #${retries})` : '';
  dot.title = `SSE: ${status}${retryStr}`;
}

// ── Rate tracking (ETA / speed for ACQUIRING) ─────────────────────────────────

function updateRate(prev, job) {
  if (job.status !== 'ACQUIRING') {
    delete state.rates[job.id];
    return;
  }
  if (!job.size || job.acquired_bytes == null) return;

  const now = Date.now();
  const r = state.rates[job.id];

  if (!r) {
    state.rates[job.id] = { lastBytes: job.acquired_bytes, lastTime: now, ewmaRate: 0 };
    return;
  }

  const deltaBytes = job.acquired_bytes - r.lastBytes;
  const deltaSec = (now - r.lastTime) / 1000;
  if (deltaSec < 0.5 || deltaBytes < 0) return;

  const instantRate = deltaBytes / deltaSec;
  const alpha = 0.3;
  const ewmaRate = r.ewmaRate === 0 ? instantRate : alpha * instantRate + (1 - alpha) * r.ewmaRate;
  state.rates[job.id] = { lastBytes: job.acquired_bytes, lastTime: now, ewmaRate };
}

function fmtRate(bytesPerSec) {
  if (!bytesPerSec || bytesPerSec <= 0) return null;
  return fmtSize(bytesPerSec) + '/s';
}

function fmtETA(remainingBytes, bytesPerSec) {
  if (!bytesPerSec || bytesPerSec <= 0 || remainingBytes <= 0) return null;
  const secs = Math.round(remainingBytes / bytesPerSec);
  if (secs < 60) return `${secs}s left`;
  if (secs < 3600) return `${Math.round(secs / 60)}m left`;
  return `${Math.floor(secs / 3600)}h ${Math.round((secs % 3600) / 60)}m left`;
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
  if (name === 'submit') {
    clearTimeout(probeTimer);
    const el = document.getElementById('probe-result');
    if (el) el.style.display = 'none';
  }
}

// ── Disk gauge ────────────────────────────────────────────────────────────────

async function updateDiskGauge() {
  const { backendURL } = getSettings();
  if (!backendURL) return;
  try {
    const resp = await fetch(`${backendURL}/healthz`);
    if (!resp.ok) return;
    const data = await resp.json();
    const el = document.getElementById('disk-gauge');
    if (!el) return;
    const free = data.scratch_free_bytes;
    const total = data.scratch_total_bytes;
    if (!free && !total) { el.textContent = ''; return; }
    el.textContent = `${fmtSize(free)} free`;
    el.className = 'disk-gauge' + (free < 5 * 1024 * 1024 * 1024 ? ' low' : '');
  } catch { /* silently ignore */ }
}

async function diskGaugeLoop() {
  while (true) {
    await updateDiskGauge();
    await sleep(30000);
  }
}

// ── URL probe ─────────────────────────────────────────────────────────────────

let probeTimer = null;
function probeURLDebounced() {
  clearTimeout(probeTimer);
  probeTimer = setTimeout(doProbe, 500);
}

async function doProbe() {
  const urlVal = document.getElementById('f-url').value.trim();
  const resultEl = document.getElementById('probe-result');
  if (!resultEl) return;
  if (!urlVal.startsWith('http')) { resultEl.style.display = 'none'; return; }

  const { backendURL, apiKey } = getSettings();
  if (!backendURL || !apiKey) return;

  try {
    const resp = await apiFetch(`/api/probe?url=${encodeURIComponent(urlVal)}`);
    if (!resp.ok) { resultEl.style.display = 'none'; return; }
    const data = await resp.json();

    if (data.error) {
      resultEl.textContent = `⚠ ${data.error}`;
      resultEl.style.display = 'block';
      return;
    }

    const parts = [];
    if (data.size != null) parts.push(fmtSize(data.size));
    if (data.content_type) parts.push(data.content_type.split(';')[0].trim());
    if (data.accept_ranges) parts.push('streaming ✓');

    if (data.filename) {
      const fnField = document.getElementById('f-filename');
      if (fnField && !fnField.value) fnField.value = data.filename;
    }

    resultEl.textContent = parts.join(' · ');
    resultEl.style.display = parts.length ? 'block' : 'none';
  } catch {
    resultEl.style.display = 'none';
  }
}

// ── Views ─────────────────────────────────────────────────────────────────────

const TERMINAL_STATUSES = new Set(['DONE', 'FAILED', 'CANCELED']);

function renderJobsView() {
  const container = document.getElementById('jobs-list');
  const allJobs = Object.values(state.jobs).sort(
    (a, b) => new Date(b.created_at) - new Date(a.created_at)
  );

  const active = allJobs.filter(j => !TERMINAL_STATUSES.has(j.status));
  const archived = allJobs.filter(j => TERMINAL_STATUSES.has(j.status));

  if (active.length === 0 && archived.length === 0) {
    const msg = state.sseStatus === 'connected'
      ? '<p>No jobs yet. Click the + button to submit one.</p>'
      : '<p>Connecting…</p>';
    container.innerHTML = `<div class="empty-state">${msg}</div>`;
    return;
  }

  let html = '';

  if (active.length > 0) {
    html += active.map(renderJobCard).join('');
  } else if (state.sseStatus === 'connected') {
    html += '<div class="empty-state" style="padding:40px 20px"><p>No active jobs. Click + to start one.</p></div>';
  }

  if (archived.length > 0) {
    const isOpen = localStorage.getItem('az_archive_open') !== 'false';
    html += `
      <div class="archive-header" onclick="toggleArchive()">
        <span>Archive (${archived.length})</span>
        <span class="archive-chevron">${isOpen ? '▲' : '▼'}</span>
      </div>
      <div id="archive-section"${isOpen ? '' : ' style="display:none"'}>
        ${archived.map(renderJobCard).join('')}
      </div>`;
  }

  container.innerHTML = html;
}

function toggleArchive() {
  const section = document.getElementById('archive-section');
  if (!section) return;
  const isOpen = section.style.display !== 'none';
  section.style.display = isOpen ? 'none' : '';
  const chevron = document.querySelector('.archive-chevron');
  if (chevron) chevron.textContent = isOpen ? '▼' : '▲';
  localStorage.setItem('az_archive_open', String(!isOpen));
}

function ncFolderURL() {
  const { ncURL, ncToken } = getSettings();
  if (!ncURL || !ncToken) return null;
  try {
    const publicPhp = ncURL.indexOf('/public.php');
    const base = publicPhp >= 0 ? ncURL.slice(0, publicPhp) : new URL(ncURL).origin;
    return `${base}/s/${ncToken}`;
  } catch { return null; }
}

function renderJobCard(job) {
  const statusLow = job.status.toLowerCase();
  const size = job.size != null ? fmtSize(job.size) : '—';
  const progress = job.chunks_total > 0
    ? `Chunk ${job.chunks_done} / ${job.chunks_total}`
    : '';

  let acquireDetail = '';
  if (job.status === 'ACQUIRING' && job.size) {
    const r = state.rates[job.id];
    const rate = r ? fmtRate(r.ewmaRate) : null;
    const eta = (r && r.ewmaRate > 0) ? fmtETA(job.size - job.acquired_bytes, r.ewmaRate) : null;
    acquireDetail = `${fmtSize(job.acquired_bytes)} / ${size}`;
    if (rate) acquireDetail += ` · ${rate}`;
    if (eta) acquireDetail += ` · ${eta}`;
  }

  const meta = [size, job.stage, progress || acquireDetail].filter(Boolean).join(' · ');

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
    body = `
      <p class="job-error">Error: ${esc(job.error || 'unknown')}</p>
      <div class="chunk-action" style="margin-top: 12px;">
        <span class="chunk-label">Retry will submit a fresh job with the same URL and headers.</span>
        <button class="btn btn-primary btn-sm" onclick="retryJob('${esc(job.id)}')">↻ Retry</button>
      </div>`;
  } else if (job.status === 'CANCELED') {
    body = `
      <div class="chunk-action">
        <span class="chunk-label">Job canceled.</span>
        <button class="btn btn-primary btn-sm" onclick="retryJob('${esc(job.id)}')">↻ Retry</button>
      </div>`;
  }

  const deleteTitle = TERMINAL_STATUSES.has(job.status) ? 'Delete from list' : 'Cancel and delete';
  const deleteBtn = `<button class="btn btn-ghost btn-sm" onclick="deleteJob('${esc(job.id)}')" title="${deleteTitle}">✕</button>`;

  const folderHref = ncFolderURL();
  const folderBtn = folderHref
    ? `<a class="btn btn-ghost btn-sm" href="${esc(folderHref)}" target="_blank" rel="noopener" title="Browse Nextcloud folder">↗</a>`
    : '';

  return `
    <div class="job-card status-${statusLow}">
      <div class="job-header">
        <span class="job-filename" title="${esc(job.filename)}">${esc(job.filename)}</span>
        <span class="badge ${statusLow}">${job.status}</span>
        ${folderBtn}
        ${deleteBtn}
      </div>
      <div class="job-meta">${esc(meta)}</div>
      ${progressBar}
      ${body}
    </div>`;
}

function renderChunkAction(job) {
  const chunk = job.current_chunk;
  const name = chunk.name;

  // Chunk is still uploading to Nextcloud — not available for download yet.
  if (chunk.status === 'uploading') {
    return `
      <div class="chunk-action">
        <span class="chunk-label">Uploading <strong>${esc(name)}</strong> (${fmtSize(chunk.size)}) to Nextcloud…</span>
      </div>`;
  }

  // Chunk is uploaded — show download + ack UI.
  let downloadBtn = '';
  const { ncURL, ncToken } = getSettings();
  if (ncURL && ncToken) {
    try {
      const publicPhp = ncURL.indexOf('/public.php');
      const base = publicPhp >= 0 ? ncURL.slice(0, publicPhp) : new URL(ncURL).origin;
      const href = `${base}/s/${ncToken}/download?files=${encodeURIComponent(chunk.name)}`;
      downloadBtn = `<a class="btn btn-primary btn-sm" href="${esc(href)}" target="_blank" rel="noopener">⬇ Download from Nextcloud</a>`;
    } catch { /* ignore */ }
  }

  // Secondary line shown when the next chunk is already being pre-uploaded.
  let nextUploadingLine = '';
  if (job.uploading_chunk) {
    const nextName = job.uploading_chunk.name;
    nextUploadingLine = `<span class="chunk-label" style="opacity:0.65">Uploading ${esc(nextName)} in background…</span>`;
  }

  return `
    <div class="chunk-action">
      <span class="chunk-label">Ready: <strong>${esc(name)}</strong> (${fmtSize(chunk.size)})</span>
      ${downloadBtn}
      <button class="btn btn-success btn-sm" onclick="ackChunk('${esc(job.id)}', ${chunk.idx})">
        ✓ Mark done
      </button>
      ${nextUploadingLine}
    </div>`;
}

function renderDone(job) {
  if (job.no_chunk) {
    return `
      <div class="chunk-action">
        <span class="chunk-label">Downloaded as <strong>${esc(job.filename)}</strong>.</span>
      </div>`;
  }

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
    return `cd %USERPROFILE%\\Downloads\\${job.id}\r\ncopy /b ${cmdQuote(`${job.filename}.part*`)} ${cmdQuote(job.filename)}`;
  }
  const partPrefix = shQuote(`${job.filename}.part`);
  return `cd ~/Downloads/${job.id}\ncat ${partPrefix}* > ${shQuote(job.filename)} && rm ${partPrefix}*`;
}

function shQuote(value) {
  return `'${String(value).replaceAll("'", "'\\''")}'`;
}

function cmdQuote(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
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
    no_chunk: document.getElementById('f-no-chunk').checked,
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
  const job = state.jobs[jobId];
  const isTerminal = job && TERMINAL_STATUSES.has(job.status);
  const prompt = isTerminal
    ? 'Delete this job from the list? Any scratch file on the VPS will be removed.'
    : 'Cancel and delete this job? The goroutine will stop and the scratch file will be deleted.';
  if (!confirm(prompt)) return;
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

async function retryJob(jobId) {
  const job = state.jobs[jobId];
  if (!job) return;
  const s = getSettings();
  if (!s.ncURL || !s.ncToken) {
    toast('Configure Nextcloud URL and token in Settings.', true);
    return;
  }

  const body = {
    url: job.url,
    filename: job.filename || undefined,
    referer: job.referer || undefined,
    user_agent: job.user_agent || undefined,
    stage: job.stage,
    deliver_now: job.deliver_now,
    no_chunk: job.no_chunk,
    nextcloud_url: s.ncURL,
    nextcloud_token: s.ncToken,
  };

  try {
    const resp = await apiFetch('/api/jobs', { method: 'POST', body: JSON.stringify(body) });
    if (!resp.ok) { const t = await resp.text(); toast(`Retry failed: ${t}`, true); return; }
    const newJob = await resp.json();
    state.jobs[newJob.id] = newJob;
    // Remove the old failed/canceled job — it's been superseded.
    await apiFetch(`/api/jobs/${jobId}`, { method: 'DELETE' }).catch(() => {});
    delete state.jobs[jobId];
    renderJobsView();
    toast('Retrying as new job.');
  } catch (e) {
    toast(`Error: ${e.message}`, true);
  }
}

function copyCmd(jobId) {
  const el = document.getElementById(`cmd-${jobId}`);
  if (!el) return;
  navigator.clipboard.writeText(el.textContent).then(() => toast('Copied!'));
}

// ── Utilities ─────────────────────────────────────────────────────────────────


function fmtSize(bytes) {
  if (bytes == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let n = bytes, i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i > 0 ? n.toFixed(1) : Math.round(n)) + ' ' + units[i];
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
  renderJobsView();

  diskGaugeLoop();

  if (!s.backendURL || !s.apiKey) {
    openModal('settings');
    toast('Configure your backend URL and API key to get started.');
  } else {
    startSSE();
  }

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('sw.js').catch(console.warn);
  }

  // Keyboard shortcuts
  document.addEventListener('keydown', e => {
    // Don't intercept when typing in an input or textarea
    const tag = e.target.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || e.target.isContentEditable) return;
    if (e.key === 'n' && !e.ctrlKey && !e.metaKey && !e.altKey) {
      openModal('submit');
    }
    if (e.key === 'Escape') {
      ['submit', 'settings'].forEach(name => closeModal(null, name));
    }
  });

  // Paste-to-submit: paste a URL anywhere outside inputs to open the submit modal
  document.addEventListener('paste', e => {
    const tag = e.target.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA') return;
    const text = (e.clipboardData || window.clipboardData).getData('text/plain').trim();
    if (text.startsWith('http://') || text.startsWith('https://')) {
      openModal('submit');
      const urlField = document.getElementById('f-url');
      urlField.value = text;
      probeURLDebounced();
    }
  });
}

document.addEventListener('DOMContentLoaded', init);
