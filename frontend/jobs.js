// Job Console: submit a query to the orchestrator on the elected leader and watch the
// pipeline (RETRIEVE -> RANK -> SUMMARIZE -> SYNTHESIZE) run across the nodes.
// Renders only what /api/jobs and /api/election report.

(function() {
  const AG = window.AgentGrid;
  let currentJobId = null;
  let pollToken = 0;

  function esc(v) {
    return String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function render() {
    const root = document.getElementById('job-console');
    root.innerHTML = `
      <form id="job-form" class="job-form">
        <input id="job-query" class="form-control" maxlength="500"
               value="How do leader election and failure detectors handle a crashed node?">
        <select id="job-policy" class="form-control">
          <option value="WEIGHTED">WEIGHTED</option>
          <option value="LEAST_LOADED">LEAST_LOADED</option>
          <option value="ROUND_ROBIN">ROUND_ROBIN</option>
        </select>
        <button type="submit" id="job-submit" class="btn btn-primary">Submit</button>
      </form>
      <div class="job-grid">
        <div>
          <div class="job-status-row" id="job-status">No job selected.</div>
          <div class="job-pipeline" id="job-pipeline"></div>
          <div class="job-answer" id="job-answer"></div>
        </div>
        <div>
          <h4 class="job-subhead">Recent jobs</h4>
          <div class="job-recent" id="job-recent"></div>
        </div>
      </div>
    `;

    document.getElementById('job-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = document.getElementById('job-submit');
      btn.disabled = true;
      try {
        const resp = await AG.submitJob(document.getElementById('job-query').value, document.getElementById('job-policy').value);
        select(resp.jobId);
      } catch (err) {
        document.getElementById('job-status').innerHTML = `<span class="text-rose">${esc(err.message)}</span>`;
      } finally {
        btn.disabled = false;
      }
    });
  }

  function statusBadge(status) {
    return `<span class="job-badge job-${esc(status)}">${esc(status)}</span>`;
  }

  function renderJob(job) {
    document.getElementById('job-status').innerHTML = `
      ${statusBadge(job.status)}
      <span class="mono-cell">${esc(job.jobId)}</span>
      <span>policy <b>${esc(job.policy)}</b></span>
      <span>leader node ${esc(job.leaderNode)}</span>
      <span>${job.completedSubtasks}/${job.subtasks} subtasks</span>
      <span>makespan ${job.makespanMs == null ? '—' : job.makespanMs + ' ms'}</span>
      ${job.orphanAdoptedBy != null ? `<span>recorded as orphan by node ${esc(job.orphanAdoptedBy)}</span>` : ''}
      ${job.error ? `<span class="text-rose">${esc(job.error)}</span>` : ''}
    `;
    document.getElementById('job-pipeline').innerHTML = (job.stages || []).map(stage => `
      <div class="job-stage">
        <div class="job-stage-head">
          <span>${esc(stage.type)}</span>
          <span class="job-stage-meta">${esc(stage.status)}${stage.durationMs != null ? ' · ' + stage.durationMs + ' ms' : ''}</span>
        </div>
        <div class="job-chips">
          ${(stage.subtasks || []).map(s => `
            <span class="job-chip chip-${esc(s.status)}" title="${esc(s.input)}&#10;dispatch L=${esc(s.dispatchLamport)} result L=${esc(s.resultLamport)} complete L=${esc(s.completeLamport)}">
              ${esc(s.subtaskId)} → ${s.node == null ? '—' : 'N' + esc(s.node)}
            </span>`).join('') || '<span class="job-stage-meta">no subtasks yet</span>'}
        </div>
      </div>
    `).join('');
    document.getElementById('job-answer').innerHTML = job.answer
      ? `<h4 class="job-subhead">Answer</h4><p>${esc(job.answer)}</p>`
      : (job.status === 'ORPHANED'
          ? `<h4 class="job-subhead">Answer</h4><p class="text-rose">The leader stopped before this job finished; completed subtasks are kept above. Recovery is not implemented.</p>`
          : '');
  }

  async function select(jobId) {
    currentJobId = jobId;
    const token = ++pollToken;
    try {
      await AG.waitForJob(jobId, job => {
        if (token === pollToken) renderJob(job);
      });
    } catch (err) {
      if (token === pollToken) {
        document.getElementById('job-status').innerHTML = `<span class="text-rose">${esc(err.message)}</span>`;
      }
    }
    refreshRecent();
  }

  async function refreshRecent() {
    try {
      const jobs = await AG.listJobs();
      const el = document.getElementById('job-recent');
      if (!jobs.length) {
        el.innerHTML = '<span class="job-stage-meta">No jobs yet.</span>';
        return;
      }
      el.innerHTML = `
        <table class="events-table">
          <thead><tr><th>Job</th><th>Policy</th><th>Status</th><th>Makespan</th></tr></thead>
          <tbody>
            ${jobs.slice(0, 12).map(j => `
              <tr class="job-row ${j.jobId === currentJobId ? 'selected' : ''}" data-job="${esc(j.jobId)}">
                <td class="mono-cell">${esc(j.jobId)}</td>
                <td class="mono-cell">${esc(j.policy)}</td>
                <td>${statusBadge(j.status)}</td>
                <td class="mono-cell">${j.makespanMs == null ? '—' : j.makespanMs + ' ms'}</td>
              </tr>`).join('')}
          </tbody>
        </table>`;
      el.querySelectorAll('.job-row').forEach(row => row.addEventListener('click', () => select(row.dataset.job)));
    } catch (ignored) {}
  }

  function renderLeader(el) {
    const badge = document.getElementById('job-leader-badge');
    if (!badge || !el) return;
    badge.textContent = el.leaderId == null ? 'leader: none agreed' : `orchestrator on leader node ${el.leaderId}`;
    badge.className = 'badge ' + (el.leaderId == null ? 'offline' : 'online');
  }

  document.addEventListener('DOMContentLoaded', () => {
    render();
    refreshRecent();
    AG.onElectionUpdate(renderLeader);
    setInterval(refreshRecent, 3000);
  });

  window.AgentGridJobs = { select };
})();
