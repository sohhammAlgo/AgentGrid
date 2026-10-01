// Experiment 6 Panel: the same job under each routing policy, run by the orchestrator on the
// leader over the live members (by default 5 nodes with pools 2/4/4/6/6; WEIGHTED uses each
// member's weight). Every number shown comes from /api/jobs/{id}: subtasks per node, makespan,
// and each node's peak queue depth. The per-node columns are the current members plus any
// node a shown job used, and are rebuilt when the membership changes.

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp6 = {
  render: function(container) {
    const AG = window.AgentGrid;
    const POLICIES = ['ROUND_ROBIN', 'WEIGHTED', 'LEAST_LOADED'];

    container.innerHTML = `
      <div class="panel-inner" id="exp6-root">
        <div style="margin-bottom: 16px;">
          <h2 style="font-size: 16px; font-weight: 700;">Experiment 6: Load Balancing Policies</h2>
          <p style="font-size: 13px; color: var(--text-muted);">
            Runs the same job once per policy, one after another, through the leader's orchestrator.
            Each job fans RETRIEVE and SUMMARIZE out to up to 20 subtasks; nodes add a fixed simulated delay per subtask.
          </p>
        </div>
        <div class="exp4-box">
          <div class="exp4-row">
            <div class="form-group" style="margin-bottom: 0; flex: 1;">
              <label class="form-label" for="exp6-query">Query</label>
              <input id="exp6-query" class="form-control" maxlength="500"
                     value="How do leader election and failure detectors handle a crashed node?">
            </div>
            <div class="form-group" style="margin-bottom: 0;">
              <label class="form-label" for="exp6-runs">Runs per policy</label>
              <select id="exp6-runs" class="form-control">
                <option value="1">1</option>
                <option value="3">3</option>
              </select>
            </div>
            <button id="exp6-run" class="btn btn-primary" style="height: 38px;">Run comparison</button>
          </div>
          <div id="exp6-progress" class="job-stage-meta" style="margin-top: 10px;"></div>
        </div>
        <div id="exp6-results"></div>
      </div>
    `;

    const runBtn = document.getElementById('exp6-run');
    const panel = this;
    panel._rows = [];
    panel._onMembership = () => { if (panel._rows.length) renderTable(panel._rows); };
    if (!panel._membershipSubscribed) {
      panel._membershipSubscribed = true;
      AG.onMembershipChange(m => {
        if (document.getElementById('exp6-root') && panel._onMembership) panel._onMembership(m);
      });
    }

    function esc(v) {
      return String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function median(values) {
      const s = values.filter(v => v != null).sort((a, b) => a - b);
      if (!s.length) return null;
      return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2;
    }

    function renderTable(rows) {
      panel._rows = rows;
      const nodes = [...new Set(AG.memberIds().map(String).concat(rows.flatMap(r => Object.keys(r.job.subtasksPerNode || {})
        .concat(Object.keys(r.job.peakQueueDepth || {})))))].sort((a, b) => a - b);
      const byPolicy = POLICIES.map(p => ({ p, runs: rows.filter(r => r.policy === p) })).filter(x => x.runs.length);
      document.getElementById('exp6-results').innerHTML = `
        <div class="exp4-table-wrap" style="max-height: none;">
          <table class="events-table">
            <thead>
              <tr>
                <th>Policy</th><th>Run</th><th>Status</th><th>Makespan</th>
                ${nodes.map(n => `<th>N${esc(n)} subtasks</th>`).join('')}
                ${nodes.map(n => `<th>N${esc(n)} peak queue</th>`).join('')}
              </tr>
            </thead>
            <tbody>
              ${byPolicy.map(({ p, runs }) => runs.map((r, i) => `
                <tr>
                  <td class="mono-cell">${esc(p)}</td>
                  <td class="mono-cell">${i + 1}</td>
                  <td><span class="job-badge job-${esc(r.job.status)}">${esc(r.job.status)}</span></td>
                  <td class="mono-cell">${r.job.makespanMs == null ? '—' : r.job.makespanMs + ' ms'}</td>
                  ${nodes.map(n => `<td class="mono-cell">${esc((r.job.subtasksPerNode || {})[n] ?? 0)}</td>`).join('')}
                  ${nodes.map(n => `<td class="mono-cell">${esc((r.job.peakQueueDepth || {})[n] ?? '—')}</td>`).join('')}
                </tr>`).join('') + (runs.length > 1 ? `
                <tr>
                  <td class="mono-cell"><b>${esc(p)}</b></td><td class="mono-cell">median</td><td></td>
                  <td class="mono-cell"><b>${median(runs.map(r => r.job.makespanMs))} ms</b></td>
                  <td colspan="${nodes.length * 2}"></td>
                </tr>` : '')).join('')}
            </tbody>
          </table>
        </div>`;
    }

    runBtn.addEventListener('click', async () => {
      const query = document.getElementById('exp6-query').value;
      const runs = parseInt(document.getElementById('exp6-runs').value, 10);
      const progress = document.getElementById('exp6-progress');
      const rows = [];
      runBtn.disabled = true;
      try {
        for (let i = 0; i < runs; i++) {
          for (const policy of POLICIES) {
            progress.textContent = `Running ${policy} (run ${i + 1} of ${runs})...`;
            const resp = await AG.submitJob(query, policy);
            const job = await AG.waitForJob(resp.jobId);
            rows.push({ policy, job });
            renderTable(rows);
          }
        }
        progress.textContent = `Done: ${rows.length} jobs.`;
      } catch (err) {
        progress.replaceChildren(window.AgentGrid.h('span', { className: 'text-rose' }, err.message));
      } finally {
        runBtn.disabled = false;
      }
    });
  }
};
