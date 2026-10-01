// Experiment 4 Panel: Leader Election (Bully / Ring) with failure detection

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp4 = {
  render: function(container) {
    const AG = window.AgentGrid;

    container.innerHTML = `
      <div class="panel-inner" id="exp4-root">
        <div style="margin-bottom: 16px;">
          <h3 style="font-size: 16px; font-weight: 700;">Experiment 4: Leader Election</h3>
          <p style="font-size: 13px; color: var(--text-muted);">
            Every node pings its leader every 500 ms; three missed heartbeats trigger an election.
            Convergence is measured by the control plane from node events: first ELECTION_STARTED to last LEADER_ACCEPTED.
          </p>
        </div>

        <div class="exp4-box">
          <h4>Controls</h4>
          <div class="exp4-row">
            <div class="form-group" style="margin-bottom: 0;">
              <label class="form-label">Algorithm</label>
              <div class="toggle-group">
                <button class="toggle-btn" id="exp4-alg-BULLY">Bully</button>
                <button class="toggle-btn" id="exp4-alg-RING">Ring</button>
              </div>
            </div>
            <div class="form-group" style="margin-bottom: 0; flex: 1;">
              <label class="form-label" for="exp4-start-node">Start election from</label>
              <select id="exp4-start-node" class="form-control"></select>
            </div>
            <button id="exp4-start-btn" class="btn btn-primary" style="height: 38px;">Start Election</button>
            <button id="exp4-kill-btn" class="btn btn-danger" style="height: 38px;">Kill Leader</button>
          </div>
        </div>

        <div class="exp4-box">
          <h4>Agreement</h4>
          <div class="exp4-stats">
            <div class="stat-item"><span class="stat-label">Algorithm</span><span class="stat-val" id="exp4-alg">—</span></div>
            <div class="stat-item"><span class="stat-label">Agreed</span><span class="stat-val" id="exp4-agreed">—</span></div>
            <div class="stat-item"><span class="stat-label">Leader</span><span class="stat-val" id="exp4-leader">—</span></div>
            <div class="stat-item"><span class="stat-label">Backup</span><span class="stat-val" id="exp4-backup">—</span></div>
          </div>
          <div class="exp4-views" id="exp4-views"></div>
        </div>

        <div class="exp4-box" style="margin-bottom: 0;">
          <h4>Last Election Episode</h4>
          <div id="exp4-episode"></div>
        </div>
      </div>
    `;

    const startSelect = document.getElementById('exp4-start-node');
    const startBtn = document.getElementById('exp4-start-btn');
    const killBtn = document.getElementById('exp4-kill-btn');

    function esc(v) {
      return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    // Runs on every cluster update (up to 5 times a second): the options are created once per
    // node and then only relabelled, so an open dropdown is not rebuilt under the cursor.
    function renderPicker() {
      const nodes = AG.getState().nodes;
      nodes.forEach(n => {
        let opt = startSelect.querySelector(`option[value="${n.id}"]`);
        if (!opt) {
          opt = document.createElement('option');
          opt.value = String(n.id);
          startSelect.appendChild(opt);
        }
        const label = 'Node ' + n.id + (n.up ? '' : ' [OFFLINE]');
        if (opt.textContent !== label) opt.textContent = label;
        if (opt.disabled === !!n.up) opt.disabled = !n.up;
      });
    }

    function renderElection(el) {
      if (!document.getElementById('exp4-root')) return;
      if (!el) return;

      ['BULLY', 'RING'].forEach(a => {
        document.getElementById('exp4-alg-' + a).classList.toggle('active', el.algorithm === a);
      });
      document.getElementById('exp4-alg').textContent = el.algorithm || '—';
      const agreedEl = document.getElementById('exp4-agreed');
      agreedEl.textContent = el.agreed ? 'yes' : 'no';
      agreedEl.style.color = el.agreed ? 'var(--accent-emerald)' : 'var(--accent-rose)';
      document.getElementById('exp4-leader').textContent = el.leaderId === null ? '—' : 'Node ' + el.leaderId;
      document.getElementById('exp4-backup').textContent = el.backupId === null ? '—' : 'Node ' + el.backupId;
      killBtn.disabled = el.leaderId === null;

      const views = el.views || {};
      document.getElementById('exp4-views').innerHTML = Object.keys(views).map(id =>
        `<span class="badge" style="font-family: var(--font-mono);">Node ${id} &rarr; ${views[id] === null ? '—' : views[id]}</span>`
      ).join('');

      const ep = el.lastEpisode;
      const box = document.getElementById('exp4-episode');
      const epKey = ep ? ep.startTrueMs + ':' + ep.endTrueMs : 'none';
      if (box.dataset.key === epKey) return;
      box.dataset.key = epKey;
      if (!ep) {
        box.innerHTML = `<span style="font-size: 12px; color: var(--text-dim);">No election episode has completed yet.</span>`;
        return;
      }
      const kinds = Object.keys(ep.byKind || {});
      box.innerHTML = `
        <div class="exp4-stats">
          <div class="stat-item"><span class="stat-label">Convergence</span><span class="stat-val text-cyan">${ep.convergenceMs} ms</span></div>
          <div class="stat-item"><span class="stat-label">Messages</span><span class="stat-val">${ep.messages}</span></div>
          <div class="stat-item"><span class="stat-label">Elected</span><span class="stat-val">${ep.leaderId === null ? '—' : 'Node ' + ep.leaderId}</span></div>
          <div class="stat-item"><span class="stat-label">Initiators</span><span class="stat-val">${esc((ep.initiators || []).join(', '))}</span></div>
        </div>
        <div class="exp4-views">
          ${kinds.map(k => `<span class="badge">${esc(k)}: ${ep.byKind[k]}</span>`).join('')}
        </div>
        <div class="exp4-table-wrap">
          <table class="events-table">
            <thead>
              <tr><th>+ms</th><th>Kind</th><th>From</th><th>To</th><th>Lamport</th></tr>
            </thead>
            <tbody>
              ${(ep.table || []).map(r => `
                <tr>
                  <td class="mono-cell">${r.offsetMs}</td>
                  <td><span class="type-badge type-ELECTION_MSG">${esc(r.kind)}</span></td>
                  <td class="mono-cell">Node ${r.from}</td>
                  <td class="mono-cell">Node ${r.to}</td>
                  <td class="mono-cell" style="color: var(--accent-indigo);">L=${r.lamport}</td>
                </tr>`).join('')}
            </tbody>
          </table>
        </div>
      `;
    }

    ['BULLY', 'RING'].forEach(a => {
      document.getElementById('exp4-alg-' + a).addEventListener('click', async () => {
        try {
          await AG.setElectionAlgorithm(a);
        } catch (err) {
          alert('Failed to set algorithm: ' + err.message);
        }
      });
    });

    startBtn.addEventListener('click', async () => {
      startBtn.disabled = true;
      try {
        await AG.startElection(parseInt(startSelect.value, 10));
      } catch (err) {
        alert('Failed to start election: ' + err.message);
      } finally {
        startBtn.disabled = false;
      }
    });

    killBtn.addEventListener('click', async () => {
      const el = AG.getState().election;
      if (!el || el.leaderId === null) return;
      killBtn.disabled = true;
      try {
        await AG.killNode(el.leaderId);
      } catch (err) {
        alert('Failed to kill leader: ' + err.message);
      }
    });

    renderPicker();
    renderElection(AG.getState().election);

    // Subscribe once; always call the render functions of the latest render().
    const panel = this;
    panel._renderElection = renderElection;
    panel._renderPicker = renderPicker;
    if (!panel._subscribed) {
      panel._subscribed = true;
      AG.onElectionUpdate(el => panel._renderElection(el));
      AG.onClusterUpdate(() => {
        if (document.getElementById('exp4-root')) panel._renderPicker();
      });
    }
  }
};
