// Experiment 2 Panel: Multi-Threaded Pool Execution & Concurrency Burst

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp2 = {
  render: function(container) {
    container.innerHTML = `
      <div class="panel-inner">
        <div style="margin-bottom: 16px;">
          <h3 style="font-size: 16px; font-weight: 700;">Experiment 2: Thread-Pooled Concurrent Burst</h3>
          <p style="font-size: 13px; color: var(--text-muted);">
            Dispatches concurrent Subtasks to verify thread-pool concurrency gating, measure total makespan, and observe speedup across heterogeneous nodes.
          </p>
        </div>

        <form id="exp2-form">
          <div style="display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px;">
            <div class="form-group">
              <label class="form-label" for="exp2-node-select">Target Node</label>
              <select id="exp2-node-select" class="form-control"></select>
            </div>

            <div class="form-group">
              <label class="form-label" for="exp2-count-input">Burst Subtask Count</label>
              <input type="number" id="exp2-count-input" class="form-control" min="1" max="48" value="12">
            </div>

            <div class="form-group">
              <label class="form-label" for="exp2-type-select">Subtask Type</label>
              <select id="exp2-type-select" class="form-control">
                <option value="SUMMARIZE">SUMMARIZE</option>
                <option value="RETRIEVE">RETRIEVE</option>
                <option value="RANK">RANK</option>
                <option value="SYNTHESIZE">SYNTHESIZE</option>
              </select>
            </div>

            <div class="form-group" id="exp2-doc-group">
              <label class="form-label" for="exp2-doc-select">Document (SUMMARIZE)</label>
              <select id="exp2-doc-select" class="form-control"></select>
            </div>
          </div>

          <div style="display: flex; justify-content: flex-end; margin-top: 8px;">
            <button type="submit" id="exp2-burst-btn" class="btn btn-primary">
              <span>Dispatch Concurrent Burst</span>
            </button>
          </div>
        </form>

        <!-- Live Metrics / Burst Summary -->
        <div id="exp2-summary-container" style="margin-top: 20px; display: none;">
          <h4 style="font-size: 13px; font-weight: 600; text-transform: uppercase; color: var(--text-dim); margin-bottom: 8px;">
            Burst Performance Metrics
          </h4>
          <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 8px; padding: 14px;">
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;">
              <div class="stat-item">
                <span class="stat-label">Total Subtasks</span>
                <span class="stat-val" id="exp2-res-count">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Total Makespan</span>
                <span class="stat-val text-cyan" id="exp2-res-makespan">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Peak Queue / Ideal Makespan</span>
                <span class="stat-val" style="color: var(--accent-emerald);" id="exp2-res-ideal">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Target Pool Size</span>
                <span class="stat-val" id="exp2-res-pool">-</span>
              </div>
            </div>
          </div>

          <div style="margin-top: 14px; max-height: 240px; overflow-y: auto; border: 1px solid var(--border-color); border-radius: 8px;">
            <table class="events-table">
              <thead>
                <tr>
                  <th style="width: 40px;">#</th>
                  <th>Subtask ID</th>
                  <th>Payload</th>
                  <th style="width: 80px;">Sent</th>
                  <th style="width: 80px;">Received</th>
                  <th style="width: 90px;">Latency</th>
                </tr>
              </thead>
              <tbody id="exp2-calls-tbody"></tbody>
            </table>
          </div>
        </div>
      </div>
    `;

    const form = document.getElementById('exp2-form');
    const nodeSelect = document.getElementById('exp2-node-select');
    const countInput = document.getElementById('exp2-count-input');
    const typeSelect = document.getElementById('exp2-type-select');
    const burstBtn = document.getElementById('exp2-burst-btn');
    const docSelect = document.getElementById('exp2-doc-select');
    const docGroup = document.getElementById('exp2-doc-group');
    const AG = window.AgentGrid;
    const panel = this;

    // Node options come from the membership; rebuilt in place when it changes (selection kept).
    const nodeLabel = (id, n) => 'Node ' + id + (n ? ' (pool: ' + n.poolSize + ')' + (n.up ? '' : ' [OFFLINE]') : '');
    AG.fillNodeSelect(nodeSelect, nodeLabel);
    panel._onMembership = () => AG.fillNodeSelect(nodeSelect, nodeLabel);
    if (!panel._membershipSubscribed) {
      panel._membershipSubscribed = true;
      AG.onMembershipChange(m => {
        if (document.getElementById('exp2-node-select') && panel._onMembership) panel._onMembership(m);
      });
    }

    // SUMMARIZE summarises one corpus document: pick it here (default: the first one).
    AG.getCorpus().then(docs => {
      docSelect.replaceChildren(...docs.map(d => AG.h('option', { value: d.id }, d.id + ' — ' + d.title)));
    }).catch(err => {
      docSelect.replaceChildren(AG.h('option', { value: '' }, 'corpus unavailable: ' + err.message));
    });
    const syncDocVisibility = () => { docGroup.style.visibility = typeSelect.value === 'SUMMARIZE' ? 'visible' : 'hidden'; };
    typeSelect.addEventListener('change', syncDocVisibility);
    syncDocVisibility();

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const nodeId = parseInt(nodeSelect.value, 10);
      const count = parseInt(countInput.value, 10);
      const type = typeSelect.value;

      burstBtn.disabled = true;
      burstBtn.innerHTML = `<span>Executing burst of ${count}...</span>`;
      
      let peakQueue = 0;
      const peakTracker = (nodes) => {
        const n = nodes.find(x => x.id === nodeId);
        if (n && n.queueDepth !== null && n.queueDepth > peakQueue) {
          peakQueue = n.queueDepth;
        }
      };
      window.AgentGrid.onClusterUpdate(peakTracker);

      try {
        const resp = await window.AgentGrid.invokeRmi(nodeId, type, count, type === 'SUMMARIZE' ? docSelect.value : null);
        const node = window.AgentGrid.getState().nodes.find(n => n.id === nodeId);
        const poolSize = node ? node.poolSize : '?';

        document.getElementById('exp2-res-count').textContent = resp.count;
        document.getElementById('exp2-res-makespan').textContent = `${resp.makespanMs} ms`;
        document.getElementById('exp2-res-pool').textContent = `${poolSize} workers`;

        const workMs = resp.simulatedWorkMs || 150;
        const ideal = Math.ceil(count / (poolSize === '?' ? 1 : poolSize)) * workMs;
        document.getElementById('exp2-res-ideal').textContent = `Peak: ${peakQueue} / Ideal: ${ideal} ms`;

        const tbody = document.getElementById('exp2-calls-tbody');
        // Subtask output is text produced by the node; build cells with text nodes only.
        const h = window.AgentGrid.h;
        tbody.replaceChildren(...(resp.calls || []).map(c => h('tr', {}, [
          h('td', { className: 'mono-cell' }, String(c.index + 1)),
          h('td', { className: 'mono-cell' }, c.subtaskId),
          h('td', { style: { color: 'var(--text-main)', fontSize: '11px' } }, c.payload || c.error),
          h('td', { className: 'mono-cell' }, 'L=' + c.lamportSent),
          h('td', { className: 'mono-cell', style: { color: 'var(--accent-indigo)', fontWeight: '600' } }, 'L=' + c.lamportReceived),
          h('td', { className: 'mono-cell', style: { color: 'var(--accent-cyan)' } }, c.latencyMs + ' ms')
        ])));

        document.getElementById('exp2-summary-container').style.display = 'block';

      } catch (err) {
        alert('Burst execution failed: ' + err.message);
      } finally {
        burstBtn.disabled = false;
        burstBtn.innerHTML = `<span>Dispatch Concurrent Burst</span>`;
        const idx = window.AgentGrid.getState().clusterListeners.indexOf(peakTracker);
        if (idx > -1) window.AgentGrid.getState().clusterListeners.splice(idx, 1);
      }
    });
  }
};
