// Experiment 1 Panel: Single Remote Method Invocation (RMI)

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp1 = {
  render: function(container) {
    container.innerHTML = `
      <div class="panel-inner">
        <div style="margin-bottom: 16px;">
          <h2 style="font-size: 16px; font-weight: 700;">Experiment 1: RMI Remote Subtask Invocation</h2>
          <p style="font-size: 13px; color: var(--text-muted);">
            Dispatches a single Subtask to a selected cluster node over Java RMI and verifies remote execution, Lamport clock ordering, and round-trip latency.
          </p>
        </div>

        <form id="exp1-form">
          <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px;">
            <div class="form-group">
              <label class="form-label" for="exp1-node-select">Target Node</label>
              <select id="exp1-node-select" class="form-control"></select>
            </div>

            <div class="form-group">
              <label class="form-label" for="exp1-type-select">Subtask Type</label>
              <select id="exp1-type-select" class="form-control">
                <option value="SUMMARIZE">SUMMARIZE (Extract key points)</option>
                <option value="RETRIEVE">RETRIEVE (Query documents)</option>
                <option value="RANK">RANK (Relevance score)</option>
                <option value="SYNTHESIZE">SYNTHESIZE (Synthesize findings)</option>
              </select>
            </div>

            <div class="form-group" id="exp1-doc-group">
              <label class="form-label" for="exp1-doc-select">Document (SUMMARIZE)</label>
              <select id="exp1-doc-select" class="form-control"></select>
            </div>
          </div>

          <div style="display: flex; justify-content: flex-end; margin-top: 8px;">
            <button type="submit" id="exp1-invoke-btn" class="btn btn-primary">
              <span>Execute RMI Call</span>
            </button>
          </div>
        </form>

        <div id="exp1-result-container" style="margin-top: 20px; display: none;">
          <h3 style="font-size: 13px; font-weight: 600; text-transform: uppercase; color: var(--text-dim); margin-bottom: 8px;">
            Invocation Results
          </h3>
          <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 8px; padding: 14px;">
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 12px;">
              <div class="stat-item">
                <span class="stat-label">Agent ID</span>
                <span class="stat-val text-cyan" id="exp1-res-agent">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Latency</span>
                <span class="stat-val" id="exp1-res-latency">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Lamport Sent</span>
                <span class="stat-val" id="exp1-res-lamport-sent">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Lamport Received</span>
                <span class="stat-val" id="exp1-res-lamport-rec">-</span>
              </div>
            </div>
            <div>
              <span class="stat-label">Result Payload</span>
              <div id="exp1-res-payload" class="mono-cell" style="background: rgba(255,255,255,0.04); padding: 8px 12px; border-radius: 6px; margin-top: 4px; color: var(--text-main);"></div>
            </div>
          </div>
        </div>

        <div style="margin-top: 24px;">
          <h3 style="font-size: 13px; font-weight: 600; text-transform: uppercase; color: var(--text-dim); margin-bottom: 8px;">
            Active Node Registry Bindings
          </h3>
          <div id="exp1-bindings-container" style="display: flex; flex-wrap: wrap; gap: 8px;">
            <!-- Rendered based on selected node -->
          </div>
        </div>
      </div>
    `;

    const form = document.getElementById('exp1-form');
    const nodeSelect = document.getElementById('exp1-node-select');
    const typeSelect = document.getElementById('exp1-type-select');
    const invokeBtn = document.getElementById('exp1-invoke-btn');
    const docSelect = document.getElementById('exp1-doc-select');
    const docGroup = document.getElementById('exp1-doc-group');
    const AG = window.AgentGrid;
    const panel = this;

    // Node options come from the membership; they are rebuilt in place when it changes.
    const nodeLabel = (id, n) => 'Node ' + id + (n ? ' (port :' + n.port + ')' + (n.up ? '' : ' [OFFLINE]') : '');
    AG.fillNodeSelect(nodeSelect, nodeLabel);

    // SUMMARIZE summarises one corpus document: pick it here (default: the first one).
    AG.getCorpus().then(docs => {
      docSelect.replaceChildren(...docs.map(d => AG.h('option', { value: d.id }, d.id + ' — ' + d.title)));
    }).catch(err => {
      docSelect.replaceChildren(AG.h('option', { value: '' }, 'corpus unavailable: ' + err.message));
    });
    const syncDocVisibility = () => { docGroup.style.visibility = typeSelect.value === 'SUMMARIZE' ? 'visible' : 'hidden'; };
    typeSelect.addEventListener('change', syncDocVisibility);
    syncDocVisibility();

    panel._onMembership = () => {
      AG.fillNodeSelect(nodeSelect, nodeLabel);
      updateBindingsView();
    };
    if (!panel._membershipSubscribed) {
      panel._membershipSubscribed = true;
      AG.onMembershipChange(m => {
        if (document.getElementById('exp1-node-select') && panel._onMembership) panel._onMembership(m);
      });
    }

    function updateBindingsView() {
      const selectedId = parseInt(nodeSelect.value, 10);
      const node = window.AgentGrid.getState().nodes.find(n => n.id === selectedId);
      const container = document.getElementById('exp1-bindings-container');
      if (!container) return;
      // Binding names come from the node's registry: rendered as text.
      if (!node || !node.bindings || node.bindings.length === 0) {
        container.replaceChildren(AG.h('span', { style: { fontSize: '12px', color: 'var(--text-dim)' } },
          'No active bindings (Node may be offline)'));
      } else {
        container.replaceChildren(...node.bindings.map(b => AG.h('span', { className: 'badge', style: {
          fontFamily: 'var(--font-mono)', background: 'rgba(99, 102, 241, 0.15)', color: '#818cf8',
          borderColor: 'rgba(99, 102, 241, 0.3)' } }, '"' + b + '"')));
      }
    }

    nodeSelect.addEventListener('change', updateBindingsView);
    updateBindingsView();

    // Cluster updates keep the bindings and the [OFFLINE] labels fresh (subscribed once).
    panel._onCluster = () => {
      AG.fillNodeSelect(nodeSelect, nodeLabel);
      updateBindingsView();
    };
    if (!panel._clusterSubscribed) {
      panel._clusterSubscribed = true;
      AG.onClusterUpdate(() => {
        if (document.getElementById('exp1-node-select') && panel._onCluster) panel._onCluster();
      });
    }

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const nodeId = parseInt(nodeSelect.value, 10);
      const type = typeSelect.value;

      invokeBtn.disabled = true;
      invokeBtn.innerHTML = `<span>Invoking...</span>`;

      try {
        const resp = await window.AgentGrid.invokeRmi(nodeId, type, 1, type === 'SUMMARIZE' ? docSelect.value : null);
        const call = resp.calls && resp.calls[0];
        if (call) {
          document.getElementById('exp1-res-agent').textContent = call.agentId;
          document.getElementById('exp1-res-latency').textContent = `${call.latencyMs} ms`;
          document.getElementById('exp1-res-lamport-sent').textContent = `L=${call.lamportSent}`;
          document.getElementById('exp1-res-lamport-rec').textContent = `L=${call.lamportReceived}`;
          document.getElementById('exp1-res-payload').textContent = call.payload;
          document.getElementById('exp1-result-container').style.display = 'block';
        }
      } catch (err) {
        alert('RMI Invocation error: ' + err.message);
      } finally {
        invokeBtn.disabled = false;
        invokeBtn.innerHTML = `<span>Execute RMI Call</span>`;
      }
    });
  }
};
