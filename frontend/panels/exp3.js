// Experiment 3 Panel: Physical Clock Synchronization (Berkeley Algorithm) & Drift

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp3 = {
  render: function(container) {
    container.innerHTML = `
      <div class="panel-inner">
        <div style="margin-bottom: 16px;">
          <h2 style="font-size: 16px; font-weight: 700;">Experiment 3: Physical Clock Drift & Berkeley Sync</h2>
          <p style="font-size: 13px; color: var(--text-muted);">
            Manipulate artificial hardware clock skew across nodes and trigger round-trip compensated Berkeley algorithm synchronization to converge node physical clocks. The coordinator is the elected leader node, which is included in the average. Berkeley converges the nodes to their mean offset, not to real time, which is what LWW on the blackboard needs: the nodes agree with each other.
          </p>
        </div>

        <!-- Section 1: Set Clock Drift -->
        <div style="background: rgba(0,0,0,0.25); border: 1px solid var(--border-color); border-radius: 10px; padding: 16px; margin-bottom: 20px;">
          <h3 style="font-size: 13px; font-weight: 600; text-transform: uppercase; color: var(--text-dim); margin-bottom: 12px;">
            Inject / Adjust Physical Clock Drift
          </h3>
          <form id="exp3-drift-form" style="display: grid; grid-template-columns: 1fr 1fr auto; gap: 12px; align-items: flex-end;">
            <div class="form-group" style="margin-bottom: 0;">
              <label class="form-label" for="exp3-drift-node">Target Node</label>
              <select id="exp3-drift-node" class="form-control"></select>
            </div>

            <div class="form-group" style="margin-bottom: 0;">
              <label class="form-label" for="exp3-delta-input">Delta Offset (ms)</label>
              <input type="number" id="exp3-delta-input" class="form-control" value="5000" step="100">
            </div>

            <button type="submit" id="exp3-apply-drift-btn" class="btn btn-primary" style="height: 38px;">
              Apply Drift
            </button>
          </form>
        </div>

        <!-- Section 2: Berkeley Synchronization Round -->
        <div style="background: rgba(0,0,0,0.25); border: 1px solid var(--border-color); border-radius: 10px; padding: 16px;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            <div>
              <h3 style="font-size: 13px; font-weight: 600; text-transform: uppercase; color: var(--text-dim);">
                Berkeley Round on the Leader Node
              </h3>
              <span style="font-size: 12px; color: var(--text-muted);">
                Asks the elected leader to poll every live node, compensate each reading for RTT, include itself in the average, and send each node its correction. Unreachable nodes are skipped. Returns 409 while no leader is agreed.
              </span>
            </div>
            <button id="exp3-sync-btn" class="btn btn-primary" style="background: linear-gradient(135deg, var(--accent-emerald), #059669);">
              Sync Now (Berkeley Round)
            </button>
          </div>

          <div id="exp3-sync-result-box" style="display: none; background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 8px; padding: 14px; margin-top: 14px;">
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;">
              <div class="stat-item">
                <span class="stat-label">Max Spread Before</span>
                <span class="stat-val" style="color: var(--accent-rose);" id="exp3-res-before">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Max Spread After</span>
                <span class="stat-val" style="color: var(--accent-emerald);" id="exp3-res-after">-</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">Spread Reduction</span>
                <span class="stat-val text-cyan" id="exp3-res-reduction">-</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;

    const driftForm = document.getElementById('exp3-drift-form');
    const driftNodeSelect = document.getElementById('exp3-drift-node');
    const deltaInput = document.getElementById('exp3-delta-input');
    const applyDriftBtn = document.getElementById('exp3-apply-drift-btn');
    const syncBtn = document.getElementById('exp3-sync-btn');
    const AG = window.AgentGrid;
    const panel = this;

    // Node options follow the membership (rebuilt in place, selection kept); the label shows the
    // offset from the control-plane clock, which Berkeley converges to the nodes' mean, not real time.
    const nodeLabel = (id, n) => 'Node ' + id + (n && n.clockOffsetMs != null
      ? ' (offset vs control plane: ' + (n.clockOffsetMs >= 0 ? '+' : '') + n.clockOffsetMs + ' ms)' : '');
    AG.fillNodeSelect(driftNodeSelect, nodeLabel);
    panel._onMembership = () => AG.fillNodeSelect(driftNodeSelect, nodeLabel);
    if (!panel._membershipSubscribed) {
      panel._membershipSubscribed = true;
      AG.onMembershipChange(m => {
        if (document.getElementById('exp3-drift-node') && panel._onMembership) panel._onMembership(m);
      });
    }

    driftForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const nodeId = parseInt(driftNodeSelect.value, 10);
      const deltaMs = parseInt(deltaInput.value, 10);

      applyDriftBtn.disabled = true;
      try {
        await window.AgentGrid.adjustDrift(nodeId, deltaMs);
      } catch (err) {
        alert('Failed to set drift: ' + err.message);
      } finally {
        applyDriftBtn.disabled = false;
      }
    });

    syncBtn.addEventListener('click', async () => {
      syncBtn.disabled = true;
      syncBtn.textContent = 'Synchronizing...';

      try {
        const res = await window.AgentGrid.syncClocks();
        document.getElementById('exp3-res-before').textContent = `${res.spreadBefore} ms`;
        document.getElementById('exp3-res-after').textContent = `${res.spreadAfter} ms`;
        const diff = res.spreadBefore - res.spreadAfter;
        document.getElementById('exp3-res-reduction').textContent = `${diff} ms`;

        document.getElementById('exp3-sync-result-box').style.display = 'block';

      } catch (err) {
        alert('Sync error: ' + err.message);
      } finally {
        syncBtn.disabled = false;
        syncBtn.textContent = 'Sync Now (Berkeley Round)';
      }
    });
  }
};
