// AgentGrid-Lite Frontend Controller

(function() {
  const state = {
    nodes: [],
    events: [],
    sortOrder: 'lamport', // 'lamport' | 'nodeWallMs'
    activeModule: null,
    modules: [],
    clusterListeners: []
  };

  window.AgentGrid = {
    getState: () => state,
    onClusterUpdate: (cb) => state.clusterListeners.push(cb),
    invokeRmi: async (nodeId, type, count) => {
      const res = await fetch('/api/rmi/invoke', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ node: nodeId, type, count })
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        throw new Error(err.error || 'Invocation failed');
      }
      return res.json();
    },
    adjustDrift: async (nodeId, deltaMs) => {
      const res = await fetch('/api/clock/drift', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ node: nodeId, deltaMs })
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        throw new Error(err.error || 'Drift update failed');
      }
      return res.json();
    },
    syncClocks: async () => {
      const res = await fetch('/api/clock/sync', {
        method: 'POST'
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        throw new Error(err.error || 'Clock sync failed');
      }
      return res.json();
    }
  };

  // Initialization
  document.addEventListener('DOMContentLoaded', () => {
    initSse();
    initEventControls();
    loadModules();
  });

  // =========================================================================
  // SSE & POLLING
  // =========================================================================

  function initSse() {
    const statusLabel = document.getElementById('connection-label');
    const statusDot = document.querySelector('.status-dot');

    if (window.EventSource) {
      const es = new EventSource('/api/stream');

      es.addEventListener('open', () => {
        statusLabel.textContent = 'Live SSE Connected';
        statusDot.className = 'status-dot connected pulse';
      });

      es.addEventListener('cluster', (e) => {
        try {
          const nodes = JSON.parse(e.data);
          updateCluster(nodes);
        } catch (err) {
          console.error('Failed to parse cluster SSE:', err);
        }
      });

      es.addEventListener('event', (e) => {
        try {
          const ev = JSON.parse(e.data);
          addEvent(ev);
        } catch (err) {
          console.error('Failed to parse event SSE:', err);
        }
      });

      es.addEventListener('error', () => {
        statusLabel.textContent = 'SSE Disconnected (Polling)';
        statusDot.className = 'status-dot';
      });
    }

    // Fallback polling loop
    setInterval(async () => {
      try {
        const res = await fetch('/api/cluster');
        if (res.ok) {
          const nodes = await res.json();
          updateCluster(nodes);
        }
      } catch (ignored) {}
    }, 2500);
  }

  function updateCluster(nodes) {
    state.nodes = nodes;
    renderClusterGrid(nodes);
    state.clusterListeners.forEach(cb => {
      try { cb(nodes); } catch (e) { console.error(e); }
    });
  }

  // =========================================================================
  // CLUSTER FLEET CARDS
  // =========================================================================

  function renderClusterGrid(nodes) {
    const grid = document.getElementById('cluster-grid');
    const badge = document.getElementById('online-count-badge');

    const onlineCount = nodes.filter(n => n.up).length;
    badge.textContent = `${onlineCount} / ${nodes.length} Online`;
    badge.className = `badge ${onlineCount === nodes.length ? 'online' : (onlineCount > 0 ? '' : 'offline')}`;

    grid.innerHTML = nodes.map(n => {
      const isUp = n.up;
      const pool = n.poolSize || 1;
      const depth = n.queueDepth || 0;
      const pct = Math.min(100, Math.round((depth / pool) * 100));
      const driftSign = n.clockOffsetMs >= 0 ? '+' : '';
      const isHot = depth > pool;

      return `
        <div class="node-card ${isUp ? '' : 'offline'}" id="node-card-${n.id}">
          <div class="node-card-header">
            <div class="node-title-wrap">
              <span class="node-title">Node ${n.id}</span>
              <span class="node-port">port :${n.port}</span>
            </div>
            <span class="badge ${isUp ? 'online' : 'offline'}">${isUp ? 'ONLINE' : 'OFFLINE'}</span>
          </div>

          <div class="pool-metric">
            <div class="pool-label-row">
              <span>Pool: ${depth} / ${pool} active</span>
              <span class="mono-cell">${pct}%</span>
            </div>
            <div class="pool-progress-bar">
              <div class="pool-progress-fill ${isHot ? 'hot' : ''}" style="width: ${pct}%"></div>
            </div>
          </div>

          <div class="node-stats-row">
            <div class="stat-item">
              <span class="stat-label">Clock Offset</span>
              <span class="stat-val ${n.clockOffsetMs === 0 ? '' : 'text-cyan'}">${driftSign}${n.clockOffsetMs} ms</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Lamport</span>
              <span class="stat-val">${n.lamport}</span>
            </div>
          </div>

          <div class="node-card-actions">
            <button class="btn btn-sm btn-danger" onclick="window.AgentGridActions.killNode(${n.id})" ${!isUp ? 'disabled' : ''}>
              Kill
            </button>
            <button class="btn btn-sm btn-warning" onclick="window.AgentGridActions.restartNode(${n.id})">
              Restart
            </button>
          </div>
        </div>
      `;
    }).join('');
  }

  window.AgentGridActions = {
    killNode: async (nodeId) => {
      try {
        await fetch(`/api/nodes/${nodeId}/kill`, { method: 'POST' });
      } catch (err) {
        alert('Failed to kill node ' + nodeId + ': ' + err.message);
      }
    },
    restartNode: async (nodeId) => {
      try {
        await fetch(`/api/nodes/${nodeId}/restart`, { method: 'POST' });
      } catch (err) {
        alert('Failed to restart node ' + nodeId + ': ' + err.message);
      }
    }
  };

  // =========================================================================
  // DYNAMIC MODULE TABS
  // =========================================================================

  async function loadModules() {
    try {
      const res = await fetch('/api/modules');
      if (!res.ok) throw new Error('Failed to load modules');
      const modules = await res.json();
      state.modules = modules;
      renderTabs(modules);

      if (modules.length > 0) {
        selectTab(modules[0].id);
      }
    } catch (err) {
      console.error(err);
      document.getElementById('module-content').innerHTML = `
        <div style="color: var(--accent-rose); padding: 20px;">
          Failed to load modules from /api/modules: ${err.message}
        </div>
      `;
    }
  }

  function renderTabs(modules) {
    const tabsContainer = document.getElementById('module-tabs');
    tabsContainer.innerHTML = modules.map(m => `
      <button class="tab-btn" id="tab-btn-${m.id}" onclick="window.AgentGridTabs.selectTab('${m.id}')">
        ${m.title}
      </button>
    `).join('');
  }

  async function selectTab(moduleId) {
    state.activeModule = moduleId;

    // Update active tab button style
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    const activeBtn = document.getElementById(`tab-btn-${moduleId}`);
    if (activeBtn) activeBtn.classList.add('active');

    const content = document.getElementById('module-content');
    content.innerHTML = `<div style="padding: 20px; color: var(--text-dim);">Loading panel for ${moduleId}...</div>`;

    // Dynamically load panels/<moduleId>.js if not loaded
    const scriptId = `panel-script-${moduleId}`;
    if (!document.getElementById(scriptId)) {
      const script = document.createElement('script');
      script.id = scriptId;
      script.src = `panels/${moduleId}.js`;
      script.onload = () => {
        if (window.AgentGridPanels && window.AgentGridPanels[moduleId]) {
          window.AgentGridPanels[moduleId].render(content);
        }
      };
      script.onerror = () => {
        content.innerHTML = `<div style="color: var(--accent-rose); padding: 20px;">Failed to load panels/${moduleId}.js</div>`;
      };
      document.body.appendChild(script);
    } else {
      if (window.AgentGridPanels && window.AgentGridPanels[moduleId]) {
        window.AgentGridPanels[moduleId].render(content);
      }
    }
  }

  window.AgentGridTabs = { selectTab };

  // =========================================================================
  // EVENT LOG & DUAL SORTING
  // =========================================================================

  function initEventControls() {
    const lamportBtn = document.getElementById('order-lamport-btn');
    const wallBtn = document.getElementById('order-wall-btn');
    const clearBtn = document.getElementById('clear-events-btn');

    lamportBtn.addEventListener('click', () => {
      state.sortOrder = 'lamport';
      lamportBtn.classList.add('active');
      wallBtn.classList.remove('active');
      renderEvents();
    });

    wallBtn.addEventListener('click', () => {
      state.sortOrder = 'nodeWallMs';
      wallBtn.classList.add('active');
      lamportBtn.classList.remove('active');
      renderEvents();
    });

    clearBtn.addEventListener('click', () => {
      state.events = [];
      renderEvents();
    });

    // Initial fetch of events
    fetch('/api/events')
      .then(r => r.json())
      .then(events => {
        state.events = events;
        renderEvents();
      })
      .catch(console.error);
  }

  function addEvent(ev) {
    // Avoid duplicate events by seq
    if (!state.events.some(e => e.seq === ev.seq)) {
      state.events.push(ev);
      if (state.events.length > 500) {
        state.events.shift();
      }
      renderEvents();
    }
  }

  function renderEvents() {
    const tbody = document.getElementById('events-tbody');
    const countBadge = document.getElementById('event-count-badge');
    countBadge.textContent = `${state.events.length} events`;

    if (state.events.length === 0) {
      tbody.innerHTML = `<tr class="empty-row"><td colspan="6">Awaiting cluster events...</td></tr>`;
      return;
    }

    // Sort according to active mode
    const sorted = [...state.events].sort((a, b) => {
      if (state.sortOrder === 'lamport') {
        return a.lamport - b.lamport || a.seq - b.seq;
      } else {
        return a.nodeWallMs - b.nodeWallMs || a.seq - b.seq;
      }
    });

    tbody.innerHTML = sorted.map(e => `
      <tr>
        <td class="mono-cell">${e.seq}</td>
        <td><span class="type-badge type-${e.type}">${e.type}</span></td>
        <td class="mono-cell">${e.node > 0 ? 'Node ' + e.node : 'Cluster'}</td>
        <td style="color: var(--text-main); word-break: break-all;">${escapeHtml(e.details)}</td>
        <td class="mono-cell" style="color: var(--accent-indigo); font-weight: 600;">L=${e.lamport}</td>
        <td class="mono-cell" style="color: var(--accent-cyan);">${formatWallTime(e.nodeWallMs)}</td>
      </tr>
    `).join('');
  }

  function formatWallTime(ms) {
    if (!ms || ms <= 0) return '-';
    const d = new Date(ms);
    const timeStr = d.toTimeString().split(' ')[0];
    const millis = String(d.getMilliseconds()).padStart(3, '0');
    return `${timeStr}.${millis}`;
  }

  function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

})();
