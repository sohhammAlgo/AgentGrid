// AgentGrid-Lite Frontend Controller

(function() {
  const state = {
    nodes: [],
    events: [],
    sortOrder: 'lamport', // 'lamport' | 'nodeWallMs'
    activeModule: null,
    modules: [],
    clusterListeners: [],
    election: null,
    electionListeners: []
  };

  async function postJson(url, body, failMsg) {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {})
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Request failed' }));
      throw new Error(err.error || failMsg);
    }
    return res.json();
  }

  window.AgentGrid = {
    getState: () => state,
    onClusterUpdate: (cb) => state.clusterListeners.push(cb),
    onElectionUpdate: (cb) => state.electionListeners.push(cb),
    setElectionAlgorithm: (name) => postJson('/api/election/algorithm', { name }, 'Algorithm change failed'),
    startElection: (nodeId) => postJson('/api/election/start', { node: nodeId }, 'Election start failed'),
    killNode: (nodeId) => postJson(`/api/nodes/${nodeId}/kill`, {}, 'Kill failed'),
    submitJob: (query, policy, consistency) =>
      postJson('/api/jobs', consistency ? { query, policy, consistency } : { query, policy }, 'Job submission failed'),
    getJson: async (url, failMsg) => {
      const res = await fetch(url);
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        throw new Error(err.error || failMsg || 'Request failed');
      }
      return res.json();
    },
    bbOverview: (prefix) => window.AgentGrid.getJson('/api/blackboard?prefix=' + encodeURIComponent(prefix || ''), 'Blackboard lookup failed'),
    bbMetrics: () => window.AgentGrid.getJson('/api/blackboard/metrics', 'Metrics lookup failed'),
    bbRead: (node, key) => window.AgentGrid.getJson(`/api/blackboard/read?node=${encodeURIComponent(node)}&key=${encodeURIComponent(key)}`, 'Read failed'),
    bbWrite: (node, key, value, mode) => postJson('/api/blackboard/write', { node, key, value, mode }, 'Write failed'),
    getClockAuto: () => window.AgentGrid.getJson('/api/clock/auto', 'Auto-sync lookup failed'),
    setClockAuto: (enabled) => postJson('/api/clock/auto', { enabled }, 'Auto-sync change failed'),
    /**
     * Builds a DOM element: h('td', {className: 'x', title: '...'}, 'text' | node | [children]).
     * Text always goes in as a text node (textContent semantics), never as HTML.
     */
    h: (tag, props, children) => {
      const el = document.createElement(tag);
      Object.entries(props || {}).forEach(([k, v]) => {
        if (v === undefined || v === null) return;
        if (k === 'style') Object.assign(el.style, v);
        else if (k.startsWith('on')) el.addEventListener(k.substring(2), v);
        else if (k in el) el[k] = v;
        else el.setAttribute(k, v);
      });
      [].concat(children === undefined || children === null ? [] : children).forEach(c => {
        if (c === null || c === undefined || c === false) return;
        el.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
      });
      return el;
    },
    getJob: async (jobId) => {
      const res = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`);
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        throw new Error(err.error || 'Job lookup failed');
      }
      return res.json();
    },
    listJobs: async () => {
      const res = await fetch('/api/jobs');
      if (!res.ok) throw new Error('Job list failed');
      return res.json();
    },
    /** Polls a job until it leaves QUEUED/RUNNING; calls onUpdate with every snapshot. */
    waitForJob: async (jobId, onUpdate, intervalMs = 250) => {
      for (;;) {
        const job = await window.AgentGrid.getJob(jobId);
        if (onUpdate) onUpdate(job);
        if (job.status !== 'QUEUED' && job.status !== 'RUNNING') return job;
        await new Promise(r => setTimeout(r, intervalMs));
      }
    },
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
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
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

      es.addEventListener('election', (e) => {
        try {
          updateElection(JSON.parse(e.data));
        } catch (err) {
          console.error('Failed to parse election SSE:', err);
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
        if (state.modules.some(m => m.id === 'exp4')) {
          const er = await fetch('/api/election');
          if (er.ok) updateElection(await er.json());
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

  function updateElection(election) {
    state.election = election;
    renderClusterGrid(state.nodes);
    state.electionListeners.forEach(cb => {
      try { cb(election); } catch (e) { console.error(e); }
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
      const depth = n.queueDepth === null ? 0 : n.queueDepth;
      const running = Math.min(depth, pool);
      const queued = Math.max(0, depth - pool);
      const pct = Math.min(100, Math.round((depth / pool) * 100));
      const driftSign = (n.clockOffsetMs !== null && n.clockOffsetMs >= 0) ? '+' : '';
      const isHot = depth > pool;
      const isBackup = isUp && state.election && state.election.backupId === n.id;
      const roleBadges = (n.isLeader ? '<span class="badge leader">LEADER</span>' : '')
        + (isBackup ? '<span class="badge backup">BACKUP</span>' : '');

      return `
        <div class="node-card ${isUp ? '' : 'offline'} ${n.isLeader ? 'is-leader' : ''}" id="node-card-${n.id}">
          <div class="node-card-header">
            <div class="node-title-wrap">
              <span class="node-title">Node ${n.id}</span>
              <span class="node-port">port :${n.port}</span>
            </div>
            <div class="node-badges">
              ${roleBadges}
              <span class="badge ${isUp ? 'online' : 'offline'}">${isUp ? 'ONLINE' : 'OFFLINE'}</span>
            </div>
          </div>

          <div class="pool-metric">
            <div class="pool-label-row">
              <span>Pool: ${running} running, ${queued} queued</span>
              <span class="mono-cell">${pct}%</span>
            </div>
            <div class="pool-progress-bar">
              <div class="pool-progress-fill ${isHot ? 'hot' : ''}" style="width: ${pct}%"></div>
            </div>
          </div>

          <div class="node-stats-row">
            <div class="stat-item">
              <span class="stat-label">clock vs control plane</span>
              <span class="stat-val ${n.clockOffsetMs === 0 ? '' : 'text-cyan'}">${n.clockOffsetMs === null ? '—' : driftSign + n.clockOffsetMs + ' ms'}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Lamport</span>
              <span class="stat-val">${n.lamport === null ? '—' : n.lamport}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">leader view</span>
              <span class="stat-val">${n.leaderView === null || n.leaderView === undefined ? '—' : 'Node ' + n.leaderView}</span>
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
        await fetch(`/api/nodes/${nodeId}/kill`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
      } catch (err) {
        alert('Failed to kill node ' + nodeId + ': ' + err.message);
      }
    },
    restartNode: async (nodeId) => {
      try {
        await fetch(`/api/nodes/${nodeId}/restart`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
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

    // Event details can contain user text (blackboard keys and values, job queries), so every
    // cell is built with textContent; nothing from an event goes through innerHTML.
    const rows = sorted.map(e => {
      const tr = document.createElement('tr');
      tr.appendChild(cell(String(e.seq), 'mono-cell'));
      const typeCell = document.createElement('td');
      const badge = document.createElement('span');
      badge.className = 'type-badge type-' + String(e.type).replace(/[^A-Z0-9_]/g, '');
      badge.textContent = e.type;
      typeCell.appendChild(badge);
      tr.appendChild(typeCell);
      tr.appendChild(cell(e.node > 0 ? 'Node ' + e.node : 'Cluster', 'mono-cell'));
      const details = cell(e.details || '', '');
      details.style.color = 'var(--text-main)';
      details.style.wordBreak = 'break-all';
      tr.appendChild(details);
      const lamport = cell('L=' + e.lamport, 'mono-cell');
      lamport.style.color = 'var(--accent-indigo)';
      lamport.style.fontWeight = '600';
      tr.appendChild(lamport);
      const wall = cell(formatWallTime(e.nodeWallMs), 'mono-cell');
      wall.style.color = 'var(--accent-cyan)';
      tr.appendChild(wall);
      return tr;
    });
    tbody.replaceChildren(...rows);
  }

  function cell(text, className) {
    const td = document.createElement('td');
    if (className) td.className = className;
    td.textContent = text;
    return td;
  }

  function formatWallTime(ms) {
    if (!ms || ms <= 0) return '-';
    const d = new Date(ms);
    const timeStr = d.toTimeString().split(' ')[0];
    const millis = String(d.getMilliseconds()).padStart(3, '0');
    return `${timeStr}.${millis}`;
  }


})();
