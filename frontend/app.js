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

  // Each node card is created once (keyed by node id) and then updated in place: cluster
  // snapshots arrive every 200 ms to 1 s, and rebuilding the cards destroyed the Kill/Restart
  // button between mousedown and mouseup, so clicks were lost.
  const cards = new Map();
  const PENDING_TIMEOUT_MS = 10000;

  function renderClusterGrid(nodes) {
    const grid = document.getElementById('cluster-grid');
    const badge = document.getElementById('online-count-badge');

    const onlineCount = nodes.filter(n => n.up).length;
    badge.textContent = `${onlineCount} / ${nodes.length} Online`;
    badge.className = `badge ${onlineCount === nodes.length ? 'online' : (onlineCount > 0 ? '' : 'offline')}`;

    nodes.forEach(n => {
      let c = cards.get(n.id);
      if (!c) {
        c = createCard(n.id);
        cards.set(n.id, c);
        grid.appendChild(c.root);
      }
      c.node = n;
      const isBackup = !!(n.up && state.election && state.election.backupId === n.id);
      const key = JSON.stringify([n.up, n.isLeader, isBackup, n.port, n.poolSize, n.queueDepth,
        n.clockOffsetMs, n.lamport, n.leaderView]);
      if (key !== c.key) {
        c.key = key;
        updateCard(c, n, isBackup);
      }
      settlePending(c);
    });
  }

  function createCard(id) {
    const h = window.AgentGrid.h;
    const c = { id, key: null, node: null, pending: null };
    c.title = h('span', { className: 'node-title' }, 'Node ' + id);
    c.port = h('span', { className: 'node-port' });
    c.leaderBadge = h('span', { className: 'badge leader', style: { display: 'none' } }, 'LEADER');
    c.backupBadge = h('span', { className: 'badge backup', style: { display: 'none' } }, 'BACKUP');
    c.statusBadge = h('span', { className: 'badge' });
    c.poolText = h('span');
    c.poolPct = h('span', { className: 'mono-cell' });
    c.poolFill = h('div', { className: 'pool-progress-fill' });
    c.clock = h('span', { className: 'stat-val' });
    c.lamport = h('span', { className: 'stat-val' });
    c.leaderView = h('span', { className: 'stat-val' });
    c.killBtn = h('button', { className: 'btn btn-sm btn-danger', type: 'button' }, 'Kill');
    c.restartBtn = h('button', { className: 'btn btn-sm btn-warning', type: 'button' }, 'Restart');
    c.note = h('div', { className: 'node-card-note' });
    const stat = (label, val) => h('div', { className: 'stat-item' }, [h('span', { className: 'stat-label' }, label), val]);
    c.root = h('div', { className: 'node-card', id: 'node-card-' + id }, [
      h('div', { className: 'node-card-header' }, [
        h('div', { className: 'node-title-wrap' }, [c.title, c.port]),
        h('div', { className: 'node-badges' }, [c.leaderBadge, c.backupBadge, c.statusBadge])
      ]),
      h('div', { className: 'pool-metric' }, [
        h('div', { className: 'pool-label-row' }, [c.poolText, c.poolPct]),
        h('div', { className: 'pool-progress-bar' }, c.poolFill)
      ]),
      h('div', { className: 'node-stats-row' }, [
        stat('clock vs control plane', c.clock), stat('Lamport', c.lamport), stat('leader view', c.leaderView)
      ]),
      h('div', { className: 'node-card-actions' }, [c.killBtn, c.restartBtn]),
      c.note
    ]);
    // Handlers are attached once, here; the buttons are never recreated.
    c.killBtn.addEventListener('click', () => nodeAction(c, 'kill'));
    c.restartBtn.addEventListener('click', () => nodeAction(c, 'restart'));
    return c;
  }

  function updateCard(c, n, isBackup) {
    const pool = n.poolSize || 1;
    const depth = n.queueDepth === null || n.queueDepth === undefined ? 0 : n.queueDepth;
    const pct = Math.min(100, Math.round((depth / pool) * 100));
    c.root.classList.toggle('offline', !n.up);
    c.root.classList.toggle('is-leader', !!n.isLeader);
    c.port.textContent = 'port :' + n.port;
    c.leaderBadge.style.display = n.isLeader ? '' : 'none';
    c.backupBadge.style.display = isBackup ? '' : 'none';
    c.statusBadge.className = 'badge ' + (n.up ? 'online' : 'offline');
    c.statusBadge.textContent = n.up ? 'ONLINE' : 'OFFLINE';
    c.poolText.textContent = `Pool: ${Math.min(depth, pool)} running, ${Math.max(0, depth - pool)} queued`;
    c.poolPct.textContent = pct + '%';
    c.poolFill.style.width = pct + '%';
    c.poolFill.classList.toggle('hot', depth > pool);
    const off = n.clockOffsetMs;
    c.clock.textContent = off === null || off === undefined ? '—' : (off >= 0 ? '+' : '') + off + ' ms';
    c.clock.classList.toggle('text-cyan', off !== 0 && off !== null && off !== undefined);
    c.lamport.textContent = n.lamport === null || n.lamport === undefined ? '—' : String(n.lamport);
    c.leaderView.textContent = n.leaderView === null || n.leaderView === undefined ? '—' : 'Node ' + n.leaderView;
    if (!c.pending) setButtons(c);
  }

  function setButtons(c) {
    c.killBtn.disabled = !(c.node && c.node.up);
    c.restartBtn.disabled = false;
    c.killBtn.textContent = 'Kill';
    c.restartBtn.textContent = 'Restart';
  }

  /**
   * Kill/Restart: both buttons of this node are disabled and labelled at once; repeat clicks are
   * ignored until the API call has returned AND a later snapshot shows the new state (DOWN
   * after a kill, UP after a restart), or 10 s have passed.
   */
  async function nodeAction(c, action) {
    if (c.pending) return;
    const p = { action, returned: false, timer: null };
    c.pending = p;
    c.note.textContent = '';
    c.killBtn.disabled = true;
    c.restartBtn.disabled = true;
    (action === 'kill' ? c.killBtn : c.restartBtn).textContent = action === 'kill' ? 'Killing...' : 'Restarting...';
    p.timer = setTimeout(() => {
      if (c.pending === p) finishPending(c, 'No ' + (action === 'kill' ? 'DOWN' : 'UP') + ' state seen within 10 s.');
    }, PENDING_TIMEOUT_MS);
    try {
      const res = await fetch(`/api/nodes/${c.id}/${action}`, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'HTTP ' + res.status }));
        throw new Error(err.error || ('HTTP ' + res.status));
      }
      if (c.pending === p) p.returned = true;   // settled by the next snapshot showing the new state
    } catch (err) {
      if (c.pending === p) finishPending(c, (action === 'kill' ? 'Kill' : 'Restart') + ' failed: ' + err.message);
    }
  }

  function settlePending(c) {
    const p = c.pending;
    if (!p || !p.returned || !c.node) return;
    if ((p.action === 'kill' && !c.node.up) || (p.action === 'restart' && c.node.up)) {
      finishPending(c, '');
    }
  }

  function finishPending(c, message) {
    if (c.pending && c.pending.timer) clearTimeout(c.pending.timer);
    c.pending = null;
    c.note.textContent = message;   // textContent only
    setButtons(c);
  }

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
