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
    electionListeners: [],
    // {epoch, size, quorum, members: [{id, port, poolSize, weight}], nextId, ...} from the
    // "membership" SSE event or GET /api/cluster?view=membership.
    membership: null,
    membershipListeners: [],
    // Called with every event from the SSE stream (e.g. MAPREDUCE_COMPLETE refreshes the index state).
    eventListeners: []
  };
  let corpusPromise = null;

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
    /** Called with the membership view whenever its epoch changes (a node was added or removed). */
    onMembershipChange: (cb) => state.membershipListeners.push(cb),
    getMembership: () => state.membership,
    /** Called with every new event from the SSE stream. */
    onEvent: (cb) => state.eventListeners.push(cb),
    /** Current member ids, ascending (falls back to the polled node list before the first view). */
    memberIds: () => state.membership ? state.membership.members.map(m => m.id) : state.nodes.map(n => n.id),
    /**
     * Fills a <select> with one option per current member, in place: options of removed members
     * are dropped, new ones inserted in id order, labels updated. The current selection is kept
     * if that node is still a member (otherwise the first member is selected).
     */
    fillNodeSelect: (select, label) => {
      const ids = window.AgentGrid.memberIds();
      const byId = new Map(state.nodes.map(n => [n.id, n]));
      const keep = select.value;
      [...select.options].forEach(o => { if (!ids.includes(parseInt(o.value, 10))) o.remove(); });
      ids.forEach((id, i) => {
        let opt = [...select.options].find(o => parseInt(o.value, 10) === id);
        if (!opt) {
          opt = document.createElement('option');
          opt.value = String(id);
          select.insertBefore(opt, select.options[i] || null);
        }
        const text = label ? label(id, byId.get(id)) : 'Node ' + id;
        if (opt.textContent !== text) opt.textContent = text;
      });
      if (keep && ids.includes(parseInt(keep, 10))) select.value = keep;
      else if (ids.length) select.value = String(ids[0]);
    },
    /** [{id, title}] of the bundled corpus documents (fetched once). */
    getCorpus: () => {
      if (!corpusPromise) {
        corpusPromise = window.AgentGrid.getJson('/api/corpus', 'Corpus lookup failed').catch(err => { corpusPromise = null; throw err; });
      }
      return corpusPromise;
    },
    addNode: (poolSize, weight) => postJson('/api/cluster/nodes', { poolSize, weight }, 'Add node failed'),
    removeNode: (nodeId) => postJson(`/api/cluster/nodes/${nodeId}/remove`, {}, 'Remove node failed'),
    setElectionAlgorithm: (name) => postJson('/api/election/algorithm', { name }, 'Algorithm change failed'),
    startElection: (nodeId) => postJson('/api/election/start', { node: nodeId }, 'Election start failed'),
    killNode: (nodeId) => postJson(`/api/nodes/${nodeId}/kill`, {}, 'Kill failed'),
    submitJob: (query, policy, consistency, retrieval) => {
      const body = { query, policy };
      if (consistency) body.consistency = consistency;
      if (retrieval) body.retrieval = retrieval;
      return postJson('/api/jobs', body, 'Job submission failed');
    },
    // Exp 7: MapReduce (PySpark in local mode on the control-plane host) and its inverted index.
    runMapReduce: (job, partitions, compare) =>
      postJson('/api/mapreduce/run', { job, partitions, compare }, 'MapReduce run failed'),
    mapReduceStatus: () => window.AgentGrid.getJson('/api/mapreduce/status', 'MapReduce status failed'),
    mapReduceIndex: () => window.AgentGrid.getJson('/api/mapreduce/index', 'Index lookup failed'),
    /** The last successful MapReduce result, or null if there is none (404). */
    mapReduceLast: async () => {
      const res = await fetch('/api/mapreduce/last');
      if (res.status === 404) return null;
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: 'Request failed' }));
        throw new Error(err.error || 'MapReduce result lookup failed');
      }
      return res.json();
    },
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
    invokeRmi: async (nodeId, type, count, doc) => {
      const res = await fetch('/api/rmi/invoke', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(doc ? { node: nodeId, type, count, doc } : { node: nodeId, type, count })
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
    initFleetControls();
    initSse();
    initEventControls();
    loadModules();
    refreshMembership();
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

      es.addEventListener('membership', (e) => {
        try {
          updateMembership(JSON.parse(e.data));
        } catch (err) {
          console.error('Failed to parse membership SSE:', err);
        }
      });

      es.addEventListener('event', (e) => {
        try {
          const ev = JSON.parse(e.data);
          addEvent(ev);
          if (ev.type === 'MEMBERSHIP_CHANGED') refreshMembership();
          state.eventListeners.forEach(cb => {
            try { cb(ev); } catch (listenerErr) { console.error('Event listener failed:', listenerErr); }
          });
        } catch (err) {
          console.error('Failed to parse event SSE:', err);
        }
      });

      es.addEventListener('error', () => {
        statusLabel.textContent = 'SSE Disconnected (Polling)';
        statusDot.className = 'status-dot';
      });
    }

    // Fallback polling loop: one request gives the membership and the node statuses.
    setInterval(async () => {
      try {
        const res = await fetch('/api/cluster?view=membership');
        if (res.ok) {
          const view = await res.json();
          updateMembership(view);
          updateCluster(view.nodes || []);
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

  async function refreshMembership() {
    try {
      const res = await fetch('/api/cluster?view=membership');
      if (res.ok) {
        const view = await res.json();
        updateMembership(view);
        updateCluster(view.nodes || []);
      }
    } catch (ignored) {}
  }

  /** Header (epoch / quorum) and, when the epoch changed, every membership listener. */
  function updateMembership(view) {
    if (!view || !Array.isArray(view.members)) return;
    const previous = state.membership;
    state.membership = view;
    const badge = document.getElementById('membership-badge');
    if (badge) {
      badge.textContent = `epoch ${view.epoch} · ${view.size} members · STRONG quorum ${view.quorum}`;
    }
    const addBtn = document.getElementById('add-node-btn');
    if (addBtn && !addBtn.dataset.pending) {
      addBtn.disabled = view.size >= view.maxMembers;
      addBtn.title = view.size >= view.maxMembers ? `the cluster already has the maximum of ${view.maxMembers} members` : '';
    }
    if (!previous || previous.epoch !== view.epoch) {
      state.membershipListeners.forEach(cb => {
        try { cb(view); } catch (e) { console.error(e); }
      });
    }
  }

  // =========================================================================
  // CLUSTER FLEET CARDS
  // =========================================================================

  // Each node card is created once (keyed by node id) and then updated in place: cluster
  // snapshots arrive every 200 ms to 1 s, and rebuilding the cards destroyed the Kill/Restart
  // button between mousedown and mouseup, so clicks were lost. The fleet follows the
  // membership: a card is added when a node joins and removed when it leaves.
  const cards = new Map();
  const PENDING_TIMEOUT_MS = 10000;
  const CONFIRM_MS = 3000;

  function renderClusterGrid(nodes) {
    const grid = document.getElementById('cluster-grid');
    const badge = document.getElementById('online-count-badge');

    const onlineCount = nodes.filter(n => n.up).length;
    badge.textContent = `${onlineCount} / ${nodes.length} Online`;
    badge.className = `badge ${onlineCount === nodes.length ? 'online' : (onlineCount > 0 ? '' : 'offline')}`;

    const ids = new Set(nodes.map(n => n.id));
    for (const [id, c] of cards) {
      if (!ids.has(id)) {          // no longer a member
        if (c.pending && c.pending.timer) clearTimeout(c.pending.timer);
        if (c.confirmTimer) clearTimeout(c.confirmTimer);
        c.root.remove();
        cards.delete(id);
      }
    }
    [...nodes].sort((a, b) => a.id - b.id).forEach(n => {
      let c = cards.get(n.id);
      if (!c) {
        c = createCard(n.id);
        cards.set(n.id, c);
        // keep the cards in id order
        const after = [...cards.values()].filter(o => o.id > n.id && o.root.parentNode === grid)
          .sort((a, b) => a.id - b.id)[0];
        grid.insertBefore(c.root, after ? after.root : null);
      }
      c.node = n;
      const isBackup = !!(n.up && state.election && state.election.backupId === n.id);
      const key = JSON.stringify([n.up, n.isLeader, isBackup, n.port, n.poolSize, n.weight, n.queueDepth,
        n.clockOffsetMs, n.lamport, n.leaderView, n.epoch]);
      if (key !== c.key) {
        c.key = key;
        updateCard(c, n, isBackup);
      }
      settlePending(c);
    });
  }

  function createCard(id) {
    const h = window.AgentGrid.h;
    const c = { id, key: null, node: null, pending: null, confirmTimer: null };
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
    c.removeBtn = h('button', { className: 'btn btn-sm btn-ghost', type: 'button',
      title: 'Remove this node from the cluster (a new membership epoch)' }, 'Remove');
    c.note = h('div', { className: 'node-card-note' });
    const stat = (label, val, title) => h('div', { className: 'stat-item', title: title || '' },
      [h('span', { className: 'stat-label' }, label), val]);
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
        stat('clock vs control plane', c.clock,
          'Offset from the control-plane clock; Berkeley converges nodes to their mean, not real time'),
        stat('Lamport', c.lamport), stat('leader view', c.leaderView)
      ]),
      h('div', { className: 'node-card-actions' }, [c.killBtn, c.restartBtn, c.removeBtn]),
      c.note
    ]);
    // Handlers are attached once, here; the buttons are never recreated.
    c.killBtn.addEventListener('click', () => nodeAction(c, 'kill'));
    c.restartBtn.addEventListener('click', () => nodeAction(c, 'restart'));
    c.removeBtn.addEventListener('click', () => removeClick(c));
    return c;
  }

  function updateCard(c, n, isBackup) {
    const pool = n.poolSize || 1;
    const depth = n.queueDepth === null || n.queueDepth === undefined ? 0 : n.queueDepth;
    const pct = Math.min(100, Math.round((depth / pool) * 100));
    c.root.classList.toggle('offline', !n.up);
    c.root.classList.toggle('is-leader', !!n.isLeader);
    c.port.textContent = 'port :' + n.port + ' · weight ' + (n.weight == null ? '—' : n.weight)
      + (n.up && n.epoch != null ? ' · epoch ' + n.epoch : '');
    c.leaderBadge.style.display = n.isLeader ? '' : 'none';
    c.backupBadge.style.display = isBackup ? '' : 'none';
    c.statusBadge.className = 'badge ' + (n.up ? 'online' : 'offline');
    c.statusBadge.textContent = n.up ? 'ONLINE' : 'OFFLINE';
    c.poolText.textContent = `Pool ${pool}: ${Math.min(depth, pool)} running, ${Math.max(0, depth - pool)} queued`;
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
    c.removeBtn.disabled = false;
    c.killBtn.textContent = 'Kill';
    c.restartBtn.textContent = 'Restart';
    if (!c.confirmTimer) {
      c.removeBtn.textContent = 'Remove';
      c.removeBtn.classList.remove('btn-danger');
      c.removeBtn.classList.add('btn-ghost');
    }
  }

  function disableAll(c) {
    c.killBtn.disabled = true;
    c.restartBtn.disabled = true;
    c.removeBtn.disabled = true;
  }

  /**
   * Kill/Restart: the node's buttons are disabled and labelled at once; repeat clicks are
   * ignored until the API call has returned AND a later snapshot shows the new state (DOWN
   * after a kill, UP after a restart), or 10 s have passed.
   */
  async function nodeAction(c, action) {
    if (c.pending) return;
    cancelConfirm(c);
    const p = { action, returned: false, timer: null };
    c.pending = p;
    c.note.textContent = '';
    disableAll(c);
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

  /**
   * Remove is two-step: the first click turns the button into "Confirm remove?" for 3 s; a
   * second click within that time sends the request. The server's 409/404 reason is shown on
   * the card (text only). On success the card disappears when the new membership arrives.
   */
  async function removeClick(c) {
    if (c.pending) return;
    if (!c.confirmTimer) {
      c.removeBtn.textContent = 'Confirm remove?';
      c.removeBtn.classList.remove('btn-ghost');
      c.removeBtn.classList.add('btn-danger');
      c.confirmTimer = setTimeout(() => cancelConfirm(c), CONFIRM_MS);
      return;
    }
    cancelConfirm(c);
    const p = { action: 'remove', returned: false, timer: null };
    c.pending = p;
    c.note.textContent = '';
    disableAll(c);
    c.removeBtn.textContent = 'Removing...';
    p.timer = setTimeout(() => {
      if (c.pending === p) finishPending(c, 'Node still a member 10 s after the remove request.');
    }, PENDING_TIMEOUT_MS);
    try {
      await window.AgentGrid.removeNode(c.id);
      if (c.pending === p) p.returned = true;   // the card goes away with the next membership
      refreshMembership();
    } catch (err) {
      if (c.pending === p) finishPending(c, 'Remove refused: ' + err.message);
    }
  }

  function cancelConfirm(c) {
    if (c.confirmTimer) {
      clearTimeout(c.confirmTimer);
      c.confirmTimer = null;
    }
    if (!c.pending) {
      c.removeBtn.textContent = 'Remove';
      c.removeBtn.classList.remove('btn-danger');
      c.removeBtn.classList.add('btn-ghost');
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

  /** "Add node": pool size and weight inputs (defaults 4 / 4), single-flight, server error shown as text. */
  function initFleetControls() {
    const box = document.getElementById('fleet-controls');
    if (!box) return;
    const h = window.AgentGrid.h;
    const pool = h('input', { type: 'number', className: 'form-control fleet-input', id: 'add-node-pool', min: 1, max: 32,
      value: 4, title: 'Worker pool size of the new node (1-32)' });
    const weight = h('input', { type: 'number', className: 'form-control fleet-input', id: 'add-node-weight', min: 1, max: 100,
      value: 4, title: 'WEIGHTED routing weight of the new node (1-100)' });
    const btn = h('button', { className: 'btn btn-sm btn-primary', type: 'button', id: 'add-node-btn' }, 'Add node');
    const status = h('span', { className: 'fleet-status', id: 'add-node-status' });
    box.replaceChildren(
      h('label', { className: 'fleet-label' }, ['pool ', pool]),
      h('label', { className: 'fleet-label' }, ['weight ', weight]),
      btn, status);
    btn.addEventListener('click', async () => {
      if (btn.dataset.pending) return;
      btn.dataset.pending = '1';
      btn.disabled = true;
      btn.textContent = 'Adding...';
      status.className = 'fleet-status';
      status.textContent = 'starting the node, pulling its snapshot, then bumping the epoch...';
      const timer = setTimeout(() => {
        if (btn.dataset.pending) status.textContent = 'still waiting for the server (adds can take up to ~25 s if the node is slow to start)...';
      }, PENDING_TIMEOUT_MS);
      try {
        const r = await window.AgentGrid.addNode(parseInt(pool.value, 10), parseInt(weight.value, 10));
        status.textContent = `node ${r.node} added (epoch ${r.membership.epoch}, quorum ${r.membership.quorum}) in ${r.totalMs} ms`;
        refreshMembership();
      } catch (err) {
        status.className = 'fleet-status text-rose';
        status.textContent = err.message;   // server text, rendered as text
      } finally {
        clearTimeout(timer);
        delete btn.dataset.pending;
        btn.textContent = 'Add node';
        const m = state.membership;
        btn.disabled = !!(m && m.size >= m.maxMembers);
      }
    });
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
