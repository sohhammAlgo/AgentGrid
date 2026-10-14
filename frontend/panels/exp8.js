// Experiment 8 Panel: primary-backup replication of the orchestrator's job state.
// The elected leader is the primary; the live member with the next highest id is the backup and gets
// every job state change before the job moves on. Everything shown comes from GET /api/replication
// (polled while the panel is open) and from the replication events of the SSE stream.
// Built with text nodes (AgentGrid.h), never innerHTML.

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp8 = {
  render: function(container) {
    const AG = window.AgentGrid;
    const h = (...a) => AG.h(...a);

    // Event types the log shows: replication itself, and what it does to jobs.
    const LOG_TYPES = new Set(['REPLICATION_PROMOTED', 'REPLICATION_BACKUP_SET', 'REPLICATION_BACKUP_LOST',
      'REPLICATION_NO_BACKUP', 'REPLICATION_SYNCED', 'REPLICATION_CATCH_UP', 'REPLICATION_STEPPED_DOWN',
      'REPLICATION_STALE_REJECTED', 'REPLICATION_TOGGLED', 'JOB_RECOVERING', 'JOB_HANDED_OVER', 'JOB_ORPHANED',
      'NODE_KILLED', 'NODE_RESTARTED']);
    const MAX_LOG_ROWS = 60;
    let timer = null;
    // Every timer and listener below is tied to this root: once a tab switch replaces it, they stop.
    const root = h('div', { className: 'panel-inner', id: 'exp8-root' });

    const toggle = h('input', { type: 'checkbox', id: 'exp8-toggle', disabled: true });
    const toggleLabel = h('span', { id: 'exp8-toggle-label' }, 'Primary-backup replication');
    const toggleState = h('span', { className: 'badge', id: 'exp8-toggle-state' }, '—');
    const toggleNote = h('span', { className: 'job-stage-meta', id: 'exp8-toggle-note' });
    const warning = h('div', { className: 'repl-warning', id: 'exp8-warning' });

    const primaryCard = card('Primary', 'is-primary');
    const backupCard = card('Backup', 'is-backup');
    const members = h('div', { className: 'repl-members', id: 'exp8-members' });

    const tTerm = tile('Term');
    const tPrimarySeq = tile('Primary last seq');
    const tBackupSeq = tile('Backup last seq');
    const tLag = tile('Lag (events)');
    const tStale = tile('Stale writes refused');
    const tRecovered = tile('Jobs recovered');

    const logBody = h('tbody', { id: 'exp8-log-body' },
      h('tr', { className: 'empty-row' }, h('td', { colSpan: 3 }, 'No replication events yet.')));

    root.replaceChildren(...[
      h('div', { style: { marginBottom: '16px' } }, [
        h('h2', { style: { fontSize: '16px', fontWeight: '700' } }, 'Experiment 8: Primary-Backup Replication'),
        h('p', { style: { fontSize: '14px', color: 'var(--text-muted)' } },
          'The leader runs jobs as the primary and sends every job state change (submit, dispatch, result, done) to the '
          + 'backup, the live node with the next highest id, waiting for its ack. If the primary dies, the new leader '
          + 'replays its copy of the log and resumes the job: it ends RECOVERED instead of ORPHANED. Every event and dispatch '
          + 'carries a term; a lower term is refused, so a replaced primary cannot write.')
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Setting'),
        h('div', { className: 'exp4-row' }, [
          h('label', { className: 'repl-switch', htmlFor: 'exp8-toggle' }, [toggle, h('span', { className: 'repl-track' }), toggleLabel]),
          toggleState,
          toggleNote
        ]),
        warning
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Roles'),
        h('div', { className: 'repl-grid' }, [primaryCard.root, backupCard.root]),
        members
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Log position'),
        h('div', { className: 'exp4-stats', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))' } },
          [tTerm.root, tPrimarySeq.root, tBackupSeq.root, tLag.root, tStale.root, tRecovered.root])
      ]),
      h('div', { className: 'exp4-box', style: { marginBottom: '0' } }, [
        h('h3', {}, 'Replication events (live)'),
        h('p', { className: 'job-stage-meta', style: { marginBottom: '8px' } },
          'To see it: submit a job in the Job Console, then Kill the primary node while it runs. Switch replication off '
          + 'and repeat to see ORPHANED instead.'),
        h('div', { className: 'repl-log' }, h('table', { className: 'events-table' }, [
          h('thead', {}, h('tr', {}, [h('th', { style: { width: '170px' } }, 'Type'), h('th', { style: { width: '64px' } }, 'Node'),
            h('th', {}, 'Details')])),
          logBody
        ]))
      ])
    ]);
    container.replaceChildren(root);

    // ---- small builders --------------------------------------------------------------------
    function tile(label) {
      const val = h('span', { className: 'stat-val' }, '—');
      return { val, root: h('div', { className: 'stat-item' }, [h('span', { className: 'stat-label' }, label), val]) };
    }

    function card(role, cls) {
      const title = h('span', {}, role + ': —');
      const badge = h('span', { className: 'badge' }, '—');
      const term = tile('Term');
      const seq = tile('Last seq');
      const events = tile('Log events');
      return {
        title, badge, term, seq, events,
        root: h('div', { className: 'repl-card ' + cls + ' is-empty' }, [
          h('div', { className: 'repl-card-head' }, [title, badge]),
          h('div', { className: 'repl-stats' }, [term.root, seq.root, events.root])
        ])
      };
    }

    function setCard(c, role, node, entry, empty) {
      c.root.classList.toggle('is-empty', !!empty);
      c.title.textContent = empty ? role + ': none' : role + ': Node ' + node;
      c.badge.className = 'badge ' + (empty ? '' : role === 'Primary' ? 'leader' : 'backup');
      c.badge.textContent = empty ? '—' : role.toUpperCase();
      c.term.val.textContent = entry && entry.term != null ? String(entry.term) : '—';
      c.seq.val.textContent = entry && entry.lastSeq != null ? String(entry.lastSeq) : '—';
      c.events.val.textContent = entry && entry.logEvents != null ? String(entry.logEvents) : '—';
    }

    // ---- state -----------------------------------------------------------------------------
    let pending = false;

    function renderView(v) {
      const on = !!v.enabled;
      if (!pending) {
        toggle.checked = on;
        toggle.disabled = false;
      }
      toggleState.textContent = on ? 'ON' : 'OFF';
      toggleState.className = 'badge ' + (on ? 'online' : 'offline');
      toggleNote.textContent = on
        ? 'a job whose leader dies is resumed by the backup (RECOVERED)'
        : 'a job whose leader dies is ORPHANED, as before replication';

      warning.textContent = v.warning || (on && v.primary != null ? 'In sync: the backup holds every event the primary has logged.' : '');
      warning.classList.toggle('ok', !v.warning && on && v.primary != null);

      const byNode = new Map((v.nodes || []).map(n => [n.node, n]));
      setCard(primaryCard, 'Primary', v.primary, byNode.get(v.primary), v.primary == null);
      setCard(backupCard, 'Backup', v.backup, byNode.get(v.backup), v.backup == null);

      tTerm.val.textContent = v.term == null ? '—' : String(v.term);
      tPrimarySeq.val.textContent = v.lastSeq == null ? '—' : String(v.lastSeq);
      tBackupSeq.val.textContent = v.backupLastSeq == null ? '—' : String(v.backupLastSeq);
      tLag.val.textContent = v.lag == null ? '—' : String(v.lag);
      tLag.val.classList.toggle('text-rose', v.lag != null && v.lag > 0);
      tStale.val.textContent = String(v.staleRejected == null ? 0 : v.staleRejected);
      tRecovered.val.textContent = String(v.recoveredJobs == null ? 0 : v.recoveredJobs);

      members.replaceChildren(...(v.nodes || []).map(n => h('span', {
        className: 'badge ' + (n.role === 'PRIMARY' ? 'leader' : n.role === 'BACKUP' ? 'backup' : n.up ? '' : 'offline'),
        title: 'term ' + (n.term == null ? '—' : n.term) + ', last seq ' + (n.lastSeq == null ? '—' : n.lastSeq)
      }, 'Node ' + n.node + ' · ' + n.role + (n.term == null ? '' : ' · t' + n.term + ' · seq ' + n.lastSeq))));
    }

    async function refresh() {
      if (!root.isConnected) {
        clearInterval(timer);
        return;
      }
      try {
        renderView(await AG.getReplication());
      } catch (err) {
        warning.classList.remove('ok');
        warning.textContent = 'Replication state unavailable: ' + err.message;
      }
    }

    toggle.addEventListener('change', async () => {
      const want = toggle.checked;
      pending = true;
      toggle.disabled = true;
      toggleNote.textContent = 'switching ' + (want ? 'on' : 'off') + '...';
      try {
        renderView(await AG.setReplication(want));
      } catch (err) {
        toggle.checked = !want;
        toggleNote.textContent = 'could not switch: ' + err.message;   // server text, rendered as text
      } finally {
        pending = false;
        toggle.disabled = false;
      }
    });

    // ---- live log ----------------------------------------------------------------------------
    function addRow(ev) {
      if (!LOG_TYPES.has(ev.type)) return;
      if (logBody.querySelector('.empty-row')) logBody.replaceChildren();
      const badge = h('span', { className: 'type-badge type-' + String(ev.type).replace(/[^A-Z0-9_]/g, '') }, ev.type);
      const row = h('tr', {}, [
        h('td', {}, badge),
        h('td', { className: 'mono-cell' }, ev.node > 0 ? 'Node ' + ev.node : 'Cluster'),
        h('td', { style: { color: 'var(--text-main)', wordBreak: 'break-word' } }, ev.details || '')
      ]);
      logBody.insertBefore(row, logBody.firstChild);
      while (logBody.children.length > MAX_LOG_ROWS) logBody.lastChild.remove();
    }

    // Replication events recorded so far (oldest first, so the newest ends up on top).
    fetch('/api/events').then(r => r.ok ? r.json() : []).then(list => {
      list.filter(e => LOG_TYPES.has(e.type)).sort((a, b) => a.seq - b.seq).slice(-MAX_LOG_ROWS).forEach(addRow);
    }).catch(() => {});

    const seen = new Set();
    AG.onEvent(ev => {
      if (!root.isConnected) return;
      if (seen.has(ev.seq)) return;
      seen.add(ev.seq);
      addRow(ev);
      if (String(ev.type).startsWith('REPLICATION_') || ev.type === 'JOB_RECOVERING') refresh();
    });

    refresh();
    timer = setInterval(refresh, 1000);
  }
};
