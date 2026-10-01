// Experiment 5 Panel: the replicated blackboard across the live nodes.
// Keys and values are user text: everything here is built with text nodes (AgentGrid.h),
// never with innerHTML. Every number comes from /api/blackboard, /api/blackboard/metrics,
// /api/blackboard/read and /api/clock/auto.

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp5 = {
  render: function(container) {
    const AG = window.AgentGrid;
    const h = (...a) => AG.h(...a);
    const panel = this;

    // Node ids come from the current membership (added / removed nodes included).
    const nodeIds = () => AG.memberIds();

    // ---- controls -------------------------------------------------------------------
    const autoToggle = h('input', { type: 'checkbox', id: 'exp5-auto' });
    const autoNote = h('span', { className: 'job-stage-meta' }, '');

    const wNode = h('select', { className: 'form-control' });
    AG.fillNodeSelect(wNode);
    // "needs a live quorum of Q of N members": follows the membership.
    const quorumText = h('span');
    const renderQuorum = () => {
      const m = AG.getMembership();
      quorumText.textContent = m
        ? 'a live quorum of floor(n/2)+1 = ' + m.quorum + ' of the ' + m.size + ' members (epoch ' + m.epoch + ')'
        : 'a live quorum of floor(n/2)+1 members';
    };
    renderQuorum();
    panel._onMembership = () => {
      AG.fillNodeSelect(wNode);
      renderQuorum();
    };
    if (!panel._membershipSubscribed) {
      panel._membershipSubscribed = true;
      AG.onMembershipChange(m => {
        if (document.getElementById('exp5-root') && panel._onMembership) panel._onMembership(m);
      });
    }
    // No maxLength here: the browser would silently cut a longer paste and store a different,
    // shorter key. The server enforces key <= 128 and value <= 1024 characters (400 otherwise)
    // and its message is shown below.
    const wKey = h('input', { className: 'form-control', value: 'demo/k1', placeholder: 'key (max 128)' });
    const wValue = h('input', { className: 'form-control', value: 'hello', placeholder: 'value (max 1024)' });
    const wMode = h('select', { className: 'form-control' }, [h('option', { value: 'STRONG' }, 'STRONG'), h('option', { value: 'EVENTUAL' }, 'EVENTUAL')]);
    const wButton = h('button', { className: 'btn btn-primary', type: 'button' }, 'Write');
    const wResult = h('div', { className: 'job-stage-meta', style: { marginTop: '8px' } }, '');

    const rKey = h('input', { className: 'form-control', value: 'demo/k1' });
    const rButton = h('button', { className: 'btn btn-primary', type: 'button' }, 'Read from every node');
    const rResult = h('div', {});

    const prefix = h('input', { className: 'form-control', maxLength: 128, value: '', placeholder: 'key prefix filter (empty = all)' });
    const hideJobs = h('input', { type: 'checkbox', checked: true });
    const metricsBox = h('div', { className: 'exp4-stats', style: { gridTemplateColumns: 'repeat(4, 1fr)' } });
    // The replica table's scroll container, head and body persist across the 2 s refreshes
    // (rebuilding them reset the scroll position); a refresh with unchanged data is skipped.
    const replicaNote = h('div', { className: 'job-stage-meta', style: { marginBottom: '6px' } });
    const replicaError = h('div', { className: 'text-rose' });
    const replicaHead = h('thead');
    const replicaBody = h('tbody');
    const replicaBox = h('div', {}, [replicaError, replicaNote,
      h('div', { className: 'exp4-table-wrap', style: { maxHeight: '420px' } },
        h('table', { className: 'events-table' }, [replicaHead, replicaBody]))]);
    let replicaSig = null;

    const box = (title, children) => h('div', { className: 'exp4-box' }, [h('h4', {}, title)].concat(children));

    container.replaceChildren(h('div', { className: 'panel-inner', id: 'exp5-root' }, [
      h('div', { style: { marginBottom: '16px' } }, [
        h('h3', { style: { fontSize: '16px', fontWeight: '700' } }, 'Experiment 5: Replicated Blackboard'),
        h('p', { style: { fontSize: '13px', color: 'var(--text-muted)' } }, [
          'STRONG writes replicate synchronously to every live replica and need ', quorumText,
          '; with fewer they are refused and nothing is written. '
          + 'EVENTUAL writes are acknowledged locally and pushed to peers after a simulated lag, with retries and periodic anti-entropy. '
          + 'Replicas resolve conflicts by last-writer-wins on the writer node\'s Berkeley-corrected clock (ties go to the higher node id). '
          + 'Replication is not fenced by membership epoch; anti-entropy covers gaps.'])
      ]),
      box('Clock sync', [h('label', { style: { display: 'flex', gap: '8px', alignItems: 'center', fontSize: '13px' } },
        [autoToggle, 'Auto Berkeley sync (periodic on the leader, and on node rejoin)', autoNote])]),
      box('Write', [h('div', { className: 'exp4-row' }, [wNode, wKey, wValue, wMode, wButton]), wResult]),
      box('Read', [h('div', { className: 'exp4-row' }, [rKey, rButton]), rResult]),
      box('Metrics', [metricsBox]),
      box('Replicas', [h('div', { className: 'exp4-row', style: { marginBottom: '10px' } }, [prefix,
        h('label', { style: { display: 'flex', gap: '6px', alignItems: 'center', fontSize: '12px' } }, [hideJobs, 'hide job/ keys'])]),
        replicaBox])
    ]));

    // ---- behaviour ------------------------------------------------------------------
    async function loadAuto() {
      try {
        const a = await AG.getClockAuto();
        autoToggle.checked = !!a.enabled;
        autoNote.textContent = a.berkeleyIntervalMs > 0 ? '(every ' + a.berkeleyIntervalMs + ' ms)' : '(periodic round disabled in config)';
      } catch (err) {
        autoNote.textContent = err.message;
      }
    }
    autoToggle.addEventListener('change', async () => {
      try {
        const r = await AG.setClockAuto(autoToggle.checked);
        autoNote.textContent = (r.enabled ? 'enabled' : 'disabled') + ' on nodes ' + JSON.stringify(r.applied);
      } catch (err) {
        autoNote.textContent = err.message;
        loadAuto();
      }
    });

    wButton.addEventListener('click', async () => {
      wButton.disabled = true;
      try {
        const o = await AG.bbWrite(parseInt(wNode.value, 10), wKey.value, wValue.value, wMode.value);
        wResult.replaceChildren(h('span', { className: 'job-badge ' + (o.stored ? 'job-COMPLETE' : 'job-FAILED') }, o.status),
          ' ' + o.mode + ' via node ' + o.node + ' in ' + o.latencyMs + ' ms'
          + (o.timestamp != null ? ', ts ' + o.timestamp + ' (writer ' + o.writer + ')' : '') + ' — ' + o.message);
        refresh();
      } catch (err) {
        wResult.replaceChildren(h('span', { className: 'text-rose' }, err.message));
      } finally {
        wButton.disabled = false;
      }
    });

    rButton.addEventListener('click', async () => {
      const key = rKey.value;
      const rows = [];
      for (const id of nodeIds()) {
        try {
          const r = await AG.bbRead(id, key);
          rows.push(h('tr', {}, [
            h('td', { className: 'mono-cell' }, 'Node ' + id),
            h('td', {}, h('span', { className: 'job-badge ' + (r.verdict === 'STALE' ? 'job-FAILED' : r.verdict === 'FRESH' ? 'job-COMPLETE' : 'job-QUEUED') }, r.verdict)),
            h('td', {}, r.found ? r.value : '(none)'),
            h('td', { className: 'mono-cell' }, r.timestamp == null ? '—' : String(r.timestamp)),
            h('td', { className: 'mono-cell' }, r.writer == null ? '—' : 'N' + r.writer)
          ]));
        } catch (err) {
          rows.push(h('tr', {}, [h('td', { className: 'mono-cell' }, 'Node ' + id), h('td', { colSpan: 4, className: 'text-rose' }, err.message)]));
        }
      }
      rResult.replaceChildren(h('table', { className: 'events-table' }, [
        h('thead', {}, h('tr', {}, ['Node', 'Verdict', 'Value', 'Timestamp', 'Writer'].map(t => h('th', {}, t)))),
        h('tbody', {}, rows)
      ]), h('div', { className: 'job-stage-meta' }, 'FRESH/STALE compare with the last value the control plane wrote and got acknowledged for this key; UNCHECKED: no such write (or an lww/ key); NOT_READY: the replica has not caught up since it started.'));
    });

    function stat(label, value) {
      return h('div', { className: 'stat-item' }, [h('span', { className: 'stat-label' }, label), h('span', { className: 'stat-val' }, value == null ? '—' : String(value))]);
    }

    async function refreshMetrics() {
      const m = await AG.bbMetrics();
      metricsBox.replaceChildren(
        stat('STRONG stored', m.strong.stored), stat('STRONG degraded', m.strong.storedDegraded),
        stat('STRONG refused', m.strong.refused), stat('STRONG failed partial', m.strong.failedPartial),
        stat('STRONG median write', m.strong.medianWriteLatencyMs == null ? null : m.strong.medianWriteLatencyMs + ' ms'),
        stat('EVENTUAL accepted', m.eventual.accepted), stat('EVENTUAL pending', m.eventual.pendingPropagation),
        stat('EVENTUAL median write', m.eventual.medianWriteLatencyMs == null ? null : m.eventual.medianWriteLatencyMs + ' ms'),
        stat('Stale reads', m.reads.stale), stat('Fresh reads', m.reads.fresh),
        stat('Not-ready reads', m.reads.notReady), stat('Anti-entropy applied', m.eventual.antiEntropyApplied)
      );
    }

    async function refreshReplicas() {
      const o = await AG.bbOverview(prefix.value);
      replicaError.textContent = '';
      const sig = JSON.stringify([o, prefix.value, hideJobs.checked]);
      if (sig === replicaSig) return;
      replicaSig = sig;
      const nodes = o.nodes || [];
      const keys = new Set();
      nodes.forEach(n => (n.entries || []).forEach(e => keys.add(e.key)));
      let keyList = [...keys].sort();
      if (hideJobs.checked) keyList = keyList.filter(k => !k.startsWith('job/'));
      const shown = keyList.slice(0, 150);
      const byNode = {};
      nodes.forEach(n => { byNode[n.node] = {}; (n.entries || []).forEach(e => { byNode[n.node][e.key] = e; }); });

      const head = h('tr', {}, [h('th', {}, 'Key')].concat(nodes.map(n => h('th', {},
        'N' + n.node + (!n.up ? ' (down)' : !n.reachable ? ' (unreachable)' : (n.ready ? '' : ' (not ready)')
          + (n.clockSynced === false ? ' · clock not synced' : '') + (n.pendingPropagation ? ' · pending ' + n.pendingPropagation : ''))))));
      const rows = shown.map(k => h('tr', {}, [h('td', { className: 'mono-cell' }, k)].concat(nodes.map(n => {
        const e = byNode[n.node][k];
        if (!n.up || !n.reachable) return h('td', { className: 'mono-cell job-stage-meta' }, '—');
        if (!e) return h('td', { className: 'mono-cell bb-missing', title: 'missing on this replica' }, 'missing');
        const shortValue = e.value.length > 60 ? e.value.substring(0, 60) + '…' : e.value;
        return h('td', { className: 'mono-cell' + (e.stale ? ' bb-stale' : ''), title: e.value + '\nts ' + e.timestamp + ', writer node ' + e.writer + (e.stale ? '\nSTALE: another replica holds a newer write' : '') },
          [h('div', {}, shortValue), h('div', { className: 'job-stage-meta' }, 'ts ' + e.timestamp + ' · N' + e.writer)]);
      }))));
      replicaNote.textContent = o.keys + ' keys in total' + (keyList.length > shown.length ? ', showing the first ' + shown.length : '')
        + (hideJobs.checked ? ' (job/ keys hidden)' : '') + '. EVENTUAL lag ' + o.eventualLagMs + ' ms (simulated). Stale cells are highlighted.';
      replicaHead.replaceChildren(head);
      replicaBody.replaceChildren(...rows);
    }

    async function refresh() {
      if (!document.getElementById('exp5-root')) return;
      try {
        await Promise.all([refreshMetrics(), refreshReplicas()]);
      } catch (err) {
        replicaError.textContent = err.message;
        replicaSig = null;
      }
    }

    prefix.addEventListener('change', refresh);
    hideJobs.addEventListener('change', refresh);
    loadAuto();
    refresh();
    if (panel._timer) clearInterval(panel._timer);
    panel._timer = setInterval(() => {
      if (document.getElementById('exp5-root')) refresh();
      else { clearInterval(panel._timer); panel._timer = null; }
    }, 2000);
  }
};
