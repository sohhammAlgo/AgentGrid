// Experiment 9 Panel: MPI-style collective communication (Broadcast, Scatter, Gather) between the node
// processes over plain TCP. The communicator is every current member sorted by node id (rank = index);
// the root defaults to the leader's rank. Everything shown comes from GET /api/collective (polled while
// the panel is open) and from the POST /api/collective/* results. Built with text nodes (AgentGrid.h),
// never innerHTML.

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp9 = {
  // Kept on the panel so switching tabs does not lose them.
  _last: {},        // 'broadcast' | 'scatter' | 'gather' | 'demo' -> last result
  _errors: {},      // same keys -> last error text
  _compare: {},     // 'LINEAR' | 'TREE' -> last broadcast with that algorithm
  _rootNode: '',    // chosen root as a node id ('' = the leader's rank)

  render: function(container) {
    const AG = window.AgentGrid;
    const h = (...a) => AG.h(...a);
    const panel = this;
    let view = null;
    let busy = false;
    let timer = null;

    // ---- small builders --------------------------------------------------------------------
    const meta = (text) => h('p', { className: 'job-stage-meta', style: { marginBottom: '10px' } }, text);
    const field = (label, id, control, grow) => h('div', { className: 'form-group', style: { marginBottom: '0', flex: grow ? '1 1 220px' : null } },
      [h('label', { className: 'form-label', htmlFor: id }, label), control]);
    // Errors go in a text-rose span (as in the other panels): .job-stage-meta would override its colour.
    const setStatus = (el, text, isError) => el.replaceChildren(isError ? h('span', { className: 'text-rose' }, text) : text);
    const fmt = (v) => v == null ? '—' : Number(v).toFixed(3);
    // Long arrays are shortened for display: the head, the tail and the count.
    const arr = (a) => {
      const v = a || [];
      return '[' + (v.length <= 16 ? v.join(', ') : v.slice(0, 8).join(', ') + ', …, ' + v.slice(-4).join(', ')) + ']'
        + (v.length > 16 ? ' (' + v.length + ' ints)' : '');
    };
    function table(head, rows) {
      return h('div', { className: 'exp4-table-wrap' }, h('table', { className: 'events-table' }, [
        h('thead', {}, h('tr', {}, head.map(t => h('th', {}, t)))),
        h('tbody', {}, rows.length ? rows.map(r => h('tr', {}, r.map((c, i) =>
          h('td', { className: i === 0 ? '' : 'mono-cell' }, c))))
          : h('tr', { className: 'empty-row' }, h('td', { colSpan: head.length }, 'Nothing yet.')))
      ]));
    }
    function totals(r) {
      return meta(r.messages + ' messages, ' + r.bytesSent + ' bytes, ' + r.rounds + (r.rounds === 1 ? ' round, ' : ' rounds, ')
        + fmt(r.timeMs) + ' ms from the first send to the last receipt (' + fmt(r.totalMs) + ' ms with the control plane);'
        + ' op ' + r.opId + ', epoch ' + r.epoch + (r.lamport != null ? ', Lamport ' + r.lamport : ''));
    }
    function trace(r) {
      return [h('h4', { style: { marginTop: '12px' } }, 'Message trace (ms after the operation started)'),
        table(['Round', 'From', 'To', 'Bytes', 'Sent', 'Received'], (r.trace || []).map(t => [
          // Non-breaking spaces keep "rank 4" and "node 5" whole when a cell wraps.
          String(t.round), 'rank ' + t.from + ' / node ' + t.fromNode, 'rank ' + t.to + ' / node ' + t.toNode,
          String(t.bytes), fmt(t.sentAtMs), fmt(t.receivedAtMs)]))];
    }
    function lines(items) {
      return h('ul', { className: 'inset mono-cell', style: { listStyle: 'none', margin: '0 0 8px', overflowWrap: 'anywhere' } },
        items.map(t => h('li', {}, t)));
    }

    // ---- communicator ------------------------------------------------------------------------
    const commMeta = meta('Loading the communicator…');
    const rootSelect = h('select', { className: 'form-control', id: 'exp9-root-select', style: { maxWidth: '280px' } });
    const commTable = h('div');

    // ---- broadcast ---------------------------------------------------------------------------
    const message = h('input', { className: 'form-control', id: 'exp9-message', type: 'text', maxLength: 1024,
      value: 'Hello from the root', placeholder: 'Message (1-1024 characters)' });
    const algorithm = h('select', { className: 'form-control', id: 'exp9-algorithm', style: { width: '130px' } },
      [h('option', { value: 'TREE' }, 'TREE'), h('option', { value: 'LINEAR' }, 'LINEAR')]);
    const bcastBtn = h('button', { className: 'btn btn-primary', type: 'button', id: 'exp9-bcast-btn', style: { height: '38px' } }, 'Broadcast');
    const bcastStatus = meta('');
    const bcastOut = h('div');
    const compareOut = h('div');

    // ---- scatter -----------------------------------------------------------------------------
    const dataInput = h('input', { className: 'form-control', id: 'exp9-data', type: 'text',
      placeholder: 'Comma-separated integers, length divisible by the size (at most 1000)' });
    const scatterBtn = h('button', { className: 'btn btn-primary', type: 'button', id: 'exp9-scatter-btn', style: { height: '38px' } }, 'Scatter');
    const scatterStatus = meta('');
    const scatterOut = h('div');

    // ---- gather ------------------------------------------------------------------------------
    const delay = h('input', { type: 'checkbox', id: 'exp9-delay', checked: true });
    const gatherBtn = h('button', { className: 'btn btn-primary', type: 'button', id: 'exp9-gather-btn', style: { height: '38px' } }, 'Gather');
    const gatherStatus = meta('');
    const gatherOut = h('div');

    // ---- demo --------------------------------------------------------------------------------
    const demoBtn = h('button', { className: 'btn btn-primary', type: 'button', id: 'exp9-demo-btn', style: { height: '38px' } }, 'Run scatter → compute → gather');
    const demoStatus = meta('');
    const demoOut = h('div');

    const root = h('div', { className: 'panel-inner', id: 'exp9-root' }, [
      h('div', { style: { marginBottom: '16px' } }, [
        h('h2', { style: { fontSize: '16px', fontWeight: '700' } }, 'Experiment 9: Collective Communication (MPI-style)'),
        h('p', { style: { fontSize: '14px', color: 'var(--text-muted)' } },
          'Broadcast, Scatter and Gather between the node processes. The communicator is every current member sorted by '
          + 'node id: rank = position, size = n. The control plane starts an operation on every rank, like mpirun; the ranks '
          + 'then exchange the data directly over TCP (each node listens on its RMI port + 100).'),
        h('p', { style: { fontSize: '13px', color: 'var(--text-dim)', marginTop: '6px' } },
          'Sockets between processes on one machine, not real MPI. A rank that is down or silent past the timeout fails '
          + 'the operation, naming it.')
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Communicator'),
        commMeta,
        h('div', { className: 'exp4-row', style: { marginBottom: '10px' } }, [field('Root', 'exp9-root-select', rootSelect)]),
        commTable
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Broadcast (MPI_Bcast)'),
        meta('The root sends the same bytes to every rank. LINEAR: the root sends n − 1 messages, one per round. '
          + 'TREE: a binomial tree; every rank that has the data forwards it, ceil(log2 n) rounds.'),
        h('div', { className: 'exp4-row' }, [field('Message', 'exp9-message', message, true),
          field('Algorithm', 'exp9-algorithm', algorithm), bcastBtn]),
        bcastStatus, bcastOut, compareOut
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Scatter (MPI_Scatter)'),
        meta('The root splits the array into n equal chunks and sends chunk i to rank i, in rank order. '
          + 'The length must be divisible by n: nothing is padded.'),
        h('div', { className: 'exp4-row' }, [field('Array', 'exp9-data', dataInput, true), scatterBtn]),
        scatterStatus, scatterOut
      ]),
      h('div', { className: 'exp4-box' }, [
        h('h3', {}, 'Gather (MPI_Gather)'),
        meta('Every rank sends the chunk it holds (from the last Scatter) to the root, which puts chunk i at position i '
          + 'whatever order they arrive in. With random per-rank delays they arrive out of rank order.'),
        h('div', { className: 'exp4-row', style: { alignItems: 'center' } }, [
          h('label', { className: 'job-stage-meta', htmlFor: 'exp9-delay', style: { display: 'flex', gap: '6px', alignItems: 'center', height: '38px' } },
            [delay, 'random per-rank delay']),
          gatherBtn
        ]),
        gatherStatus, gatherOut
      ]),
      h('div', { className: 'exp4-box', style: { marginBottom: '0' } }, [
        h('h3', {}, 'Scatter → compute → gather'),
        meta('Scatters 1..N (N = 4 × size), every rank squares its own chunk, and the root gathers the squares; '
          + 'the sum is checked against N(N+1)(2N+1)/6.'),
        demoBtn, demoStatus, demoOut
      ])
    ]);
    container.replaceChildren(root);

    // ---- communicator view -------------------------------------------------------------------
    function rootRank() {
      if (!view || panel._rootNode === '') return null;   // the server's default: the leader's rank
      const r = view.ranks.find(x => String(x.node) === panel._rootNode);
      return r ? r.rank : null;
    }

    function renderView(v) {
      view = v;
      if (panel._rootNode !== '' && !v.ranks.some(r => String(r.node) === panel._rootNode)) panel._rootNode = '';
      const effective = rootRank() == null ? v.root : rootRank();
      commMeta.textContent = 'epoch ' + v.epoch + ', size ' + v.size + ', timeout ' + v.timeoutMs + ' ms, '
        + (v.leader == null ? 'no agreed leader (the default root is rank 0)' : 'leader node ' + v.leader + ' (rank ' + v.root + ')')
        + (v.running ? '; running: ' + v.running : '');
      // Rebuilt only when the ranks change: replacing the options every poll would close an open dropdown.
      const key = JSON.stringify([v.root, v.rootNode, v.ranks.map(r => r.node)]);
      if (rootSelect.dataset.key !== key) {
        rootSelect.dataset.key = key;
        rootSelect.replaceChildren(h('option', { value: '' }, 'leader’s rank (rank ' + v.root + ' / node ' + v.rootNode + ')'),
          ...v.ranks.map(r => h('option', { value: String(r.node) }, 'rank ' + r.rank + ' / node ' + r.node)));
      }
      rootSelect.value = panel._rootNode;
      commTable.replaceChildren(table(['Rank', 'Node', 'Port', 'Status', 'Role'], v.ranks.map(r => [
        'rank ' + r.rank, 'node ' + r.node, String(r.port),
        h('span', { className: 'badge ' + (r.up ? 'online' : 'offline') }, r.up ? 'UP' : 'DOWN'),
        h('span', {}, [r.rank === effective ? h('span', { className: 'badge leader' }, 'ROOT') : '',
          r.node === v.leader ? ' leader' : ''])
      ])));
      if (!dataInput.value) {
        dataInput.value = Array.from({ length: 4 * v.size }, (_, i) => i + 1).join(', ');
      }
    }

    async function refresh() {
      if (!root.isConnected) {
        clearInterval(timer);
        return;
      }
      try {
        renderView(await AG.getJson('/api/collective', 'Communicator lookup failed'));
      } catch (err) {
        commMeta.textContent = 'Communicator unavailable: ' + err.message;
      }
    }

    rootSelect.addEventListener('change', () => {
      panel._rootNode = rootSelect.value;
      if (view) renderView(view);
    });

    // ---- results -----------------------------------------------------------------------------
    function showBroadcast(r) {
      bcastOut.replaceChildren(
        lines(r.ranks.map(x => 'rank ' + x.rank + ' / node ' + x.node + (x.rank === r.root ? ' (root) holds "' : ' received "')
          + x.received + '" (' + x.bytes + ' bytes, sha256 ' + String(x.sha256).slice(0, 12) + '…)'
          + (x.round != null ? ' from rank ' + x.from + ' in round ' + x.round : '') + (x.identical ? '' : ' DIFFERS'))),
        meta((r.identical ? 'Every rank holds identical bytes (SHA-256 ' + r.sha256.slice(0, 16) + '…). ' : 'Ranks DIFFER. ')
          + r.algorithm + ' from rank ' + r.root + ' / node ' + r.rootNode + ': ' + r.rounds + ' rounds (expected ' + r.expectedRounds + ').'),
        totals(r), ...trace(r));
      const rows = ['LINEAR', 'TREE'].filter(a => panel._compare[a]).map(a => {
        const c = panel._compare[a];
        return [a, String(c.size), String(c.messages), String(c.rounds), String(c.expectedRounds), fmt(c.timeMs), c.identical ? 'yes' : 'NO'];
      });
      compareOut.replaceChildren(h('h4', { style: { marginTop: '12px' } }, 'LINEAR vs TREE (last run of each)'),
        table(['Algorithm', 'Size', 'Messages', 'Rounds', 'Expected', 'Time (ms)', 'Identical'], rows),
        meta(rows.length < 2 ? 'Run the other algorithm to compare.'
          : 'Both send n − 1 messages; the tree needs fewer rounds because ranks forward in parallel. On one machine a '
            + 'message costs well under a millisecond, so the time difference is small and noisy.'));
    }

    function showScatter(r) {
      scatterOut.replaceChildren(
        lines(r.ranks.map(x => 'rank ' + x.rank + ' / node ' + x.node + (x.rank === r.root ? ' (root) keeps ' : ' received ')
          + arr(x.chunk) + (x.correct ? '' : ' WRONG'))),
        meta(r.data.length + ' ints, chunk size ' + r.chunkSize + ': ' + (r.correct ? 'rank i holds chunk i.' : 'chunks are WRONG.')),
        totals(r), ...trace(r));
    }

    function showGather(r, out) {
      const c = r.size ? r.result.length / r.size : 0;
      const segments = r.finalOrder.map(i => 'rank ' + i + ': ' + arr(r.result.slice(i * c, (i + 1) * c)));
      out.replaceChildren(
        h('div', { className: 'exp4-stats', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', marginBottom: '10px' } }, [
          h('div', { className: 'stat-item' }, [h('span', { className: 'stat-label' }, 'Arrival order at the root'),
            h('span', { className: 'stat-val' }, r.arrivalOrder.map(i => 'rank ' + i).join(' → ') || '—')]),
          h('div', { className: 'stat-item' }, [h('span', { className: 'stat-label' }, 'Final buffer order'),
            h('span', { className: 'stat-val' }, r.finalOrder.map(i => 'rank ' + i).join(', '))])
        ]),
        table(['Rank', 'Node', 'Delay (ms)', 'Arrived', 'Chunk'], r.ranks.map(x => [
          'rank ' + x.rank, 'node ' + x.node, String(x.delayMs), x.arrived == null ? 'root (local)' : '#' + x.arrived, arr(x.chunk)])),
        h('h4', { style: { marginTop: '12px' } }, 'Root buffer (rank order)'),
        lines(segments),
        meta((r.inRankOrder ? 'Correct: the buffer is in rank order' : 'WRONG: the buffer is not in rank order')
          + (r.outOfOrder ? ', although the chunks arrived out of order.' : '.')),
        totals(r), ...trace(r));
    }

    function showDemo(r) {
      demoOut.replaceChildren(
        table(['Rank', 'Node', 'Received', 'Squared'], r.compute.ranks.map(x => ['rank ' + x.rank, 'node ' + x.node, arr(x.before), arr(x.after)])),
        h('h4', { style: { marginTop: '12px' } }, 'Gathered at rank ' + r.root + ' / node ' + r.rootNode),
        lines([arr(r.gather.result)]),
        h('p', { className: 'job-stage-meta' }, r.verified
          ? 'Sum of squares 1..' + r.n + ' = ' + r.sumOfSquares + ', expected ' + r.expected + ': verified.'
          : h('span', { className: 'text-rose' }, 'Sum of squares 1..' + r.n + ' = ' + r.sumOfSquares + ', expected '
            + r.expected + ': MISMATCH.')),
        meta('Scatter ' + fmt(r.scatter.timeMs) + ' ms (' + r.scatter.messages + ' messages), compute ' + fmt(r.compute.totalMs)
          + ' ms (no messages), gather ' + fmt(r.gather.timeMs) + ' ms (' + r.gather.messages + ' messages).'));
    }

    const shows = { broadcast: showBroadcast, scatter: showScatter, gather: r => showGather(r, gatherOut), demo: showDemo };
    const statuses = { broadcast: bcastStatus, scatter: scatterStatus, gather: gatherStatus, demo: demoStatus };

    // ---- running an operation (one at a time) ------------------------------------------------
    async function runOp(op, body, pendingText) {
      if (busy) return;
      const status = statuses[op];
      const rank = rootRank();
      if (rank != null) body.root = rank;
      busy = true;
      [bcastBtn, scatterBtn, gatherBtn, demoBtn].forEach(b => { b.disabled = true; });
      setStatus(status, pendingText, false);
      try {
        const res = await fetch('/api/collective/' + op, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
        });
        const r = await res.json().catch(() => ({ error: 'HTTP ' + res.status }));
        if (!res.ok) throw new Error(r.error || ('HTTP ' + res.status));
        panel._last[op] = r;
        delete panel._errors[op];
        if (op === 'broadcast') panel._compare[r.algorithm] = r;
        setStatus(status, '', false);
        shows[op](r);
      } catch (err) {
        panel._errors[op] = err.message;
        setStatus(status, err.message, true);   // server text, rendered as text
      } finally {
        busy = false;
        [bcastBtn, scatterBtn, gatherBtn, demoBtn].forEach(b => { b.disabled = false; });
        refresh();
      }
    }

    bcastBtn.addEventListener('click', () =>
      runOp('broadcast', { message: message.value, algorithm: algorithm.value }, 'Broadcasting (' + algorithm.value + ')…'));

    scatterBtn.addEventListener('click', () => {
      const parts = dataInput.value.split(/[\s,]+/).filter(Boolean);
      const bad = parts.find(p => !/^-?\d+$/.test(p));
      if (bad !== undefined) {
        setStatus(scatterStatus, 'Not an integer: ' + bad, true);
        return;
      }
      runOp('scatter', { data: parts.map(Number) }, 'Scattering ' + parts.length + ' ints…');
    });

    gatherBtn.addEventListener('click', () =>
      runOp('gather', { randomDelay: delay.checked }, delay.checked ? 'Gathering (random delays up to a few hundred ms)…' : 'Gathering…'));

    demoBtn.addEventListener('click', () => runOp('demo', {}, 'Scattering 1..N, squaring on every rank, gathering…'));

    // Last results and errors survive a tab switch.
    Object.keys(shows).forEach(op => {
      if (panel._last[op]) shows[op](panel._last[op]);
      if (panel._errors[op]) setStatus(statuses[op], panel._errors[op], true);
    });

    AG.onMembershipChange(() => { if (root.isConnected) refresh(); });
    refresh();
    timer = setInterval(refresh, 2000);
  }
};
