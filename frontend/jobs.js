// Job Console: submit a query to the orchestrator on the elected leader and watch the
// pipeline (RETRIEVE -> RANK -> SUMMARIZE -> SYNTHESIZE) run across the nodes, plus the
// findings the workers posted to the replicated blackboard.
// Retrieval SCAN (default) reads every document in RETRIEVE; INDEX reads only the documents the
// Exp 7 MapReduce index lists for the query's terms, and is enabled only while
// /api/mapreduce/index reports a usable (present, not stale) index.
// Renders only what /api/jobs, /api/election and /api/mapreduce/index report. Every piece of user or corpus text
// (query, answer, keys, errors) is written with text nodes, never through innerHTML.

(function() {
  const AG = window.AgentGrid;
  const h = (...a) => AG.h(...a);
  let currentJobId = null;
  let pollToken = 0;

  function render() {
    const root = document.getElementById('job-console');
    const query = h('input', { id: 'job-query', className: 'form-control', maxLength: 500,
      value: 'How do leader election and failure detectors handle a crashed node?' });
    const policy = h('select', { id: 'job-policy', className: 'form-control', title: 'Routing policy' },
      ['WEIGHTED', 'LEAST_LOADED', 'ROUND_ROBIN'].map(p => h('option', { value: p }, p)));
    const consistency = h('select', { id: 'job-consistency', className: 'form-control',
      title: 'Consistency of the findings the workers post to the blackboard' },
      [h('option', { value: 'EVENTUAL' }, 'EVENTUAL findings'), h('option', { value: 'STRONG' }, 'STRONG findings')]);
    const retrieval = h('select', { id: 'job-retrieval', className: 'form-control',
      title: 'How RETRIEVE picks documents: SCAN reads all of them, INDEX only those the MapReduce index lists' },
      [h('option', { value: 'SCAN' }, 'SCAN retrieval'),
       h('option', { value: 'INDEX', disabled: true }, 'INDEX retrieval')]);
    const submit = h('button', { type: 'submit', id: 'job-submit', className: 'btn btn-primary' }, 'Submit');
    const form = h('form', { id: 'job-form', className: 'job-form' }, [query, policy, consistency, retrieval, submit]);

    root.replaceChildren(
      form,
      h('div', { id: 'job-retrieval-hint', className: 'job-stage-meta', style: { margin: '-6px 0 12px' } },
        'INDEX retrieval: checking the MapReduce index...'),
      h('div', { className: 'job-grid' }, [
        h('div', {}, [
          h('div', { className: 'job-status-row', id: 'job-status' }, 'No job selected.'),
          h('div', { className: 'job-pipeline', id: 'job-pipeline' }),
          h('div', { className: 'job-answer', id: 'job-answer' }),
          h('div', { className: 'job-findings', id: 'job-findings' })
        ]),
        h('div', {}, [
          h('h4', { className: 'job-subhead' }, 'Recent jobs'),
          h('div', { className: 'job-recent', id: 'job-recent' })
        ])
      ])
    );

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      submit.disabled = true;
      try {
        const resp = await AG.submitJob(query.value, policy.value, consistency.value, retrieval.value);
        select(resp.jobId);
      } catch (err) {
        showError(err.message);
      } finally {
        submit.disabled = false;
      }
    });
  }

  /**
   * Enables the INDEX option only while the MapReduce index is present and not stale; called at
   * start, on every MAPREDUCE_* event and every 10 s.
   */
  async function refreshIndexState() {
    const select = document.getElementById('job-retrieval');
    const hint = document.getElementById('job-retrieval-hint');
    if (!select || !hint) return;
    const option = select.querySelector('option[value="INDEX"]');
    let text;
    let usable = false;
    try {
      const ix = await AG.mapReduceIndex();
      usable = !!ix.usable;
      if (usable) {
        text = 'INDEX retrieval available: MapReduce index of ' + ix.terms + ' terms over ' + ix.documents
          + ' documents, built ' + ix.builtAt + '.';
      } else if (!ix.available) {
        text = 'INDEX retrieval disabled: no MapReduce index yet. Run MapReduce first (MapReduce tab, job "index").';
      } else {
        text = 'INDEX retrieval disabled: the index is stale, re-run MapReduce. ' + (ix.reason || '');
      }
    } catch (err) {
      text = 'INDEX retrieval disabled: ' + err.message;
    }
    option.disabled = !usable;
    if (!usable && select.value === 'INDEX') select.value = 'SCAN';
    hint.textContent = text;
  }

  function showError(message) {
    document.getElementById('job-status').replaceChildren(h('span', { className: 'text-rose' }, message));
  }

  function statusBadge(status) {
    return h('span', { className: 'job-badge job-' + String(status).replace(/[^A-Z_]/g, '') }, status);
  }

  function renderJob(job) {
    const parts = [
      statusBadge(job.status),
      h('span', { className: 'mono-cell' }, job.jobId),
      h('span', {}, ['policy ', h('b', {}, job.policy)]),
      h('span', {}, 'findings ' + (job.consistency || '—')),
      h('span', {}, 'leader node ' + job.leaderNode),
      h('span', {}, job.completedSubtasks + '/' + job.subtasks + ' subtasks'),
      h('span', {}, 'makespan ' + (job.makespanMs == null ? '—' : job.makespanMs + ' ms'))
    ];
    const r = job.retrieval;
    if (r) {
      parts.push(h('span', {}, ['retrieval ', h('b', {}, r.mode),
        r.docsTouched == null ? '' : ' · docs touched ' + r.docsTouched + '/' + r.docsTotal
          + ' · ' + r.retrieveSubtasks + ' RETRIEVE subtasks · ' + r.retrievalMs + ' ms']));
    }
    if (job.orphanAdoptedBy != null) parts.push(h('span', {}, 'recorded as orphan by node ' + job.orphanAdoptedBy));
    if (job.error) parts.push(h('span', { className: 'text-rose' }, job.error));
    document.getElementById('job-status').replaceChildren(...parts);

    document.getElementById('job-pipeline').replaceChildren(...(job.stages || []).map(stage => {
      const chips = (stage.subtasks || []).map(s => h('span', {
        className: 'job-chip chip-' + String(s.status).replace(/[^A-Z_]/g, ''),
        title: (s.input || '') + '\ndispatch L=' + s.dispatchLamport + ' result L=' + s.resultLamport
          + ' complete L=' + s.completeLamport
      }, s.subtaskId + ' → ' + (s.node == null ? '—' : 'N' + s.node)));
      return h('div', { className: 'job-stage' }, [
        h('div', { className: 'job-stage-head' }, [
          h('span', {}, stage.type),
          h('span', { className: 'job-stage-meta' }, stage.status + (stage.durationMs != null ? ' · ' + stage.durationMs + ' ms' : ''))
        ]),
        h('div', { className: 'job-chips' }, chips.length ? chips : [h('span', { className: 'job-stage-meta' }, 'no subtasks yet')])
      ]);
    }));

    const answer = document.getElementById('job-answer');
    if (job.answer) {
      answer.replaceChildren(h('h4', { className: 'job-subhead' }, 'Answer'), h('p', {}, job.answer));
    } else if (job.status === 'ORPHANED') {
      answer.replaceChildren(h('h4', { className: 'job-subhead' }, 'Answer'),
        h('p', { className: 'text-rose' }, 'The leader stopped before this job finished; completed subtasks are kept above. Recovery is not implemented.'));
    } else {
      answer.replaceChildren();
    }
    renderFindings(job);
  }

  /** The findings (SUMMARIZE results) and answer each worker posted to the blackboard. */
  function renderFindings(job) {
    const box = document.getElementById('job-findings');
    const rows = [];
    (job.stages || []).forEach(stage => (stage.subtasks || []).forEach(s => {
      if (!s.blackboard) return;
      const bb = s.blackboard;
      rows.push(h('tr', {}, [
        h('td', { className: 'mono-cell' }, bb.key),
        h('td', { className: 'mono-cell' }, 'N' + (bb.writer == null ? s.node : bb.writer)),
        h('td', {}, h('span', { className: 'job-badge ' + (bb.stored ? 'job-COMPLETE' : 'job-FAILED') },
          bb.status + (bb.stored ? '' : ' (not stored)'))),
        h('td', { className: 'mono-cell' }, bb.latencyMs == null ? '—' : bb.latencyMs + ' ms'),
        h('td', { className: 'mono-cell' }, bb.timestamp == null ? '—' : String(bb.timestamp))
      ]));
    }));
    if (!rows.length) {
      box.replaceChildren();
      return;
    }
    const s = job.blackboard || {};
    box.replaceChildren(
      h('h4', { className: 'job-subhead' }, 'Findings on the blackboard (' + (s.consistency || '') + '): '
        + (s.stored ?? '?') + '/' + (s.posted ?? '?') + ' stored'),
      h('div', { className: 'exp4-table-wrap' }, h('table', { className: 'events-table' }, [
        h('thead', {}, h('tr', {}, ['Key', 'Writer', 'Status', 'Write', 'LWW timestamp'].map(t => h('th', {}, t)))),
        h('tbody', {}, rows)
      ]))
    );
  }

  async function select(jobId) {
    currentJobId = jobId;
    const token = ++pollToken;
    try {
      await AG.waitForJob(jobId, job => {
        if (token === pollToken) renderJob(job);
      });
    } catch (err) {
      if (token === pollToken) showError(err.message);
    }
    refreshRecent();
  }

  // The recent-jobs rows are clickable and refreshed every 3 s: each row is created once per
  // job id and its cells are updated in place, so a click is not lost to a rebuilt row.
  const recentRows = new Map();
  let recentBody = null;

  async function refreshRecent() {
    try {
      const jobs = await AG.listJobs();
      const el = document.getElementById('job-recent');
      if (!jobs.length) {
        recentRows.clear();
        recentBody = null;
        el.replaceChildren(h('span', { className: 'job-stage-meta' }, 'No jobs yet.'));
        return;
      }
      if (!recentBody || !el.contains(recentBody)) {
        recentRows.clear();
        recentBody = h('tbody');
        el.replaceChildren(h('table', { className: 'events-table' }, [
          h('thead', {}, h('tr', {}, ['Job', 'Policy', 'Status', 'Makespan'].map(t => h('th', {}, t)))),
          recentBody
        ]));
      }
      const shown = jobs.slice(0, 12);
      const keep = new Set(shown.map(j => j.jobId));
      for (const [id, r] of recentRows) {
        if (!keep.has(id)) { r.tr.remove(); recentRows.delete(id); }
      }
      shown.forEach((j, i) => {
        let r = recentRows.get(j.jobId);
        if (!r) {
          r = { id: h('td', { className: 'mono-cell' }, j.jobId), policy: h('td', { className: 'mono-cell' }),
                status: h('td'), makespan: h('td', { className: 'mono-cell' }), key: null };
          r.tr = h('tr', { className: 'job-row', onclick: () => select(j.jobId) }, [r.id, r.policy, r.status, r.makespan]);
          recentRows.set(j.jobId, r);
        }
        const mode = j.retrieval && j.retrieval.mode === 'INDEX' ? ' / INDEX' : '';
        const key = [j.policy, j.consistency, mode, j.status, j.makespanMs].join('|');
        if (key !== r.key) {
          r.key = key;
          r.policy.textContent = j.policy + (j.consistency ? ' / ' + j.consistency : '') + mode;
          r.status.replaceChildren(statusBadge(j.status));
          r.makespan.textContent = j.makespanMs == null ? '—' : j.makespanMs + ' ms';
        }
        r.tr.classList.toggle('selected', j.jobId === currentJobId);
        if (recentBody.children[i] !== r.tr) recentBody.insertBefore(r.tr, recentBody.children[i] || null);
      });
    } catch (ignored) {}
  }

  function renderLeader(el) {
    const badge = document.getElementById('job-leader-badge');
    if (!badge || !el) return;
    badge.textContent = el.leaderId == null ? 'leader: none agreed' : `orchestrator on leader node ${el.leaderId}`;
    badge.className = 'badge ' + (el.leaderId == null ? 'offline' : 'online');
  }

  document.addEventListener('DOMContentLoaded', () => {
    render();
    refreshRecent();
    refreshIndexState();
    AG.onElectionUpdate(renderLeader);
    AG.onEvent(ev => { if (String(ev.type).startsWith('MAPREDUCE_')) refreshIndexState(); });
    setInterval(refreshRecent, 3000);
    setInterval(refreshIndexState, 10000);
  });

  window.AgentGridJobs = { select, refreshIndexState };
})();
