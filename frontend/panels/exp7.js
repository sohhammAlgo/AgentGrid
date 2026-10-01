// Experiment 7 Panel: MapReduce with Apache Spark (PySpark, local mode) over the corpus the
// cluster searches. The control plane runs mapreduce/exp7_mapreduce.py --json and returns its
// one JSON line (POST /api/mapreduce/run); everything shown comes from that result. A
// successful "index" run also refreshes the inverted index that INDEX retrieval in the Job
// Console uses. Everything is built with text nodes (AgentGrid.h), never with innerHTML.

window.AgentGridPanels = window.AgentGridPanels || {};

window.AgentGridPanels.exp7 = {
  // One run at a time per page; kept on the panel so switching tabs does not lose it.
  _inFlight: null,
  _startedAt: 0,
  _lastResult: null,
  _lastError: null,

  render: function(container) {
    const AG = window.AgentGrid;
    const h = (...a) => AG.h(...a);
    const panel = this;

    const job = h('select', { className: 'form-control', id: 'exp7-job' }, [
      h('option', { value: 'index' }, 'inverted index (+ retrieval)'),
      h('option', { value: 'wordcount' }, 'word count')
    ]);
    const partitions = h('input', { className: 'form-control', id: 'exp7-partitions', type: 'number',
      min: 1, max: 16, value: '4', style: { width: '90px' } });
    const compare = h('input', { type: 'checkbox', id: 'exp7-compare' });
    const run = h('button', { className: 'btn btn-primary', type: 'button', style: { height: '38px' } }, 'Run MapReduce');
    const progress = h('div', { className: 'job-stage-meta', style: { marginTop: '10px' } });
    const results = h('div', { id: 'exp7-results' });

    container.replaceChildren(h('div', { className: 'panel-inner', id: 'exp7-root' }, [
      h('div', { style: { marginBottom: '16px' } }, [
        h('h3', { style: { fontSize: '16px', fontWeight: '700' } }, 'Experiment 7: MapReduce with Apache Spark'),
        h('p', { style: { fontSize: '13px', color: 'var(--text-muted)' } },
          'Word count and an inverted index over the 40 corpus documents: map, map-side combine, hash-partitioned '
          + 'shuffle, reduce. Every Spark result is checked against plain Python. A successful index run '
          + 'enables INDEX retrieval in the Job Console.'),
        h('p', { style: { fontSize: '12px', color: 'var(--text-dim)', marginTop: '6px' } },
          'Spark runs in local mode on the control-plane machine, not on the cluster nodes.')
      ]),
      h('div', { className: 'exp4-box' }, [
        h('div', { className: 'exp4-row' }, [
          h('div', { className: 'form-group', style: { marginBottom: '0' } },
            [h('label', { className: 'form-label', htmlFor: 'exp7-job' }, 'Job'), job]),
          h('div', { className: 'form-group', style: { marginBottom: '0' } },
            [h('label', { className: 'form-label', htmlFor: 'exp7-partitions' }, 'Partitions'), partitions]),
          h('label', { className: 'job-stage-meta', htmlFor: 'exp7-compare',
            style: { display: 'flex', gap: '6px', alignItems: 'center', height: '38px' } },
            [compare, 'also compare with local[1]']),
          run
        ]),
        progress
      ]),
      results
    ]));

    // ---- rendering ----------------------------------------------------------------------
    function stat(label, value) {
      return h('div', { className: 'stat-item' }, [h('span', { className: 'stat-label' }, label),
        h('span', { className: 'stat-val' }, value == null ? '—' : String(value))]);
    }

    function table(head, rows) {
      return h('div', { className: 'exp4-table-wrap', style: { maxHeight: 'none' } },
        h('table', { className: 'events-table' }, [
          h('thead', {}, h('tr', {}, head.map(t => h('th', {}, t)))),
          h('tbody', {}, rows.map(r => h('tr', {}, r.map((c, i) => h('td', { className: i === 0 ? '' : 'mono-cell' }, c)))))
        ]));
    }

    function section(title, children) {
      return h('div', { className: 'exp4-box' }, [h('h4', {}, title)].concat(children));
    }

    function renderResult(r) {
      const s = r.stages || {};
      const t = r.timings || {};
      const isIndex = r.job === 'index';
      const saved = s.shuffleRecordsWithout ? (100 * (s.shuffleRecordsWithout - s.shuffleRecordsWithCombiner) / s.shuffleRecordsWithout) : null;
      const parts = [];

      parts.push(h('div', { className: 'job-status-row' }, [
        h('span', { className: 'job-badge ' + (r.verified ? 'job-COMPLETE' : 'job-FAILED') },
          r.verified ? 'VERIFIED: identical to plain Python' : 'NOT VERIFIED: Spark and plain Python disagree'),
        h('span', {}, ['job ', h('b', {}, r.job)]),
        h('span', {}, r.partitions + ' partitions'),
        h('span', {}, r.docCount + ' documents'),
        h('span', {}, 'built ' + r.builtAt),
        h('span', {}, 'whole run ' + (r.wallMs == null ? '—' : (r.wallMs / 1000).toFixed(1) + ' s') + ' (incl. Spark start-up)')
      ]));

      parts.push(section('Stages', [
        table(['Stage', 'Records', 'What'], [
          ['MAP', String(s.mapPairs), isIndex ? '(term, (docId, 1)) pairs emitted by the mappers' : '(term, 1) pairs emitted by the mappers'],
          ['COMBINE', String(s.pairsAfterCombine), isIndex ? 'pairs after the map-side combine on (term, docId)' : 'pairs after the map-side combine (reduceByKey\'s combiner)'],
          ['REDUCE', String(s.reduceKeys), isIndex ? 'terms in the index (' + s.postings + ' postings)' : 'distinct terms (' + s.tokens + ' tokens)']
        ]),
        h('p', { className: 'job-stage-meta', style: { marginTop: '8px' } },
          'Shuffle: ' + s.shuffleRecordsWithCombiner + ' records cross it with the combiner vs '
          + s.shuffleRecordsWithout + ' without' + (saved == null ? '' : ' → ' + saved.toFixed(1) + '% fewer')
          + (isIndex ? ' (first shuffle, reduceByKey on (term, docId); groupByKey then shuffles every posting).' : '.'))
      ]));

      const timing = [stat('local[4] pass', t.local4Ms == null ? null : Math.round(t.local4Ms) + ' ms')];
      if (t.local1Ms != null) {
        timing.push(stat('local[1] pass', Math.round(t.local1Ms) + ' ms'));
        timing.push(stat('speed-up', t.speedup == null ? null : t.speedup.toFixed(2) + 'x'));
      }
      const timingBox = [h('div', { className: 'exp4-stats', style: { gridTemplateColumns: 'repeat(' + timing.length + ', 1fr)' } }, timing)];
      if (t.local1Ms != null) {
        timingBox.push(h('p', { className: 'job-stage-meta', style: { marginTop: '8px' } },
          'Speed-up local[4] vs local[1]: ' + (t.speedup == null ? '—' : t.speedup.toFixed(2) + 'x')
          + '. With 40 short documents, task scheduling and Python worker start-up dominate, so the gain is small.'));
      }
      parts.push(section('Timing (one pass of the job)', timingBox));

      const terms = r.topTerms || [];
      parts.push(section('Top ' + terms.length + ' terms', [table(
        isIndex ? ['#', 'Term', 'Occurrences', 'Documents (df)'] : ['#', 'Term', 'Occurrences'],
        terms.map((x, i) => isIndex ? [String(i + 1), x.term, String(x.count), String(x.df)] : [String(i + 1), x.term, String(x.count)])
      )]));

      if (isIndex) {
        const docs = r.topDocsForQuery || [];
        parts.push(section('Retrieval over the index: top ' + docs.length, [
          h('p', { className: 'job-stage-meta', style: { marginBottom: '6px' } },
            'Query: "' + r.query + '". ' + (r.docsMatchingQuery == null ? '' : r.docsMatchingQuery
              + ' of ' + r.docCount + ' documents contain at least one query term. ')
            + 'tf-idf as in the cluster\'s RANK stage.'),
          table(['#', 'Document', 'Title', 'Score'], docs.map(d => [String(d.rank), d.docId, d.title, Number(d.score).toFixed(4)]))
        ]));
      } else {
        parts.push(h('p', { className: 'job-stage-meta' }, 'The retrieval top 5 is computed by the "inverted index" job.'));
      }

      const ix = r.index;
      if (ix) {
        parts.push(h('p', { className: ix.usable ? 'job-stage-meta' : 'job-stage-meta text-rose' },
          ix.usable ? 'Index loaded by the control plane: ' + ix.terms + ' terms, ' + ix.postings
            + ' postings, corpus fingerprint OK. INDEX retrieval is available in the Job Console.'
            : 'Index not usable for INDEX retrieval: ' + (ix.reason || 'unknown reason')));
      }
      results.replaceChildren(...parts);
    }

    function renderError(message) {
      results.replaceChildren(h('div', { className: 'exp4-box' }, [
        h('h4', {}, 'MapReduce run failed'),
        h('pre', { className: 'text-rose', style: { whiteSpace: 'pre-wrap', fontSize: '12px', margin: '0' } }, message)
      ]));
    }

    function showIdle() {
      run.disabled = false;
      progress.textContent = '';
    }

    // ---- running (single flight) ---------------------------------------------------------
    let ticker = null;
    function tick() {
      if (!progress.isConnected) { clearInterval(ticker); return; }
      const secs = Math.round((Date.now() - panel._startedAt) / 1000);
      progress.textContent = 'Running... ' + secs + ' s (takes about 30 s' + (compare.checked ? ', longer with local[1]' : '') + ')';
    }

    function follow(promise) {
      run.disabled = true;
      tick();
      ticker = setInterval(tick, 1000);
      promise.then(() => {
        clearInterval(ticker);
        if (!progress.isConnected) return;
        showIdle();
        if (panel._lastError) renderError(panel._lastError);
        else if (panel._lastResult) renderResult(panel._lastResult);
      });
    }

    run.addEventListener('click', () => {
      if (panel._inFlight) return;
      const n = parseInt(partitions.value, 10);
      if (!(n >= 1 && n <= 16)) {
        renderError('Partitions must be a whole number from 1 to 16.');
        return;
      }
      panel._startedAt = Date.now();
      panel._lastError = null;
      results.replaceChildren();
      panel._inFlight = AG.runMapReduce(job.value, n, compare.checked)
        .then(r => { panel._lastResult = r; })
        .catch(err => { panel._lastError = err.message; })
        .finally(() => {
          panel._inFlight = null;
          if (window.AgentGridJobs && window.AgentGridJobs.refreshIndexState) window.AgentGridJobs.refreshIndexState();
        });
      follow(panel._inFlight);
    });

    // ---- initial state -------------------------------------------------------------------
    if (panel._inFlight) {
      follow(panel._inFlight);
    } else if (panel._lastError) {
      renderError(panel._lastError);
    } else if (panel._lastResult) {
      renderResult(panel._lastResult);
    } else {
      AG.mapReduceLast().then(r => {
        if (r && !panel._inFlight && results.isConnected) {
          panel._lastResult = r;
          renderResult(r);
        }
      }).catch(() => {});
      AG.mapReduceStatus().then(st => {
        if (st.running && !panel._inFlight && progress.isConnected) {
          progress.textContent = 'A run started elsewhere is in progress (' + Math.round(st.current.elapsedMs / 1000)
            + ' s so far); a new run is refused until it finishes.';
        }
      }).catch(() => {});
    }
  }
};
