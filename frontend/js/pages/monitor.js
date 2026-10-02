/* 作业监控 Monitor */
Components.init('monitor');
const C = Components;

let currentJob = '';
let kindFilter = '';
let lastData = null;

function setupFilters() {
  const kinds = [['', '全部 All'], ['map', 'Map'], ['reduce', 'Reduce']];
  document.getElementById('kind-filter').innerHTML = kinds.map(([k, label]) =>
    `<span class="pill${kindFilter === k ? ' active' : ''}" data-k="${k}">${label}</span>`).join('');
  document.querySelectorAll('#kind-filter .pill').forEach(p => {
    p.addEventListener('click', () => { kindFilter = p.getAttribute('data-k'); setupFilters(); render(); });
  });
}

async function render() {
  if (!currentJob) return;
  try {
    lastData = await API.get('/api/jobs/' + currentJob);
  } catch (e) { return; }
  const job = lastData.job;
  const tasks = lastData.tasks || [];

  // Summary + stage progress
  const sp = job.stage_progress || {};
  const sh = job.shuffle || {};
  const q = job.queue || {};
  const stageBar = (key, label) => {
    const st = sp[key] || {};
    let pct = st.pct || 0;
    if (key === 'shuffle') pct = sh.pct || 0;
    return C.progress(pct, label + (key === 'shuffle' ? ` (${sh.done||0}/${sh.total||0})` : ` (${st.done||0}/${st.total||0})`));
  };
  const terminal = ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(job.status);
  document.getElementById('summary').innerHTML = `
    <div class="stat-tiles">
      <div class="stat"><div class="label">作业 Job</div><div class="value" style="font-size:20px">${C.esc(job.name)}</div>
        <div class="delta mono">${C.esc(job.job_id)}</div></div>
      <div class="stat"><div class="label">状态 Status</div><div class="value" style="font-size:20px">${C.stateBadge(job.status, true)}</div></div>
      <div class="stat"><div class="label">优先级 Priority</div><div class="value" style="font-size:20px">${C.priorityBadge(job.priority)}</div>
        <div class="delta">${q.effective_priority && q.effective_priority > job.priority ? `老化提升 boosted → P${q.effective_priority}` : '基础优先级 base'}</div></div>
      <div class="stat"><div class="label">队列 Queue</div><div class="value" style="font-size:20px">${q.queue_state ? C.stateBadge(q.queue_state, true) : '-'}</div>
        <div class="delta">${q.queue_position ? `#${q.queue_position} · 份额 share ${q.fair_share} · 等待 ${q.waiting_ms ? C.fmtDur(q.waiting_ms) : '0s'}` : ''}</div></div>
      <div class="stat"><div class="label">Map 任务 Tasks</div><div class="value">${job.num_map_tasks}</div></div>
      <div class="stat"><div class="label">Reduce 任务 Tasks</div><div class="value">${job.num_reduce_tasks}</div></div>
      <div class="stat"><div class="label">输入记录 Records</div><div class="value">${C.fmtNum(job.input_rows)}</div></div>
      <div class="stat"><div class="label">故障事件 Faults</div><div class="value ${job.fault_count ? 'bad' : ''}">${job.fault_count || 0}</div></div>
    </div>
    ${terminal ? '' : `<div class="card mt"><div class="flex" style="align-items:center;gap:10px">
      <span class="small muted">调整优先级 Change priority:</span>
      <select id="prio-select" style="width:auto">
        ${[10, 8, 5, 3, 1].map(p => `<option value="${p}" ${p === job.priority ? 'selected' : ''}>P${p} ${C.priorityLabel(p)}</option>`).join('')}
      </select>
      <button class="btn small" id="prio-apply">应用 Apply</button>
      <span class="hint">下一调度周期生效 Takes effect on the next scheduler tick.</span>
    </div></div>`}
    <div class="card mt">
      <div class="grid cols-3">
        <div>${stageBar('map', 'Map')}</div>
        <div>${stageBar('shuffle', 'Shuffle')}</div>
        <div>${stageBar('reduce', 'Reduce')}</div>
      </div>
    </div>`;

  const applyBtn = document.getElementById('prio-apply');
  if (applyBtn) {
    applyBtn.addEventListener('click', async () => {
      const prio = parseInt(document.getElementById('prio-select').value, 10);
      try {
        await API.post('/api/jobs/' + currentJob + '/priority', { priority: prio });
        C.toast('优先级已更新 Priority updated: P' + prio, 'ok');
        render();
      } catch (e) { C.toast('更新失败 ' + e.message, 'error'); }
    });
  }

  // Tasks table
  const filtered = tasks.filter(t => !kindFilter || t.kind === kindFilter);
  document.getElementById('tasks').innerHTML = filtered.length ? C.table([
    { key: 'task_id', label: '任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
    { key: 'kind', label: '类型 Kind', render: r => r.kind },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'worker_name', label: 'Worker', render: r => C.esc(r.worker_name || r.worker_id || '-') },
    { key: 'attempts', label: '尝试 Att', render: r => r.attempts, num: true },
    { key: 'preemptions', label: '被抢占 Preempted', render: r => (r.stats && r.stats.preemptions) || 0, num: true },
    { key: 'progress', label: '进度 Progress', render: r => `<div style="min-width:120px">${C.progress((r.progress||0)*100)}</div>` },
    { key: 'records_processed', label: '处理 Records', render: r => C.fmtNum(r.records_processed), num: true },
    { key: 'duration_ms', label: '耗时 Dur', render: r => C.fmtDur(r.duration_ms), num: true },
  ], filtered) : C.empty();
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
document.getElementById('refresh').addEventListener('click', render);
setupFilters();
C.poll(render, 2000).start();
