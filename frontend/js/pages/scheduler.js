/* 优先级队列 Scheduler queue */
Components.init('scheduler');
const C = Components;

const WAIT_REASONS = {
  running: ['运行中 Running', ''],
  ready: ['就绪 Ready', '调度器下一轮可分配'],
  higher_priority: ['等待更高优先级 Waiting higher priority', '高优先级作业正在使用执行槽'],
  fair_share: ['等待公平份额 Fair share', '同优先级作业按加权公平运行时间轮转'],
  retry_backoff: ['重试退避 Retry backoff', '失败后按指数退避等待重试'],
  shuffle: ['Shuffle 阶段', '等待分区数据整理完成'],
  stage: ['阶段等待 Stage', '当前阶段暂不派发任务'],
  done: ['已完成 Done', ''],
};

async function load() {
  let q;
  try { q = await API.get('/api/scheduler/queue'); } catch (e) { return; }

  document.getElementById('stats').innerHTML = [
    { label: '执行槽 Slots', value: `${q.free_slots}/${q.total_slots} 空闲` },
    { label: '排队作业 Queued', value: (q.queue || []).length },
    { label: '在线节点 Workers', value: (q.workers || []).length },
    { label: '抢占 Preemption', value: q.preemption_enabled ? '开启 On' : '关闭 Off' },
  ].map(s => `<div class="stat"><div class="label">${C.esc(s.label)}</div><div class="value" style="font-size:22px">${C.esc(s.value)}</div></div>`).join('');

  const jobs = q.jobs || [];
  document.getElementById('jobs').innerHTML = jobs.length ? C.table([
    { key: 'queue_position', label: '队位 Pos', render: r => r.queue_position ? `#${r.queue_position}` : '-', num: true },
    { key: 'name', label: '作业 Job', render: r => `<b>${C.esc(r.name)}</b><div class="small mono">${C.esc(r.job_id)}</div>` },
    { key: 'status', label: '阶段 Stage', render: r => C.stateBadge(r.status, true) },
    { key: 'queue_state', label: '队列 Queue', render: r => C.queueBadge(r.queue_state) },
    { key: 'priority', label: '优先级 Priority', render: r => C.priorityBadge(r), num: true },
    { key: 'running_tasks', label: '运行/待派', render: r => `${r.running_tasks || 0} / ${r.ready_tasks || 0}`, num: true },
    { key: 'wait_reason', label: '等待原因 Reason', render: r => waitReason(r) },
    { key: 'preempted_tasks', label: '让位次数', render: r => (r.stats && r.stats.preempted_tasks) || 0, num: true },
    { key: 'fair_vruntime_ms', label: '公平份额 Fair', render: r => C.fmtDur(r.fair_vruntime_ms), num: true },
    { key: 'created_ms', label: '提交时间', render: r => C.fmtTime(r.created_ms) },
    { key: 'actions', label: '调整 Priority', render: r => priorityControl(r) },
  ], jobs) : C.empty();

  document.querySelectorAll('[data-priority-job]').forEach(sel => {
    sel.addEventListener('change', async () => {
      const jobId = sel.getAttribute('data-priority-job');
      try {
        await API.put(`/api/jobs/${encodeURIComponent(jobId)}/priority`, { priority: parseInt(sel.value, 10) });
        C.toast('优先级已更新 Priority updated', 'ok');
        load();
      } catch (e) {
        C.toast('更新失败 ' + e.message, 'error');
      }
    });
  });

  const workers = q.workers || [];
  document.getElementById('workers').innerHTML = workers.length ? C.table([
    { key: 'name', label: '节点 Worker' },
    { key: 'capacity', label: '总槽 Slots', num: true },
    { key: 'occupied', label: '占用 Used', num: true },
    { key: 'draining', label: '让位中 Draining', render: r => r.draining || 0, num: true },
    { key: 'free', label: '空闲 Free', render: r => `<b>${r.free}</b>`, num: true },
  ], workers) : C.empty();
}

function waitReason(job) {
  const item = WAIT_REASONS[job.wait_reason] || [job.wait_reason, ''];
  const aging = job.effective_priority && job.effective_priority !== job.priority
    ? `<div class="small muted">老化后有效优先级：${job.effective_priority}</div>` : '';
  return `<div>${C.esc(item[0])}</div><div class="small muted">${C.esc(item[1])}</div>${aging}`;
}

function priorityControl(job) {
  if (['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(job.status)) return '-';
  const opts = [[1, '低 Low'], [5, '普通 Normal'], [10, '高 High']].map(([v, label]) =>
    `<option value="${v}" ${job.priority === v ? 'selected' : ''}>${label}</option>`).join('');
  return `<select class="job-select" data-priority-job="${C.esc(job.job_id)}">${opts}</select>`;
}

C.poll(load, 2500).start();
load();
