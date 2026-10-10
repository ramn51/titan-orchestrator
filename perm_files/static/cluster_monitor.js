import {percentile, workerOccupancy, executionRates} from './cluster_charts.js?v=8';
import {lineChart, barChart, stackedChart, mountChartInspector} from './cluster_chart_interactions.js?v=7';
import {mountDemoControls} from './cluster_demos.js';

const $ = selector => document.querySelector(selector);
const $$ = selector => [...document.querySelectorAll(selector)];
const esc = value => String(value ?? '').replace(/[&<>"]/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const ms = value => value == null ? 'n/a' : value < 1000 ? value.toFixed(0)+' ms' : (value/1000).toFixed(2)+' s';
const phaseMs = value => value == null ? 'n/a' : value < 1 ? value.toFixed(3)+' ms' :
  value < 10 ? value.toFixed(2)+' ms' : value.toFixed(1)+' ms';
const age = timestamp => timestamp ? ms(Date.now()-timestamp) : 'n/a';
const number = value => Number.isFinite(value) ? value : null;
const mean = values => values.length ? values.reduce((a,b)=>a+b,0)/values.length : null;
const latest = array => array.at(-1) || null;
const chronological = array => [...(array || [])].sort((a,b)=>a.at-b.at);
const runFamily = run => String(run?.group || run?.name || '')
  .replace(/^(CLUSTER_[A-Z_]+)_\d{14}[a-f0-9]+$/, '$1')
  .replace(/^(WFC_[A-Z0-9_]+_\d+)_\d{10,13}$/, '$1');
const recent = array => {
  const ordered=chronological(array), limit=240;
  if (ordered.length<=limit) return ordered;
  return Array.from({length:limit},(_,index)=>
    ordered[Math.round(index*(ordered.length-1)/(limit-1))]);
};
let snapshot = null, currentView = 'live', loading = false, timer = null;
let selectedWorkerId = null, selectedMaster = false;

function tile(label, value, tone = 'neutral', detail = '') {
  return '<article class="kpi"><span class="label">'+esc(label)+'</span><div class="value '+tone+'">'+
    esc(value)+'</div><div class="detail">'+esc(detail)+'</div></article>';
}
function panel(title, hint, body) {
  return '<article class="diag"><h3>'+esc(title)+'</h3><div class="hint">'+esc(hint)+'</div>'+body+'</article>';
}
function list(items, format, emptyText='None') {
  return items.length ? '<ul class="diag-list">'+items.slice(0,12).map(format).join('')+'</ul>' :
    '<div class="big ok">✓ '+esc(emptyText)+'</div>';
}
function metricData(data) {
  const samples = chronological(data.metrics?.samples);
  const spans = (data.spans || []).filter(s=>s && s.started_at);
  const workers = latest(samples)?.workers || (data.stats?.workers || []).map(w=>{
    const [load, capacity] = String(w.load || '0/4').split('/').map(Number);
    return {id:'worker:'+w.port,load,capacity,cpu_percent:-1,memory_used_bytes:-1,
      memory_total_bytes:-1,heartbeat_rtt_ms:-1,permanent:true,capabilities:w.capabilities};
  });
  return {samples, spans, workers, board:data.board || {counts:{},jobs:[],services:[],dead_letter:[]},
    phases:chronological(data.metrics?.dispatch_phases), operations:chronological(data.metrics?.store_operations),
    events:chronological(data.metrics?.events)};
}

function renderHealth(data, m) {
  const sample = latest(m.samples), counts = m.board.counts || {};
  const workers = m.workers.length, slots = m.workers.reduce((sum,w)=>sum+(+w.capacity||0),0);
  const occupied = m.workers.reduce((sum,w)=>sum+(+w.load||0),0);
  const done = m.samples.length > 1 ? Math.max(0,latest(m.samples).completed-m.samples[0].completed) : 0;
  const waits = m.spans.map(s=>s.started_at-s.eligible_at).filter(v=>Number.isFinite(v)&&v>=0);
  const heartbeat = m.workers.map(w=>w.heartbeat_rtt_ms).filter(v=>v>=0);
  const gaps = new Set((m.board.jobs||[]).filter(j=>j.lane==='parked').map(j=>j.capability));
  const ready = counts.ready ?? sample?.queue_depth ?? data.stats?.queue_size ?? 0;
  const running = counts.running ?? sample?.running ?? 0;
  const parked = counts.capability ?? sample?.capability_wait ?? 0;
  const dead = m.board.dead_letter?.length || 0;
  const writeLoss = sample?.store_dropped_writes;
  const healthy = sample?.store_healthy;
  const values = [
    ['Workers', workers, workers?'ok':'bad', slots+' slots'],
    ['Ready queue', ready, ready>0&&running===0?'bad':'neutral','eligible for dispatch'],
    ['Running', running+'/'+slots, running?'ok':'neutral','occupied slots '+occupied],
    ['Dispatch p95', ms(percentile(m.phases.map(p=>p.route+p.select+p.record+p.store+p.send),.95)), 'neutral','single-thread loop'],
    ['Queue wait p99', ms(percentile(waits,.99)), 'neutral',waits.length+' attempts'],
    ['Heartbeat max', ms(heartbeat.length?Math.max(...heartbeat):null), heartbeat.length?'ok':'neutral','round-trip to worker'],
    ['Done / window', done,done?'ok':'neutral','terminal completions'],
    ['Dead letter',dead,dead?'bad':'ok','retry exhausted'],
    ['Parked',parked,parked?'warn':'ok','no matching capability'],
    ['Capability gaps',gaps.size,gaps.size?'bad':'ok','required skill absent'],
    ['Store',healthy===true?'● Connected':healthy===false?'● Disconnected':'n/a',
      healthy===false||writeLoss>0?'bad':healthy===true?'ok':'neutral','write loss '+(writeLoss??'n/a')]
  ];
  $('#health-tiles').innerHTML = values.map(args=>tile(...args)).join('');
  const write = m.operations.filter(o=>o.kind==='write').map(o=>o.ms), read = m.operations.filter(o=>o.kind==='read').map(o=>o.ms);
  $('#store-strip').innerHTML = [
    ['TitanStore',healthy===true?'Connected':healthy===false?'Disconnected':'No reading'],
    ['Endpoint','configured on master'],['Write p50 / p99',ms(percentile(write,.5))+' / '+ms(percentile(write,.99))],
    ['Read p50 / p99',ms(percentile(read,.5))+' / '+ms(percentile(read,.99))],
    ['Write failures',writeLoss??'n/a'],['Span journal drops',sample?.dropped_span_writes??'n/a'],
    ['Pending span writes',sample?.pending_span_writes??'n/a']
  ].map(([label,value],index)=>'<div class="strip-item '+(index===0?'store-signal '+
    (healthy===true&&!(writeLoss>0)?'ok':healthy===false||writeLoss>0?'bad':'neutral'):
    (index===4&&writeLoss>0||index===5&&sample?.dropped_span_writes>0?'bad':''))+'"><span class="label">'+esc(label)+
    '</span><strong>'+(index===0?'<i class="signal-dot" aria-hidden="true"></i>':'')+esc(value)+'</strong></div>').join('');
}

function renderDiagnostics(m) {
  const jobs = m.board.jobs || [], workers = m.workers, dead = m.board.dead_letter || [];
  const parked = jobs.filter(j=>j.lane==='parked');
  const pending = jobs.filter(j=>j.lane==='parked'||/capacity/i.test(j.reason||''));
  const supply = new Map();
  jobs.filter(j=>j.lane==='parked'||j.lane==='ready').forEach(j=>{
    const cap=j.capability||'GENERAL', row=supply.get(cap)||{waiting:0,workers:0};
    row.waiting++; supply.set(cap,row);
  });
  for (const [cap,row] of supply) row.workers=workers.filter(w=>String(w.capabilities||'').includes(cap)).length;
  const waits=m.spans.map(s=>s.started_at-s.eligible_at).filter(v=>v>=0);
  const attempts=new Map();
  m.spans.forEach(s=>attempts.set(s.attempt,(attempts.get(s.attempt)||0)+1));
  const retryCount=m.spans.filter(s=>s.attempt>1).length;
  $('#diagnostic-panels').innerHTML =
    panel('Pending reasons','The latest reason a job could not be placed.',
      list(pending,j=>'<li><b>'+esc(j.job_id)+'</b><br><span class="warn">'+esc(j.reason)+'</span></li>'))+
    panel('Capability supply / demand','Waiting jobs against workers offering each requirement.',
      list([...supply],([cap,row])=>'<li><b class="'+(row.workers?'':'bad')+'">'+esc(cap)+'</b> · '+
        row.waiting+' waiting / '+row.workers+' worker(s)</li>','No gaps'))+
    panel('Dead-letter queue','Jobs that exhausted retries, with the last failure.',
      list(dead,j=>'<li><b>'+esc(j.job_id)+'</b><br><span class="bad">'+esc(j.reason||'Reason unavailable')+'</span></li>'))+
    panel('Queue wait','Eligibility to dispatch, including the tail.',
      '<div class="big">'+esc(ms(percentile(waits,.95)))+' p95</div><div class="small">p50 '+esc(ms(percentile(waits,.5)))+
      ' · p99 '+esc(ms(percentile(waits,.99)))+' · max '+esc(waits.length?ms(Math.max(...waits)):'n/a')+'</div>')+
    panel('Retries','Attempt distribution, not just terminal failure.',
      '<div class="big">'+(m.spans.length?(retryCount*100/m.spans.length).toFixed(1)+'%':'n/a')+
      '</div><ul class="diag-list">'+[...attempts].sort((a,b)=>a[0]-b[0]).map(([n,v])=>'<li>#'+n+' · '+v+' attempt(s)</li>').join('')+'</ul>');
}

function renderQueue(m) {
  const names = [
    ['delayed','Delayed','Timer has not fired'],
    ['blocked','Blocked','Waiting for parent jobs'],
    ['ready','Ready','Eligible, sorted by priority'],
    ['parked','Parked','No worker offers this capability']
  ];
  const counts=m.board.counts||{}, jobs=m.board.jobs||[];
  $('#queue-lanes').innerHTML=names.map(([key,title,hint])=>{
    const matching=jobs.filter(j=>j.lane===key).sort((a,b)=>(b.priority||0)-(a.priority||0));
    const count=key==='blocked'?counts.dependencies:key==='parked'?counts.capability:counts[key];
    const shown=Math.min(8,matching.length), total=count??matching.length;
    return '<article class="lane '+key+'"><div class="lane-head"><h3>'+title+'</h3><strong>'+esc(count??matching.length)+'</strong></div>'+
      '<p>'+hint+'</p>'+matching.slice(0,8).map(j=>'<div class="job-chip"><b>'+esc(j.job_id)+'</b>'+
      '<small>priority '+esc(j.priority)+' · '+esc(j.capability)+' · '+esc(j.reason)+'</small></div>').join('')+
      (!matching.length&&total===0?'<div class="more">No jobs in this lane</div>':'')+
      (!matching.length&&total>0?'<div class="more">Job samples unavailable</div>':'')+
      (total>shown?'<div class="more">+'+(total-shown)+' more</div>':'')+'</article>';
  }).join('');
}

function renderBudget(m) {
  const waits=m.spans.map(s=>s.started_at-s.eligible_at).filter(v=>v>=0);
  const phases=m.phases.map(p=>p.route+p.select+p.record+p.store+p.send);
  const store=m.operations.filter(o=>o.kind==='write').map(o=>o.ms);
  const heartbeat=m.workers.map(w=>w.heartbeat_rtt_ms).filter(v=>v>=0);
  const values=[['Queue wait',percentile(waits,.99),'#ebad67'],
    ['Dispatch placement',percentile(phases,.99),'#75bbfa'],
    ['Store write',percentile(store,.99),'#69dbaa'],
    ['Worker heartbeat',percentile(heartbeat,.99),'#b49af5']];
  const total=values.reduce((sum,item)=>sum+(item[1]||0),0);
  $('#latency-budget').innerHTML=values.map(([name,value,color])=>'<div class="budget-item"><span>'+name+' p99</span>'+
    '<strong>'+esc(ms(value))+'</strong><div class="budget-bar"><i style="width:'+
    (total&&value?Math.max(1,value/total*100):0)+'%;background:'+color+'"></i></div></div>').join('');
}

function renderTopology(data,m) {
  const workers=m.workers;
  const canvas=$('#topology-canvas');
  const previousScroll=[canvas.scrollLeft,canvas.scrollTop];
  if (!selectedMaster&&!workers.some(w=>w.id===selectedWorkerId)) selectedWorkerId=workers[0]?.id||null;
  canvas.innerHTML='<button class="master-node '+(selectedMaster?'selected':'')+
    '" id="master-node" aria-pressed="'+selectedMaster+'" title="Inspect master queue and health">MASTER<br><small>:9090</small></button>'+
    '<div class="worker-nodes">'+(workers.length?workers.map((w,index)=>{
      const capacity=+w.capacity||0,load=+w.load||0,fill=capacity?Math.min(100,load/capacity*100):0;
      return '<div class="worker-route">'+
        '<button class="worker-node '+(w.permanent?'':'ephemeral')+(w.id===selectedWorkerId&&!selectedMaster?' selected':'')+
        '" data-worker="'+index+'" aria-pressed="'+(w.id===selectedWorkerId&&!selectedMaster)+'"><strong>'+esc(w.id)+'</strong>'+
        '<small>'+esc(w.capabilities||'GENERAL')+' · '+(w.permanent?'permanent':'ephemeral')+'</small>'+
        '<div class="slot-bar"><i style="width:'+fill+'%;background:'+(fill>=100?'var(--red)':'var(--green)')+'"></i></div>'+
        '<small>'+load+'/'+capacity+' slots · '+(load?'Busy':'Idle')+' · heartbeat '+
        esc(ms(w.heartbeat_rtt_ms>=0?w.heartbeat_rtt_ms:null))+'</small></button></div>';
    }).join(''):'<div class="chart-empty">No registered workers</div>')+'</div>';
  canvas.scrollLeft=previousScroll[0];
  canvas.scrollTop=previousScroll[1];
  renderTopologyLinks(canvas,workers);
  $('#master-node').addEventListener('click',()=>{
    selectedMaster=true;
    $('#master-node').classList.add('selected');
    $('#master-node').setAttribute('aria-pressed','true');
    $$('.worker-node').forEach(node=>{node.classList.remove('selected');node.setAttribute('aria-pressed','false');});
    inspectMaster(data,m);
  });
  $$('.worker-node').forEach(button=>button.addEventListener('click',()=>{
    selectedMaster=false;
    $('#master-node').classList.remove('selected');
    $('#master-node').setAttribute('aria-pressed','false');
    selectedWorkerId=workers[+button.dataset.worker].id;
    $$('.worker-node').forEach(node=>{node.classList.toggle('selected',node===button);
      node.setAttribute('aria-pressed',String(node===button));});
    inspectWorker(workers[+button.dataset.worker],data,m);
  }));
  if (selectedMaster||!workers.length) inspectMaster(data,m);
  else inspectWorker(workers.find(w=>w.id===selectedWorkerId)||workers[0],data,m);
}
function renderTopologyLinks(canvas,workers) {
  canvas.querySelector('.topology-links')?.remove();
  if (!workers.length) return;
  const bounds=canvas.getBoundingClientRect();
  const master=canvas.querySelector('#master-node').getBoundingClientRect();
  const sourceX=master.right-bounds.left+canvas.scrollLeft;
  const sourceY=master.top-bounds.top+canvas.scrollTop+master.height/2;
  const paths=[...canvas.querySelectorAll('.worker-node')].map((node,index)=>{
    const target=node.getBoundingClientRect();
    const endX=target.left-bounds.left+canvas.scrollLeft-5;
    const endY=target.top-bounds.top+canvas.scrollTop+target.height/2;
    const bendX=sourceX+Math.max(22,(endX-sourceX)*.47);
    const busy=(+workers[index].load||0)>0;
    return '<path d="M '+sourceX+' '+sourceY+' H '+bendX+' V '+endY+' H '+endX+
      '" fill="none" stroke="'+(busy?'#70baff':'#557883')+'" stroke-width="'+(busy?2.4:1.5)+
      '" marker-end="url(#'+(busy?'busy':'idle')+'-arrow)"/>';
  }).join('');
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
  svg.setAttribute('class','topology-links');
  svg.setAttribute('aria-hidden','true');
  svg.setAttribute('width',String(Math.max(canvas.clientWidth,canvas.scrollWidth)));
  svg.setAttribute('height',String(Math.max(canvas.clientHeight,canvas.scrollHeight)));
  svg.innerHTML='<defs><marker id="busy-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">'+
    '<path d="M0 0 L8 4 L0 8 Z" fill="#70baff"/></marker><marker id="idle-arrow" markerWidth="8" markerHeight="8" '+
    'refX="7" refY="4" orient="auto"><path d="M0 0 L8 4 L0 8 Z" fill="#557883"/></marker></defs>'+paths;
  canvas.prepend(svg);
}
function inspectMaster(data,m) {
  const counts=m.board.counts||{}, sample=latest(m.samples)||{};
  const ready=counts.ready??sample.queue_depth??0;
  const running=counts.running??sample.running??0;
  const blocked=counts.dependencies??sample.dependency_wait??0;
  const parked=counts.capability??sample.capability_wait??0;
  const delayed=counts.delayed??sample.delayed??0;
  const healthy=sample.store_healthy!==false;
  const workers=m.workers.length;
  const slots=m.workers.reduce((sum,w)=>sum+(+w.capacity||0),0);
  $('#worker-inspector').innerHTML='<h3>Master · control plane</h3><div class="worker-state '+
    (data.online&&healthy?'idle':'busy')+'">● '+(data.online&&healthy?'Online · store connected':'Needs attention')+'</div>'+
    '<div class="inspect-grid"><span>Ready queue</span><b>'+esc(ready)+'</b>'+
    '<span>Running</span><b>'+esc(running)+'</b><span>DAG blocked</span><b>'+esc(blocked)+'</b>'+
    '<span>Capability parked</span><b>'+esc(parked)+'</b><span>Delayed</span><b>'+esc(delayed)+'</b>'+
    '<span>Fleet</span><b>'+esc(workers)+' workers · '+esc(slots)+' slots</b>'+
    '<span>Store</span><b class="'+(healthy?'ok':'bad')+'">'+(healthy?'Connected':'Unhealthy')+'</b>'+
    '<span>Write drops</span><b class="'+(sample.store_dropped_writes?'bad':'ok')+'">'+
    esc(sample.store_dropped_writes??0)+'</b></div>'+
    '<p class="worker-insight">'+(ready>0&&running===0?'Ready work is not dispatching; inspect capability and worker health.':
      parked>0?'Some work is waiting for a matching capability.':'Queue and fleet health at the latest sample.')+'</p>';
}
function inspectWorker(worker,data,m) {
  const memory=worker.memory_total_bytes>0?(worker.memory_used_bytes/worker.memory_total_bytes*100).toFixed(0)+'%':'n/a';
  const cpu=worker.cpu_percent>=0?worker.cpu_percent.toFixed(0)+'%':'n/a';
  const port=Number(worker.id.split(':').at(-1));
  const statsWorker=(data.stats?.workers||[]).find(w=>Number(w.port)===port);
  const running=m.spans.filter(s=>s.worker===worker.id&&!s.ended_at);
  const active=[...new Set([statsWorker?.active_job,...running.map(s=>s.job_id)].filter(Boolean))];
  const history=(statsWorker?.history||[]).filter(job=>job.status!=='RUNNING').slice(-7).reverse();
  const heartbeats=recent(m.samples).filter(s=>(s.workers||[]).some(w=>w.id===worker.id));
  const serviceNames=statsWorker?.services||[];
  const insight=worker.capacity&&worker.load>=worker.capacity
    ? worker.cpu_percent>=0&&worker.cpu_percent<35?'Slots full, host CPU low — jobs may be waiting on I/O.'
    : worker.cpu_percent>80?'Slots full, host CPU high — real saturation.':'Slots full; inspect running jobs.'
    : worker.cpu_percent>80?'Free slots, host CPU high — new jobs may compete for CPU.':'Worker has spare slots.';
  $('#worker-inspector').innerHTML='<h3>'+esc(worker.id)+'</h3><div class="worker-state '+(worker.load?'busy':'idle')+'">'+
    (worker.load?'● Busy · '+worker.load+' slot(s) occupied':'● Idle · ready for work')+'</div><div class="inspect-grid">'+
    '<span>Capability</span><b>'+esc(worker.capabilities||'n/a')+'</b>'+
    '<span>Slots</span><b>'+esc(worker.load)+' / '+esc(worker.capacity)+'</b>'+
    '<span>Host CPU</span><b>'+cpu+'</b><span>Host memory</span><b>'+memory+'</b>'+
    '<span>Load average</span><b>'+(worker.host_load_average>=0?worker.host_load_average.toFixed(2):'n/a')+'</b>'+
    '<span>Heartbeat RTT</span><b>'+esc(ms(worker.heartbeat_rtt_ms>=0?worker.heartbeat_rtt_ms:null))+'</b>'+
    '<span>Last seen</span><b>'+esc(age(worker.last_seen))+' ago</b>'+
    '<span>Services</span><b>'+esc(serviceNames.length?serviceNames.join(', '):'None')+'</b></div>'+
    '<p class="worker-insight">'+esc(insight)+'</p>'+
    '<h4>Heartbeat history</h4><div class="chart worker-heartbeat" id="worker-heartbeat"></div>'+
    '<h4>Running now ('+active.length+')</h4><ul class="worker-job-list">'+
    (active.length?active.map(id=>'<li class="warn">'+esc(id)+'</li>').join(''):'<li>Idle — no active job reported</li>')+
    '</ul><h4>Recent runs (top 7)</h4><ul class="worker-job-list">'+
    (history.length?history.map(job=>'<li><span class="'+
    (job.status==='COMPLETED'?'ok':job.status==='FAILED'||job.status==='DEAD'?'bad':'neutral')+'">'+
    esc(job.status)+'</span> · '+esc(job.id)+' · '+esc(job.time||'n/a')+'</li>').join(''):
    '<li>No recent runs on this worker</li>')+'</ul>';
  lineChart($('#worker-heartbeat'),heartbeats.map(s=>s.at),[{name:'RTT',color:'#76bafa',
    values:heartbeats.map(s=>{const value=s.workers.find(w=>w.id===worker.id)?.heartbeat_rtt_ms;
      return value>=0?value:null;})}],{unit:'ms'});
}

function chart(type, times, series, options) {
  const target=$('[data-chart="'+type+'"]');
  const units={queue:' jobs',throughput:' jobs/min',workers:' slots','workers-analytics':' slots',
    scaling:' count','capability-debt':' jobs','retry-pressure':' attempts'};
  if (target) lineChart(target,times,series,{unit:units[type]||'',...options});
}
function renderCharts(m) {
  const samples=recent(m.samples), times=samples.map(s=>s.at);
  stackedChart($('[data-chart="queue"]'),times,[
    {name:'ready',color:'#76bafa',values:samples.map(s=>s.queue_depth)},
    {name:'running',color:'#e8c472',values:samples.map(s=>s.running)},
    {name:'blocked',color:'#ae9bf7',values:samples.map(s=>s.dependency_wait)},
    {name:'delayed',color:'#65dca9',values:samples.map(s=>s.delayed)}],{unit:' jobs'});
  const phases=recent(m.phases), phaseTimes=phases.map(p=>p.at);
  chart('dispatch',phaseTimes,[{name:'loop ms',values:phases.map(p=>p.route+p.select+p.record+p.store+p.send)}],{unit:'ms'});
  const rates=samples.map((s,i)=>i?{
    completed:Math.max(0,s.completed-samples[i-1].completed),
    failed:Math.max(0,s.failed-samples[i-1].failed),
    minutes:Math.max(1/60000,(s.at-samples[i-1].at)/60000)}:{completed:0,failed:0,minutes:1});
  const historicalRates=executionRates(m.spans);
  const measuredActivity=rates.some(rate=>rate.completed||rate.failed);
  const useHistoricalRates=historicalRates.times.length&&!measuredActivity;
  const rateTimes=useHistoricalRates?historicalRates.times:times;
  const completedRates=useHistoricalRates?historicalRates.completedRate:rates.map(r=>r.completed/r.minutes);
  const failedRates=useHistoricalRates?historicalRates.failedRate:rates.map(r=>r.failed/r.minutes);
  chart('throughput',rateTimes,[{name:'completed/min',color:'#65dca9',values:completedRates},
    {name:'failed/min',color:'#ee868e',values:failedRates}]);
  chart('failure',rateTimes,[{name:'failure %',color:'#ee868e',values:(useHistoricalRates?
    historicalRates.completed.map((completed,index)=>({completed,failed:historicalRates.failed[index]})):rates).map(r=>{
     const total=r.completed+r.failed;return total?r.failed/total*100:0;
  })}],{unit:'%',maxValue:100});
  chart('saturation',times,[{name:'occupied %',values:samples.map(s=>{
    const nodes=s.workers||[],slots=nodes.reduce((n,w)=>n+(+w.capacity||0),0);
    return slots?nodes.reduce((n,w)=>n+(+w.load||0),0)/slots*100:0;
  })}],{unit:'%',maxValue:100});
  const phaseNames=['route','select','record','store','send'];
  chart('phases',phaseTimes,phaseNames.map((name,i)=>({name,values:phases.map(p=>p[name])})),{unit:'ms'});
  const phaseTotals=phaseNames.map(name=>phases.reduce((n,p)=>n+(p[name]||0),0)),phaseSum=phaseTotals.reduce((a,b)=>a+b,0);
  $('#phase-table-body').innerHTML=phaseNames.map((name,i)=>{
    const values=phases.map(p=>p[name]).filter(Number.isFinite);
    const share=phaseSum?phaseTotals[i]/phaseSum*100:0;
    return '<tr><th>'+name+'</th><td><div class="phase-share-cell"><div class="phase-mini-bar"><i style="width:'+share.toFixed(1)+'%"></i></div><span>'+
      (phaseSum?share.toFixed(1)+'%':'n/a')+'</span></div></td><td>'+esc(phaseMs(values.length?Math.min(...values):null))+
      '</td><td>'+esc(phaseMs(percentile(values,.5)))+'</td><td>'+esc(phaseMs(values.length?Math.max(...values):null))+'</td></tr>';
  }).join('');
  const operations=recent(m.operations), opTimes=operations.map(o=>o.at);
  chart('store',opTimes,[{name:'write',values:operations.map(o=>o.kind==='write'?o.ms:null)},
    {name:'read',color:'#65dca9',values:operations.map(o=>o.kind==='read'?o.ms:null)}],{unit:'ms'});
  const writeCount=m.operations.filter(o=>o.kind==='write').length;
  const readCount=m.operations.filter(o=>o.kind==='read').length;
  const coverage=$('#store-coverage');
  if (coverage) coverage.textContent=writeCount+' writes · '+readCount+' reads captured in the retained '+
    m.operations.length+'-operation window'+(readCount?'':'. No read sample is available to plot.');
  const ids=[...new Set(samples.flatMap(s=>(s.workers||[]).map(w=>w.id)))].slice(-6);
  chart('heartbeat',times,ids.map(id=>({name:id,values:samples.map(s=>{
    const v=(s.workers||[]).find(w=>w.id===id)?.heartbeat_rtt_ms;return v>=0?v:null;
  })})),{unit:'ms'});
  const workerSeries=ids.map(id=>({name:id,values:samples.map(s=>{
    const v=(s.workers||[]).find(w=>w.id===id)?.load;return v??null;
  })}));
  const historicalWorkers=workerOccupancy(m.spans);
  const measuredWorkerActivity=workerSeries.some(series=>series.values.some(value=>value>0));
  if (historicalWorkers.times.length&&!measuredWorkerActivity) {
    chart('workers',historicalWorkers.times,historicalWorkers.series,{unit:' slots'});
  } else chart('workers',times,workerSeries);
  chart('workers-analytics',times,workerSeries);
  chart('scaling',times,[
    {name:'workers',color:'#76bafa',values:samples.map(s=>(s.workers||[]).length)},
    {name:'used slots',color:'#e8c472',values:samples.map(s=>(s.workers||[]).reduce((n,w)=>n+(+w.load||0),0))},
    {name:'available slots',color:'#65dca9',values:samples.map(s=>(s.workers||[]).reduce((n,w)=>n+(+w.capacity||0),0))}],
    {markers:m.events.filter(e=>e.type==='SCALE_OUT'||e.type==='SCALE_IN').map(e=>({
      at:e.at,label:e.type+' · '+e.details,color:e.type==='SCALE_OUT'?'#e8c472':'#76bafa'}))});
  chart('queue-age',times,[{name:'oldest ready (s)',color:'#e8c472',
    values:samples.map(s=>s.oldest_ready_ms<0||s.oldest_ready_ms==null?null:s.oldest_ready_ms/1000)},
    {name:'oldest parked (s)',color:'#ee868e',
      values:samples.map(s=>s.oldest_parked_ms<0||s.oldest_parked_ms==null?null:s.oldest_parked_ms/1000)}],{unit:'s'});
  const spanWindow=m.spans.filter(s=>s.started_at>s.eligible_at).sort((a,b)=>a.started_at-b.started_at).slice(-120);
  const spanTimes=spanWindow.map(s=>s.started_at);
  const waitValues=spanWindow.map(s=>s.started_at-s.eligible_at);
  chart('queue-tail',spanTimes,[{name:'p50 (ms)',color:'#65dca9',
    values:waitValues.map((_,i)=>percentile(waitValues.slice(Math.max(0,i-19),i+1),.5))},
    {name:'p95 (ms)',color:'#e8c472',
      values:waitValues.map((_,i)=>percentile(waitValues.slice(Math.max(0,i-19),i+1),.95))}],{unit:'ms'});
  if (useHistoricalRates) {
    chart('backlog',historicalRates.times,[{name:'completed in interval',color:'#65dca9',
      values:historicalRates.completed}],{unit:' jobs'});
  } else chart('backlog',times,[{name:'ready jobs',color:'#e8c472',values:samples.map(s=>s.queue_depth)},
    {name:'completed since last sample',color:'#65dca9',values:rates.map(r=>r.completed)}],{unit:' jobs'});
  chart('capability-debt',times,[{name:'parked',color:'#ee868e',
    values:samples.map(s=>s.capability_wait)}]);
  chart('imbalance',times,[{name:'slot spread %',color:'#b39af7',values:samples.map(s=>{
    const load=(s.workers||[]).filter(w=>w.capacity>0).map(w=>w.load/w.capacity*100);
    return load.length?Math.max(...load)-Math.min(...load):null;
  })}],{unit:'%',maxValue:100});
  const retryRows=spanWindow.map((s,i)=>({at:s.started_at,retry:s.attempt>1?1:0,failed:s.status==='FAILED'?1:0}));
  chart('retry-pressure',retryRows.map(s=>s.at),[{name:'retry attempt',color:'#e8c472',
    values:retryRows.map((_,i)=>retryRows.slice(Math.max(0,i-9),i+1).reduce((n,r)=>n+r.retry,0))},
    {name:'failed attempt',color:'#ee868e',
      values:retryRows.map((_,i)=>retryRows.slice(Math.max(0,i-9),i+1).reduce((n,r)=>n+r.failed,0))}]);
}

function renderServices(m) {
  const services=m.board.services||[];
  $('#services-body').innerHTML=services.length?services.map(s=>'<tr><td>'+esc(s.id)+'</td>'+
    '<td>'+esc(s.host)+':'+esc(s.port)+'</td><td>'+esc(s.worker||'n/a')+'</td>'+
    '<td>'+esc(age(s.since))+'</td><td>'+esc(s.deploy_attempts||'n/a')+'</td><td class="ok">Registered</td></tr>').join('')
    :'<tr><td colspan="6" class="empty-row">No registered services. This roster reports master registration, not continuous TCP liveness.</td></tr>';
}
function renderScaling(m) {
  const events=[...m.events].reverse();
  $('#cluster-events').innerHTML=events.length?events.slice(0,30).map(e=>
    '<div class="event-item"><time>'+esc(new Date(e.at).toLocaleTimeString())+'</time>'+
    '<span class="event-type '+esc(e.type)+'">'+esc(e.type)+'</span><span>'+esc(e.details)+'</span></div>').join('')
    :'<div class="chart-empty">No scaling or worker events in this window</div>';
  const ordered=chronological(m.samples), fleet=ordered.map(sample=>(sample.workers||[]).length);
  const inferredUps=fleet.slice(1).filter((value,index)=>value>fleet[index]).length;
  const inferredDowns=fleet.slice(1).filter((value,index)=>value<fleet[index]).length;
  const eventUps=events.filter(e=>e.type==='SCALE_OUT').length;
  const eventDowns=events.filter(e=>e.type==='SCALE_IN').length;
  const slots=latest(ordered)?.workers?.reduce((sum,worker)=>sum+(+worker.capacity||0),0)||0;
  $('#scale-stats').textContent='↗ Scale-out '+Math.max(eventUps,inferredUps)+' · ↘ descale '+
    Math.max(eventDowns,inferredDowns)+' · current fleet '+m.workers.length+' workers / '+slots+' slots';
}

function renderAnalytics(data,m) {
  const since=Date.now()-(+$('#time-range').value||600000);
  const windowSpans=m.spans.filter(s=>s.started_at>=since);
  const terminal=windowSpans.filter(s=>s.ended_at>=since&&
    ['COMPLETED','FAILED','DEAD','CANCELLED'].includes(s.status)).sort((a,b)=>a.ended_at-b.ended_at);
  const completed=terminal.filter(s=>s.status==='COMPLETED');
  const failed=terminal.filter(s=>s.status!=='COMPLETED');
  const settled=terminal.length;
  const waits=windowSpans.map(s=>s.started_at-s.eligible_at).filter(v=>v>=0);
  const durations=windowSpans.filter(s=>s.ended_at>s.started_at).map(s=>s.ended_at-s.started_at);
  const slow=durations.filter(v=>v>5000).length;
  const historicalWorkers=workerOccupancy(windowSpans);
  $('#analytics-tiles').innerHTML=[
    ['Success rate',settled?(completed.length/settled*100).toFixed(1)+'%':'n/a',
      settled&&failed.length?'warn':settled?'ok':'neutral',completed.length+' / '+settled+' terminal attempts in range'],
    ['Completed',completed.length,'ok','within retained spans'],
    ['Failed / cancelled',failed.length,failed.length?'bad':'ok','within selected range'],
    ['Attempts measured',windowSpans.length,'neutral','selected range'],
    ['Slower than 5s',slow,'neutral',durations.length?((slow/durations.length)*100).toFixed(0)+'%':'n/a'],
    ['Queue p99',ms(percentile(waits,.99)),'neutral','eligibility → start'],
    ['Pipelines',new Set((data.runs||[]).map(runFamily)).size,
      'neutral','distinct DAGs in history']
  ].map(args=>tile(...args)).join('');
  const rolling=terminal.slice(-240);
  chart('success',rolling.map(s=>s.ended_at),[{name:'last 20 attempts completed %',color:'#65dca9',
    values:rolling.map((_,i)=>{const window=rolling.slice(Math.max(0,i-19),i+1);
      return window.filter(s=>s.status==='COMPLETED').length/window.length*100;})}],{unit:'%',maxValue:100});
  const boundaries=[100,250,500,1000,2500,5000,10000,30000,Infinity],labels=['<100ms','<250ms','<500ms','<1s','<2.5s','<5s','<10s','<30s','30s+'];
  const buckets=boundaries.map((edge,i)=>durations.filter(v=>v<edge&&(i===0||v>=boundaries[i-1])).length);
  barChart($('[data-chart="duration"]'),labels,buckets);
  const ordered=windowSpans.filter(s=>s.started_at>s.eligible_at).sort((a,b)=>a.started_at-b.started_at);
  const win=ordered.slice(-120),waitTimes=win.map(s=>s.started_at);
  const waitWindow=index=>win.slice(Math.max(0,index-19),index+1).map(s=>s.started_at-s.eligible_at);
  chart('wait',waitTimes,[{name:'p50',color:'#65dca9',values:win.map((_,i)=>percentile(waitWindow(i),.5))},
    {name:'p95',color:'#e8c472',values:win.map((_,i)=>percentile(waitWindow(i),.95))},
    {name:'p99',color:'#ee868e',values:win.map((_,i)=>percentile(waitWindow(i),.99))}],{unit:'ms'});
  chart('workers-analytics',historicalWorkers.times,historicalWorkers.series,{unit:' slots'});
  const contenders=windowSpans.filter(s=>s.started_at-s.eligible_at>=250);
  const priorities=[...new Set(contenders.map(s=>s.priority))].sort((a,b)=>a-b);
  const priority=priorities.map(value=>({value,waits:contenders.filter(s=>s.priority===value)
    .map(s=>s.started_at-s.eligible_at)}));
  barChart($('[data-chart="priority"]'),priority.map(p=>'#'+p.value),
    priority.map(p=>percentile(p.waits,.5)),'#e8c472',{unit:'ms'});
  $('#priority-verdict').textContent=priority.length?
    'Numeric priority, not an enum. Higher number runs first among eligible jobs. '+
    priority.map(p=>'#'+p.value+': '+p.waits.length+' contended, median '+ms(percentile(p.waits,.5))).join(' · '):
    'No dispatches waited at least 250 ms in this range.';
  const runRows=[];
  for(const run of data.runs||[]) {
    const ids=Array.isArray(run.jobs)?run.jobs:Object.keys(run.jobs||{});
    const nextRun=Math.min(...(data.runs||[]).filter(other=>other.name===run.name&&other.run_ts>run.run_ts)
      .map(other=>other.run_ts),Infinity);
    const runSpans=m.spans.filter(s=>ids.includes(s.job_id)&&s.started_at>=run.run_ts&&s.started_at<nextRun);
    const ended=runSpans.filter(s=>s.ended_at>0);
    if (!ids.length || !ids.every(id=>runSpans.some(s=>s.job_id===id&&s.ended_at>0&&
      ['COMPLETED','FAILED','DEAD','CANCELLED'].includes(s.status)))) continue;
    const duration=Math.max(...ended.map(s=>s.ended_at))-run.run_ts;
    if (duration>=0) {
      // Demo submissions receive unique IDs so their jobs never collide. For
      // run comparisons, show the stable scenario name across those IDs.
      const name=runFamily(run);
      runRows.push({name,run:run.run_ts,duration});
    }
  }
  runRows.sort((a,b)=>a.run-b.run);
  const lastRuns=runRows.slice(-80);
  const dagNames=[...new Set(runRows.map(r=>r.name))];
  const series=dagNames.map((name,index)=>({name,color:'hsl('+Math.round(index*360/Math.max(1,dagNames.length))+' 65% 68%)',
    values:lastRuns.map(r=>r.name===name?r.duration/1000:null)}));
  chart('pipelines',lastRuns.map(r=>r.run),series,{unit:'s',connectGaps:true});
  const groups=new Map();
  for (const run of runRows) {
    if (!groups.has(run.name)) groups.set(run.name,[]);
    groups.get(run.name).push(run);
  }
  $('#pipeline-table').innerHTML=groups.size?[...groups].sort((a,b)=>b[1].at(-1).run-a[1].at(-1).run)
    .map(([name,runs])=>{
      const latest=runs.at(-1),previous=runs.at(-2);
      const change=previous&&previous.duration>0?(latest.duration/previous.duration-1)*100:null;
      const trend=change===null?'First measured run':Math.abs(change)<5?'Stable':change<0?'Faster':'Slower';
      const tone=trend==='Faster'?'ok':trend==='Slower'?'bad':'neutral';
      return '<tr><td><i class="pipeline-swatch" style="background:'+series.find(s=>s.name===name)?.color+'"></i>'+esc(name)+'</td>'+
        '<td>'+runs.length+' run'+(runs.length===1?'':'s')+'</td><td>'+esc(ms(latest.duration))+'</td>'+
        '<td class="'+tone+'">'+trend+(change===null?'':' · '+Math.abs(change).toFixed(1)+'%')+'</td></tr>';
    }).join('')
    :'<tr><td class="empty-row">No completed manifest-backed runs in retained span history</td></tr>';
}

function render(data) {
  snapshot=data;
  const m=metricData(data);
  const connection=$('#cluster-connection');
  connection.textContent=data.online?'● Master online':'● Master unavailable';
  connection.className='connection '+(data.online?'online':'offline');
  const requestedMs=+$('#time-range').value||600000;
  const requestedSince=Date.now()-requestedMs;
  const missing=data.online&&!m.samples.length;
  const noWork=data.online&&m.samples.length&&!m.spans.length&&!m.phases.length;
  const metricStart=m.samples.length?m.samples[0].at:null;
  const partialHistory=metricStart&&metricStart-requestedSince>60000&&data.history?.included;
  $('#cluster-notice').hidden=!missing&&!noWork&&!partialHistory;
  if (missing) $('#cluster-notice').textContent='Master is online, but the observability RPC is unavailable or has not sampled yet. Restart the master with the updated engine to enable live Cluster data.';
  else if (noWork) $('#cluster-notice').textContent='No job execution exists in the selected history range. Health, heartbeat, and store telemetry may still be available. Choose a wider range or run a workload to populate execution charts.';
  else if (partialHistory) {
    const metricMinutes=Math.max(1,Math.round((Date.now()-metricStart)/60000));
    $('#cluster-notice').textContent='The selected '+Math.round(requestedMs/3600000)+'h window contains durable execution spans, but scheduler snapshots cover only the latest '+metricMinutes+' minutes since the master restarted. Throughput, failure, worker occupancy, queue wait, and retry charts are reconstructed from spans; queue composition, CPU, memory, heartbeat, dispatch-phase, store, and scaling charts show only retained master telemetry.';
  }
  $('#sample-age').textContent=latest(m.samples)?'Sample '+age(latest(m.samples).at)+' ago':'No telemetry sample';
  renderHealth(data,m);renderDiagnostics(m);renderQueue(m);renderBudget(m);renderTopology(data,m);
  renderCharts(m);renderServices(m);renderScaling(m);renderAnalytics(data,m);
}
async function refresh() {
  if (loading||document.hidden) return;
  loading=true;
  const since=Date.now()-(+$('#time-range').value||600000);
  try {
    const response=await fetch('/api/cluster?mode='+currentView+'&since='+since,{cache:'no-store'});
    if (!response.ok) throw new Error('HTTP '+response.status);
    render(await response.json());
  } catch(error) {
    $('#cluster-connection').textContent='● Cluster API unavailable';
    $('#cluster-connection').className='connection offline';
    $('#cluster-notice').hidden=false;
    $('#cluster-notice').textContent='Could not read cluster telemetry: '+error.message;
  } finally {loading=false;}
}
function schedule() {
  if (timer) clearInterval(timer);
  const interval=+$('#refresh-rate').value;
  if (interval) timer=setInterval(refresh,interval);
}
$$('[data-view]').forEach(button=>button.addEventListener('click',()=>{
  currentView=button.dataset.view;
  $$('[data-view]').forEach(tab=>{tab.classList.toggle('active',tab===button);
    tab.setAttribute('aria-selected',String(tab===button));});
  $('#live-view').hidden=currentView!=='live';
  $('#analytics-view').hidden=currentView!=='analytics';
  refresh();
}));
$$('.cluster-section-nav a').forEach(link=>link.addEventListener('click',event=>{
  event.preventDefault();
  $$('.cluster-section-nav a').forEach(item=>item.classList.toggle('active',item===link));
  const id=link.getAttribute('href').slice(1);
  const target=id==='analytics-view'?'analytics':'live';
  if (target!==currentView) document.querySelector('[data-view="'+target+'"]').click();
  requestAnimationFrame(()=>{document.getElementById(id)?.scrollIntoView({behavior:'smooth',block:'start'});
    history.replaceState(null,'','#'+id);});
}));
if ('IntersectionObserver' in window) {
  const sectionLinks=new Map($$('.cluster-section-nav a').map(link=>[
    link.getAttribute('href').slice(1),link
  ]));
  const visibleSections=new Map();
  const sectionObserver=new IntersectionObserver(entries=>{
    entries.forEach(entry=>visibleSections.set(entry.target.id,entry.isIntersecting?entry.intersectionRatio:0));
    const visible=[...visibleSections].filter(([,ratio])=>ratio>0).sort((a,b)=>b[1]-a[1])[0];
    if (!visible) return;
    const active=sectionLinks.get(visible[0]);
    if (!active) return;
    $$('.cluster-section-nav a').forEach(link=>link.classList.toggle('active',link===active));
  },{rootMargin:'-18% 0px -65% 0px',threshold:[0,.15,.4,.75]});
  sectionLinks.forEach((_,id)=>{const section=document.getElementById(id);if(section)sectionObserver.observe(section);});
}
$('#refresh-now').addEventListener('click',refresh);
$('#refresh-rate').addEventListener('change',schedule);
$('#time-range').addEventListener('change',refresh);
$('#download-json').addEventListener('click',()=>{
  if (!snapshot) return;
  const blob=new Blob([JSON.stringify(snapshot,null,2)],{type:'application/json'});
  const link=document.createElement('a');link.href=URL.createObjectURL(blob);
  link.download='titan-cluster-'+new Date().toISOString().slice(0,19).replace(/:/g,'-')+'.json';
  link.click();setTimeout(()=>URL.revokeObjectURL(link.href),1000);
});
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
mountChartInspector();mountDemoControls();schedule();refresh();
