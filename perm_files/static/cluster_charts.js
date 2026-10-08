/** Dependency-free SVG charts for bounded cluster telemetry. */
const W = 500, H = 165, LEFT = 49, RIGHT = 12, TOP = 15, BOTTOM = 37;
const palette = ['#76bafa', '#e8c472', '#65dca9', '#ee868e', '#ae9bf7', '#e58fbc'];
const safe = value => String(value).replace(/[&<>"]/g, character =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[character]));

export function empty(target, message = 'Awaiting measured telemetry') {
  target.innerHTML = '<div class="chart-empty">' + safe(message) + '</div>';
}
const clock = timestamp => new Date(timestamp).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});
const discrete = unit => /^( jobs| slots| attempts| count)$/.test(unit);
const ageLabel = timestamp => {
  const seconds=Math.max(0,Math.round((Date.now()-timestamp)/1000));
  return seconds<60?'-'+seconds+'s':seconds<3600?'-'+Math.round(seconds/60)+'m':'-'+(seconds/3600).toFixed(1)+'h';
};
const valueLabel = (value,unit) => {
  const suffix=discrete(unit)&&Math.round(value)===1?unit.slice(0,-1):unit;
  return (discrete(unit)||Number.isInteger(value)||value>=100?value.toFixed(0):value.toFixed(1))+suffix;
};

/** series: [{name, color?, values:[number|null]}], times: epoch milliseconds. */
export function lines(target, times, series, {unit = '', maxValue = null, markers = [], connectGaps = false} = {}) {
  if (!times?.length || !series.some(item => item.values.some(Number.isFinite))) return empty(target);
  const all = series.flatMap(item => item.values).filter(Number.isFinite);
  const ceiling = Math.max(1, maxValue ?? (discrete(unit)?Math.ceil(Math.max(...all)*1.12):Math.max(...all)*1.12));
  const floor = Math.min(0, Math.min(...all));
  const first=times[0],last=times.at(-1),timeSpan=Math.max(1,last-first);
  const x = index => LEFT + (times[index]-first) * (W - LEFT - RIGHT) / timeSpan;
  const timeX = timestamp => LEFT + (timestamp-first) * (W - LEFT - RIGHT) / timeSpan;
  const y = value => TOP + (ceiling - value) * (H - TOP - BOTTOM) / Math.max(1, ceiling - floor);
  const divisions=discrete(unit)?Math.min(3,Math.round(ceiling-floor)):3;
  const grid = Array.from({length:divisions+1},(_,n) => {
    const value = floor + (ceiling-floor) * (divisions - n) / divisions;
    const py = y(value).toFixed(1);
    return '<line class="gridline" x1="'+LEFT+'" x2="'+(W-RIGHT)+'" y1="'+py+'" y2="'+py+'"/>'+
      '<text class="chart-axis" x="'+(LEFT-5)+'" text-anchor="end" y="'+(+py+3)+'">'+safe(valueLabel(value,unit))+'</text>';
  }).join('');
  const paths = series.map((item, seriesIndex) => {
    const color = item.color || palette[seriesIndex % palette.length];
    let path = '', open = false;
    item.values.forEach((value, index) => {
      if (!Number.isFinite(value)) { if (!connectGaps) open = false; return; }
      path += (open ? 'L' : 'M') + x(index).toFixed(1) + ',' + y(value).toFixed(1);
      open = true;
    });
    const dots = connectGaps ? item.values.map((value,index) => Number.isFinite(value)
      ? '<circle cx="'+x(index).toFixed(1)+'" cy="'+y(value).toFixed(1)+'" r="3.2" fill="'+color+'"><title>'+safe(item.name+': '+valueLabel(value,unit))+'</title></circle>'
      : '').join('') : '';
    return '<path d="'+path+'" fill="none" stroke="'+color+'" stroke-width="2.1" vector-effect="non-scaling-stroke"/>'+dots;
  }).join('');
  const marks = markers.filter(marker=>marker.at>=times[0]&&marker.at<=times.at(-1)).map(marker=>{
    const px=timeX(marker.at).toFixed(1);
    const color=marker.color||'#e8c472';
    return '<line x1="'+px+'" x2="'+px+'" y1="'+TOP+'" y2="'+(H-BOTTOM)+'" stroke="'+
      color+'" stroke-width="1.5" stroke-dasharray="4 3"><title>'+
      safe(marker.label||'event')+'</title></line><text x="'+px+'" y="'+(TOP+9)+'" text-anchor="middle" '+
      'font-size="11" font-weight="700" fill="'+color+'">'+
      (marker.label?.startsWith('SCALE_IN')?'↘':'↗')+'</text>';
  }).join('');
  const ticks=first===last?[first]:[first,first+(last-first)/2,last];
  const xLabels=ticks.map((timestamp,index)=>{
    const px=timeX(timestamp).toFixed(1),anchor=index===0?'start':index===ticks.length-1?'end':'middle';
    return '<text class="chart-axis" x="'+px+'" y="'+(H-19)+'" text-anchor="'+anchor+'">'+
      safe(ageLabel(timestamp))+'</text><text class="chart-axis chart-clock" x="'+px+'" y="'+(H-5)+
      '" text-anchor="'+anchor+'">'+safe(clock(timestamp))+'</text>';
  }).join('');
  target.innerHTML = '<svg viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none" role="img" aria-label="'+
    safe(series.map(item=>item.name).join(', '))+' over time">'+grid+marks+paths+xLabels+'</svg>'+
    '<div class="chart-legend">'+series.map((item,index)=>'<span><i style="background:'+
      (item.color||palette[index%palette.length])+'"></i>'+safe(item.name)+'</span>').join('')+'</div>';
}

/** Stacked areas show both total demand and each queue lane's share. */
export function stackedAreas(target, times, series, {unit = ' jobs'} = {}) {
  if (!times?.length || !series.length) return empty(target);
  const totals=times.map((_,index)=>series.reduce((sum,item)=>sum+Math.max(0,Number(item.values[index])||0),0));
  const ceiling=Math.max(1,Math.ceil(Math.max(...totals)*1.12));
  const first=times[0],last=times.at(-1),span=Math.max(1,last-first);
  const x=index=>LEFT+(times[index]-first)*(W-LEFT-RIGHT)/span;
  const y=value=>TOP+(ceiling-value)*(H-TOP-BOTTOM)/ceiling;
  const divisions=Math.min(3,ceiling);
  const grid=Array.from({length:divisions+1},(_,index)=>{
    const value=ceiling*(divisions-index)/divisions,py=y(value).toFixed(1);
    return '<line class="gridline" x1="'+LEFT+'" x2="'+(W-RIGHT)+'" y1="'+py+'" y2="'+py+'"/>'+
      '<text class="chart-axis" x="'+(LEFT-5)+'" y="'+(+py+3)+'" text-anchor="end">'+safe(valueLabel(value,unit))+'</text>';
  }).join('');
  const base=times.map(()=>0);
  const areas=series.map((item,index)=>{
    const color=item.color||palette[index%palette.length];
    const top=base.map((value,i)=>value+Math.max(0,Number(item.values[i])||0));
    const upper=top.map((value,i)=>(i?'L':'M')+x(i).toFixed(1)+','+y(value).toFixed(1)).join('');
    const lower=base.map((value,i)=>'L'+x(i).toFixed(1)+','+y(value).toFixed(1)).reverse().join('');
    top.forEach((value,i)=>{base[i]=value;});
    return '<path d="'+upper+lower+'Z" fill="'+color+'" fill-opacity=".72" stroke="'+color+'" stroke-width="1.2"/>';
  }).join('');
  const ticks=first===last?[first]:[first,first+(last-first)/2,last];
  const labels=ticks.map((timestamp,index)=>{
    const px=LEFT+(timestamp-first)*(W-LEFT-RIGHT)/span;
    const anchor=index===0?'start':index===ticks.length-1?'end':'middle';
    return '<text class="chart-axis" x="'+px+'" y="'+(H-19)+'" text-anchor="'+anchor+'">'+
      safe(ageLabel(timestamp))+'</text><text class="chart-axis chart-clock" x="'+px+'" y="'+(H-5)+
      '" text-anchor="'+anchor+'">'+safe(clock(timestamp))+'</text>';
  }).join('');
  target.innerHTML='<svg viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none" role="img" aria-label="Stacked queue composition">'+
    grid+areas+labels+'</svg><div class="chart-legend">'+series.map((item,index)=>'<span><i style="background:'+
    (item.color||palette[index%palette.length])+'"></i>'+safe(item.name)+'</span>').join('')+'</div>';
}

export function bars(target, labels, values, color = palette[0], {unit = ' jobs'} = {}) {
  if (!values?.length || !values.some(Number.isFinite)) return empty(target);
  const highest = discrete(unit)?Math.max(1,Math.ceil(Math.max(...values.filter(Number.isFinite))*1.12)):
    Math.max(1, ...values.filter(Number.isFinite)) * 1.12;
  const span = W - LEFT - RIGHT, bar = span / values.length;
  const divisions=discrete(unit)?Math.min(3,highest):3;
  const grid=Array.from({length:divisions+1},(_,i)=>{const value=highest*(divisions-i)/divisions,
    y=TOP+i*(H-TOP-BOTTOM)/divisions;
    return '<line class="gridline" x1="'+LEFT+'" x2="'+(W-RIGHT)+'" y1="'+y+'" y2="'+y+'"/>'+
      '<text class="chart-axis" x="'+(LEFT-5)+'" y="'+(y+3)+'" text-anchor="end">'+safe(valueLabel(value,unit))+'</text>';
  }).join('');
  target.innerHTML = '<svg viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none" role="img" aria-label="Distribution">'+
    grid+
    labels.map((label,index) => {
      const height = Number.isFinite(values[index])?(values[index] / highest) * (H-TOP-BOTTOM):0;
      const x = LEFT + index*bar + 3, y = H-BOTTOM-height;
      return '<rect x="'+x+'" y="'+y+'" width="'+Math.max(2,bar-6)+'" height="'+height+
        '" fill="'+color+'"><title>'+safe(label+': '+(values[index]??'n/a')+unit)+'</title></rect>'+
        '<text class="chart-axis" x="'+(x+(bar-6)/2)+'" y="'+(H-8)+'" text-anchor="middle">'+safe(label)+'</text>';
    }).join('')+'</svg>';
}

export function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((a,b)=>a-b);
  return sorted[Math.min(sorted.length-1, Math.max(0, Math.ceil(fraction*sorted.length)-1))];
}

/** Reconstruct historical concurrent slots per worker from persisted execution spans. */
export function workerOccupancy(spans, limit = 240) {
  const events = [];
  const workers = new Set();
  for (const span of spans || []) {
    const worker = String(span?.worker || '');
    const start = Number(span?.started_at), end = Number(span?.ended_at);
    if (!worker || !Number.isFinite(start) || !Number.isFinite(end) || end < start) continue;
    workers.add(worker);
    events.push({at:start,worker,delta:1},{at:end,worker,delta:-1});
  }
  events.sort((a,b)=>a.at-b.at||a.delta-b.delta);
  const names=[...workers].sort(), active=new Map(names.map(name=>[name,0])), points=[];
  for (let index=0;index<events.length;) {
    const at=events[index].at;
    while(index<events.length&&events[index].at===at) {
      const event=events[index++];
      active.set(event.worker,Math.max(0,(active.get(event.worker)||0)+event.delta));
    }
    points.push({at,loads:Object.fromEntries(active)});
  }
  const bounded=points.length<=limit?points:Array.from({length:limit},(_,index)=>
    points[Math.round(index*(points.length-1)/(limit-1))]);
  return {times:bounded.map(point=>point.at),series:names.map((name,index)=>({
    name,color:palette[index%palette.length],values:bounded.map(point=>point.loads[name]||0)
  }))};
}

/** Aggregate terminal spans into adaptive time buckets for historical throughput. */
export function executionRates(spans, limit = 240) {
  const terminal=(spans||[]).filter(span=>Number.isFinite(Number(span?.ended_at))&&
    Number(span.ended_at)>0&&['COMPLETED','FAILED','DEAD','CANCELLED'].includes(span.status))
    .sort((a,b)=>Number(a.ended_at)-Number(b.ended_at));
  if (!terminal.length) return {times:[],completed:[],failed:[],completedRate:[],failedRate:[]};
  const first=Number(terminal[0].ended_at),last=Number(terminal.at(-1).ended_at);
  const width=Math.max(1000,Math.ceil(Math.max(1,last-first)/Math.max(1,limit-1)));
  const origin=Math.floor(first/width)*width;
  const buckets=new Map();
  for (const span of terminal) {
    const at=origin+Math.floor((Number(span.ended_at)-origin)/width)*width;
    const bucket=buckets.get(at)||{completed:0,failed:0};
    if (span.status==='COMPLETED') bucket.completed++;
    else bucket.failed++;
    buckets.set(at,bucket);
  }
  const times=[...buckets.keys()].sort((a,b)=>a-b),minutes=width/60000;
  const completed=times.map(at=>buckets.get(at).completed);
  const failed=times.map(at=>buckets.get(at).failed);
  return {times,completed,failed,
    completedRate:completed.map(value=>value/minutes),
    failedRate:failed.map(value=>value/minutes)};
}
