import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const source = readFileSync(new URL('../static/cluster_charts.js', import.meta.url), 'utf8');
const {lines, bars, stackedAreas, workerOccupancy, executionRates} = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const now = Date.now();
const line = {innerHTML: ''};
lines(line, [now - 10000, now - 5000, now], [{name:'ready',values:[0,2,4]}], {unit:' jobs'});
assert.match(line.innerHTML, /-10s/);
assert.match(line.innerHTML, /-5s/);
assert.match(line.innerHTML, /\d{2}:\d{2}:\d{2}/);
assert.match(line.innerHTML, /jobs/);
const sparse = {innerHTML: ''};
lines(sparse, [now - 1000, now], [{name:'ready',values:[0,1]}], {unit:' jobs'});
assert.equal((sparse.innerHTML.match(/>1 job</g) || []).length, 1);
assert.equal((sparse.innerHTML.match(/>0 jobs</g) || []).length, 1);

const bar = {innerHTML: ''};
bars(bar, ['#1','#2'], [4,8], '#e8c472', {unit:'ms'});
assert.match(bar.innerHTML, /#1/);
assert.match(bar.innerHTML, /ms/);
assert.match(bar.innerHTML, /<line class="gridline"/);
const stacked = {innerHTML: ''};
stackedAreas(stacked, [now - 1000, now], [
  {name:'ready',color:'#76bafa',values:[2,3]},
  {name:'running',color:'#e8c472',values:[1,2]}
]);
assert.match(stacked.innerHTML, /Stacked queue composition/);
assert.match(stacked.innerHTML, /ready/);
assert.match(stacked.innerHTML, /running/);
assert.equal((stacked.innerHTML.match(/fill-opacity="\.72"/g) || []).length, 2);
const occupancy=workerOccupancy([
  {worker:'worker-a',started_at:100,ended_at:300},
  {worker:'worker-a',started_at:150,ended_at:250},
  {worker:'worker-b',started_at:200,ended_at:400}
]);
assert.deepEqual(occupancy.times,[100,150,200,250,300,400]);
assert.deepEqual(occupancy.series.find(series=>series.name==='worker-a').values,[1,2,2,1,0,0]);
assert.deepEqual(occupancy.series.find(series=>series.name==='worker-b').values,[0,0,1,1,1,0]);
const rates=executionRates([
  {ended_at:1000,status:'COMPLETED'},
  {ended_at:1500,status:'FAILED'},
  {ended_at:2500,status:'COMPLETED'},
  {ended_at:3000,status:'RUNNING'}
]);
assert.deepEqual(rates.times,[1000,2000]);
assert.deepEqual(rates.completed,[1,1]);
assert.deepEqual(rates.failed,[1,0]);
assert.deepEqual(rates.completedRate,[60,60]);
console.log('PASS cluster chart time, category, and value axes');
