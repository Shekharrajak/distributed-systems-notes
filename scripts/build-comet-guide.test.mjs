import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {operatorCounts} from './build-comet-guide.mjs';
import {isSiteFile} from './build-site.mjs';
import {diagrams} from './comet-diagrams.mjs';
import {chapters,queries} from './comet-content.mjs';
import {evidence} from './comet-evidence.mjs';
const root=new URL('../',import.meta.url);
const read=file=>fs.readFileSync(new URL(file,root),'utf8');
test('atlas preserves 88 exact plans, snapshot strings and planning-only scope',()=>{
 const m=JSON.parse(read('comet-plans/manifest.json'));
 assert.equal(m.captures.length,88);assert.equal(m.queryResultsExecuted,false);
 for(const s of Object.values(m.snapshots))assert.equal(typeof s.id,'string');
 for(let q=1;q<=22;q++){
  const records=m.captures.filter(p=>p.query===q);assert.equal(records.length,4);
  assert.equal(new Set(records.map(p=>p.engine+p.mode)).size,4);
 }
 for(const p of m.captures){assert.equal(p.status,'planned');assert.deepEqual(p.jobIdsDuringPlanning,[]);const text=read('comet-plans/'+p.file);assert.equal(createHash('sha256').update(text).digest('hex'),p.sha256);assert.deepEqual(operatorCounts(text),p.operators);assert(isSiteFile('comet-plans/'+p.file));}
 assert(read('comet-plans/no-broadcast-q16-comet.plan.txt').includes('CometBroadcastHashJoin'));
});
test('every chapter reference and diagram is known, unique and used',()=>{
 const ids=new Set(evidence.map(e=>e.id)),used=new Set(),seen=[];
 const pages=chapters({section:(id,title,html)=>({id,title,html}),table:()=>'',code:()=>'',details:()=>'',refs:(...refs)=>{for(const id of refs){assert(ids.has(id),id);used.add(id);}return '';},figure:id=>{seen.push('comet-deep-'+id);return '';}});
 for(const p of pages)assert.equal(new Set(p.sections.map(s=>s.id)).size,p.sections.length);
 assert.equal(used.size,evidence.length);assert.equal(new Set(seen).size,12);assert.deepEqual(new Set(seen),new Set(diagrams.map(d=>d.id)));assert.equal(queries.length,22);
});
test('diagrams use grouped native syntax, muted themes and curved flowcharts',()=>{
 for(const d of diagrams){assert(d.source.startsWith('%%{init:'));assert(d.source.includes('Ubuntu'));if(d.source.includes('flowchart ')){assert(d.source.includes('"curve":"basis"'));assert(d.source.includes('subgraph'));}else{assert(d.source.includes('sequenceDiagram'));assert(d.source.includes('box rgb'));}}
});
test('publisher includes curated plan artifacts but excludes logs and authoring code',()=>{
 for(const f of ['comet-guide-validation.json','comet-plans/manifest.json','comet-plans/q22.sql'])assert(isSiteFile(f));
 for(const f of ['comet-plans/comet.log','comet-plans/q23.sql','scripts/capture-comet-plans.py','comet-plans/secrets.json'])assert(!isSiteFile(f));
});
