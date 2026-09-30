import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chapters} from './kafka-recovery-content.mjs';
import {diagrams} from './kafka-recovery-diagrams.mjs';
import {evidence} from './kafka-recovery-evidence.mjs';
import {table,code} from './build-kafka-recovery.mjs';
import {isSiteFile} from './build-site.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const figures=[],references=[];
const pages=chapters({section:(id,title,html)=>({id,title,html}),table,code,figure:id=>{figures.push(id);return '';},refs:(...ids)=>{references.push(...ids);return '';},details:(title,html)=>html});
test('three chapters use unique sections and every diagram exactly once',()=>{
 assert.equal(pages.length,3);
 assert.equal(new Set(pages.map(p=>p.file)).size,pages.length);
 for(const p of pages)assert.equal(new Set(p.sections.map(s=>s.id)).size,p.sections.length);
 assert.equal(new Set(figures).size,figures.length);
 assert.deepEqual([...figures].sort(),diagrams.map(d=>d.id).sort());
});
test('all claims reference known uniquely identified evidence',()=>{
 const ids=new Set(evidence.map(e=>e.id));assert.equal(ids.size,evidence.length);
 for(const id of references)assert(ids.has(id),id);
 assert(evidence.filter(e=>e.file.includes('/test/')).length>=10);
});
test('diagrams use loaded-font theme and valid type-specific grouping',()=>{
 for(const d of diagrams){
  assert(d.source.startsWith('%%{init: '));assert(d.source.includes('Ubuntu, Arial, sans-serif'));
  if(d.source.includes('flowchart TB')||d.source.includes('flowchart LR')){assert(d.source.includes('"curve":"basis"'));assert(d.source.includes('subgraph'));}
  else{assert(d.source.includes('sequenceDiagram'));assert(d.source.includes('box rgb('));assert(!d.source.includes('classDef'));assert(!/^[^\n]*->>[^\n]*;/m.test(d.source));}
 }
});
test('code and table content is escaped',()=>{
 assert(code('<script>').includes('&lt;script&gt;'));assert(!table(['<x>'],[['&']]).includes('<x>'));
});
test('publisher includes recovery assets but not authoring scripts',()=>{
 assert(isSiteFile('kafka-recovery-validation.json'));
 for(const p of pages)assert(isSiteFile(p.file));
 assert(!isSiteFile('scripts/kafka-recovery-content.mjs'));
});
test('generated evidence is pinned and scope is explicit',()=>{
 const report=JSON.parse(fs.readFileSync(path.join(root,'kafka-recovery-validation.json'),'utf8'));
 assert.equal(report.evidence.length,evidence.length);assert.equal(report.kafkaTestsRun,false);assert.equal(report.liveFaultInjectionRun,false);
 assert(report.scopeChecks.transactionalShareApiAbsent);assert(report.scopeChecks.txPendingAbsent);
 for(const e of report.evidence){assert(e.line>0);assert(e.url.includes(`/blob/${report.sourceRevision}/`));assert(e.url.endsWith(`#L${e.line}`));}
 for(const p of pages){
  const html=fs.readFileSync(path.join(root,p.file),'utf8');
  for(const other of pages)assert(html.includes(`data-nav="${other.file}"`));
  assert(html.includes('Kafka engine tests and live fault injection were not run'));
  const ids=[...html.matchAll(/\bid="([^"]+)"/g)].map(m=>m[1]);assert.equal(new Set(ids).size,ids.length);
 }
});
