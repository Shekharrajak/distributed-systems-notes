import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chapter} from './kafka-offset-content.mjs';
import {diagrams} from './kafka-offset-diagrams.mjs';
import {evidence} from './kafka-offset-evidence.mjs';
import {table,code} from './build-kafka-recovery.mjs';
import {isSiteFile} from './build-site.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const figures=[],references=[];
const page=chapter({section:(id,title,html)=>({id,title,html}),table,code,figure:id=>{figures.push(id);return '';},refs:(...ids)=>{references.push(...ids);return '';},details:(title,html)=>html});
test('offset chapter covers nine distinct diagrams and unique sections',()=>{
 assert.equal(page.number,24);assert.equal(diagrams.length,9);
 assert.equal(new Set(page.sections.map(s=>s.id)).size,page.sections.length);
 assert.equal(new Set(figures).size,figures.length);
 assert.deepEqual([...figures].sort(),diagrams.map(d=>d.id).sort());
});
test('all offset claims link to known evidence across three pinned repositories',()=>{
 const ids=new Set(evidence.map(e=>e.id));assert.equal(ids.size,evidence.length);
 for(const id of references)assert(ids.has(id),id);
 assert.deepEqual([...new Set(evidence.map(e=>e.repo))].sort(),['Kafka','flink-connector-kafka','spark']);
 assert(evidence.filter(e=>e.file.includes('/test/')).length>=8);
});
test('offset diagrams have valid type-specific grouping and loaded-font themes',()=>{
 for(const d of diagrams){
  assert(d.source.startsWith('%%{init: '));assert(d.source.includes('Ubuntu, Arial, sans-serif'));
  if(d.source.includes('flowchart TB')){assert(d.source.includes('"curve":"basis"'));assert(d.source.includes('subgraph'));}
  else{assert(d.source.includes('sequenceDiagram'));assert(d.source.includes('box rgb('));assert(!d.source.includes('classDef'));assert(!/^[^\n]*->>[^\n]*;/m.test(d.source));}
 }
});
test('offset evidence preserves engine test and experiment validation gaps',()=>{
 const report=JSON.parse(fs.readFileSync(path.join(root,'kafka-offset-validation.json'),'utf8'));
 assert.equal(report.evidence.length,evidence.length);
 assert.equal(report.engineTestsRun,false);assert.equal(report.snippetsRun,false);assert.equal(report.liveFaultInjectionRun,false);
 for(const e of report.evidence){assert(e.line>0);assert.equal(e.revision,report.sourceRevisions[e.repo]);assert(e.url.includes(`/apache/${e.repo.toLowerCase()}/blob/${e.revision}/`));assert(e.url.endsWith(`#L${e.line}`));}
});
test('offset chapter is published and navigation is consistent',()=>{
 assert(isSiteFile(page.file));assert(isSiteFile('kafka-offset-validation.json'));assert(!isSiteFile('scripts/kafka-offset-content.mjs'));
 const html=fs.readFileSync(path.join(root,page.file),'utf8');
 const ids=[...html.matchAll(/\bid="([^"]+)"/g)].map(m=>m[1]);assert.equal(ids.length,new Set(ids).size);
 for(const file of fs.readdirSync(root).filter(f=>f.endsWith('.html'))){
  const content=fs.readFileSync(path.join(root,file),'utf8');
  if(content.includes('aria-label="Chapters"'))assert(content.includes(`data-nav="${page.file}"`),file);
 }
});
test('offset narrative keeps the key API and recovery distinctions',()=>{
 const content=page.sections.map(s=>s.html).join('\n').replace(/<[^>]*>/g,'');
 for(const phrase of ['neither for source progress','Only the most recently supplied callback','does not invalidate the completed Flink checkpoint','RealTimeTrigger','consumer group protocol','not a compare-and-set'])assert(content.includes(phrase),phrase);
});
