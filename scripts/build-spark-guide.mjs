import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {esc} from './build-kafka-guide.mjs';
import {readEvidence,table,code} from './build-kafka-recovery.mjs';
import {evidence} from './spark-guide-evidence.mjs';
import {diagrams} from './spark-guide-diagrams.mjs';
import {chapters} from './spark-guide-content.mjs';

export function buildSparkGuide(root,source){
 const revision=JSON.parse(fs.readFileSync(path.join(root,'validation.json'),'utf8')).sourceRevisions.spark;
 if(!/^[a-f0-9]{40}$/.test(revision||''))throw Error('Missing pinned Spark revision');
 const records=readEvidence(source,revision,evidence).map(r=>({...r,revision,url:r.url.replace('/apache/kafka/','/apache/spark/')}));
 const byId=new Map(records.map(r=>[r.id,r]));
 const refs=(...ids)=>`<div class="refs">Source: ${ids.map(id=>{if(!byId.has(id))throw Error(`Unknown evidence ${id}`);return `<a class="cite" href="#ev-${id}">${esc(id)}</a>`;}).join(' ')}</div>`;
 const figure=id=>{
  const d=diagrams.find(d=>d.id===id);if(!d)throw Error(`Unknown diagram ${id}`);
  return `<figure class="diagram" id="${id}"><figcaption><h3>${esc(d.title)}</h3><p>${esc(d.caption)}</p></figcaption><div class="diagram-tools"><button data-zoom="in" aria-label="Zoom in diagram">Zoom +</button><button data-zoom="out" aria-label="Zoom out diagram">Zoom −</button><button data-zoom="fit">Fit width</button><button data-zoom="actual">Actual size</button><a href="diagrams/${id}.svg" target="_blank">Full-size SVG</a><a href="diagrams/${id}.mmd">Mermaid source</a><span class="diagram-hint">Scroll within the diagram or open full size.</span></div><div class="diagram-scroll" tabindex="0" aria-label="Scrollable diagram: ${esc(d.title)}"><img src="diagrams/${id}.svg" alt="${esc(d.title+'. '+d.caption)}" loading="lazy"></div></figure>`;
 };
 const pages=chapters({section:(id,title,html)=>({id,title,html}),table,code,refs,figure,details:(title,html)=>`<details class="qa"><summary>${esc(title)}</summary>${html}</details>`});
 const navPattern=/<nav aria-label="Chapters">([\s\S]*?)<\/nav>/;
 let nav=fs.readFileSync(path.join(root,'index.html'),'utf8').match(navPattern)?.[1];if(!nav)throw Error('Missing chapter navigation');
 for(const p of pages)if(!nav.includes(`data-nav="${p.file}"`))nav+=`<a href="${p.file}" data-nav="${p.file}"><span>${p.number}</span>${esc(p.title)}</a>`;
 for(const p of pages){
  const used=new Set([...p.sections.map(s=>s.html).join('\n').matchAll(/href="#ev-([^"]+)"/g)].map(m=>m[1]));
  const sourceHtml=`<p>Pinned tracked Apache Spark source: <a href="https://github.com/apache/spark/tree/${revision}">${revision}</a>. Untracked experimental share-group files in the supplied checkout are outside this chapter’s scope.</p><p>Source and test bodies were inspected. Spark engine tests, executable snippets, performance benchmarks and live multi-executor fault injection were not run. Placements, offsets, directory trees and failure timings are illustrative. Apache source excerpts are licensed under <a href="https://www.apache.org/licenses/LICENSE-2.0">Apache License 2.0</a>.</p>${records.filter(r=>used.has(r.id)).map(r=>`<details class="code" id="ev-${r.id}"><summary>${esc(r.id)} · ${esc(r.file.split('/').at(-1))}:${r.line}</summary><p>${esc(r.claim)}</p><p class="path"><a href="${r.url}">${esc('spark/'+r.file)}</a></p>${code(r.excerpt)}</details>`).join('\n')}<p>Published supporting references: <a href="https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html">Spark Kafka integration</a>, <a href="https://spark.apache.org/docs/4.0.2/streaming/apis-on-dataframes-and-datasets.html">Structured Streaming APIs and guarantees</a>, and <a href="https://spark.apache.org/docs/4.0.1/streaming/performance-tips.html">performance guidance</a>. These releases differ from the development pin; pinned code is authoritative for method names and timing here.</p><p><a href="spark-guide-validation.json">Pinned evidence manifest and rendered artifact checks</a></p>`;
  const sections=[...p.sections,{id:'evidence',title:'Pinned code, inspected tests and validation scope',html:sourceHtml}];
  const other=pages.find(q=>q.file!==p.file);
  const html=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(p.title)} · Distributed systems field guide</title><link rel="stylesheet" href="styles.css"><link rel="stylesheet" href="kafka-rpc-atlas.css"><script defer src="app.js"></script></head><body data-page="${p.file}"><a class="skip" href="#main">Skip to content</a><header><a href="index.html">Distributed systems / source to sink</a><span>Spark · executors · checkpoint · replay</span></header><div class="layout"><aside class="sidebar"><label for="chapter-filter">Find a chapter</label><input id="chapter-filter" type="search" placeholder="Spark, state, recovery…"><nav aria-label="Chapters">${nav}</nav></aside><main id="main"><div class="hero"><p class="eyebrow">Chapter ${p.number} · Source-backed Spark deep dive</p><h1>${esc(p.title)}</h1><p class="intro">${esc(p.intro)}</p><p class="guide-footer-note">Start with the process map, follow batch 42, then expand the pinned methods and tests.</p><nav class="toc" aria-label="On this page">${sections.map(s=>`<a href="#${s.id}">${esc(s.title)}</a>`).join('')}</nav></div>${sections.map(s=>`<section id="${s.id}"><h2>${esc(s.title)}</h2>${s.html}</section>`).join('\n')}<footer><a href="index.html">Field guide home</a><a href="${other.file}">${esc(other.title)}</a><a href="kafka-offset-commits-flink-spark.html">Kafka offset commits compared</a></footer></main></div></body></html>\n`;
  fs.writeFileSync(path.join(root,p.file),html);
 }
 for(const file of fs.readdirSync(root).filter(f=>f.endsWith('.html'))){
  const dest=path.join(root,file),html=fs.readFileSync(dest,'utf8');
  if(navPattern.test(html))fs.writeFileSync(dest,html.replace(navPattern,()=>`<nav aria-label="Chapters">${nav}</nav>`));
 }
 const landing=path.join(root,'index.html'),index=fs.readFileSync(landing,'utf8');
 const entry=`<!-- spark-deep-entry --><section id="spark-execution-recovery"><h2>Spark across multiple executors: execution and recovery</h2><p><a href="spark-distributed-execution.html">Follow the driver, Kafka readers, task threads, RPCs and shuffle</a>, then <a href="spark-checkpoint-failure-recovery.html">trace checkpoint logs, state versions and failure recovery</a>. Thirteen diagrams distinguish input replay from exactly-once sink effects, with pinned implementation and test excerpts.</p></section><!-- /spark-deep-entry -->`;
 fs.writeFileSync(landing,index.includes('<!-- spark-deep-entry -->')?index.replace(/<!-- spark-deep-entry -->[\s\S]*?<!-- \/spark-deep-entry -->/,()=>entry):index.replace('<section',entry+'<section'));
 for(const d of diagrams)fs.writeFileSync(path.join(root,'diagrams',d.id+'.mmd'),d.source);
 const rawPath=path.join(root,'diagrams.md'),raw=fs.readFileSync(rawPath,'utf8');
 const block='<!-- spark-deep-diagrams -->\n\n# Spark distributed execution and recovery\n\n'+diagrams.map(d=>`## ${d.title}\n\n${d.caption}\n\n\`\`\`mermaid\n${d.source}\`\`\`\n`).join('\n')+'<!-- /spark-deep-diagrams -->';
 fs.writeFileSync(rawPath,raw.includes('<!-- spark-deep-diagrams -->')?raw.replace(/<!-- spark-deep-diagrams -->[\s\S]*?<!-- \/spark-deep-diagrams -->/,()=>block):raw+'\n'+block+'\n');
 const validation={sourceRevisions:{spark:revision},sourceInspectionOnly:true,engineTestsRun:false,snippetsRun:false,liveFaultInjectionRun:false,pages:pages.map(p=>p.file),diagrams:diagrams.map(d=>d.id),evidence:records.map(({excerpt,...r})=>r)};
 fs.writeFileSync(path.join(root,'spark-guide-validation.json'),JSON.stringify(validation,null,2)+'\n');
 return {pages:pages.length,diagrams:diagrams.length,evidence:records.length};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))console.log(buildSparkGuide(path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),path.resolve(process.argv[2]||'/Users/srajak/Documents/repos/oss/apache/spark')));
