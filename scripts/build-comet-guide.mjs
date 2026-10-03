import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {esc} from './build-kafka-guide.mjs';
import {table,code} from './build-kafka-recovery.mjs';
import {diagrams} from './comet-diagrams.mjs';
import {chapters,queries} from './comet-content.mjs';
import {evidence} from './comet-evidence.mjs';

const pins={comet:'184accac5b9cee6b761a6673c73c263adedef45e',spark:'f9358a5587a2d512c5cf08ba4b10d60007c93f6e'};
const publicComet='786bbc8fd630abfd0b29bdaffea6168f9f228863';
export function operatorCounts(plan){
 const counts={};
 for(const [,raw] of plan.matchAll(/^\(\d+\) (.+)$/gm)){
  const name=raw.replace(/ \[.*$/,'');counts[name]=(counts[name]||0)+1;
 }
 return counts;
}
export function buildCometGuide(root,capture,repositories){
 const hash=value=>createHash('sha256').update(value).digest('hex');
 const records=evidence.map(e=>{
  const repo=repositories[e.repo],revision=pins[e.repo];
  const text=execFileSync('git',['show',`${revision}:${e.file}`],{cwd:repo,encoding:'utf8',maxBuffer:10e6});
  const at=text.indexOf(e.needle);if(at<0)throw Error(`Missing source anchor ${e.id}: ${e.needle}`);
  const line=text.slice(0,at).split('\n').length;
  let url=null;
  if(e.repo==='spark')url=`https://github.com/apache/spark/blob/${revision}/${e.file}#L${line}`;
  else {
   try{const published=execFileSync('git',['show',`${publicComet}:${e.file}`],{cwd:repo,encoding:'utf8',stdio:['ignore','pipe','ignore'],maxBuffer:10e6});
    if(published===text)url=`https://github.com/apache/datafusion-comet/blob/${publicComet}/${e.file}#L${line}`;
   }catch{}
  }
  return {...e,revision,line,url,fileSha256:hash(text),excerpt:text.split('\n').slice(line-1,line-1+e.count).map((v,i)=>`${line+i}  ${v}`.trimEnd()).join('\n')};
 });
 const byId=new Map(records.map(r=>[r.id,r]));
 const refs=(...ids)=>`<div class="refs">Pinned source: ${ids.map(id=>{if(!byId.has(id))throw Error(id);return `<a href="#ev-${id}">${esc(id)}</a>`;}).join(' · ')}</div>`;
 const figure=id=>{const item=diagrams.find(d=>d.id==='comet-deep-'+id);if(!item)throw Error(id);
 return `<figure class="diagram" id="${item.id}"><figcaption><h3>${esc(item.title)}</h3><p>${esc(item.caption)}</p></figcaption><div class="diagram-tools"><button data-zoom="in" aria-label="Zoom in diagram">Zoom +</button><button data-zoom="out" aria-label="Zoom out diagram">Zoom −</button><button data-zoom="fit">Fit width</button><button data-zoom="actual">Actual size</button><a href="diagrams/${item.id}.svg">Full-size SVG</a><a href="diagrams/${item.id}.mmd">Mermaid source</a></div><div class="diagram-scroll" tabindex="0"><img src="diagrams/${item.id}.svg" alt="${esc(item.title+'. '+item.caption)}" loading="lazy"></div></figure>`;};
 const section=(id,title,html)=>({id,title,html});
 const pages=chapters({section,table,code,refs,figure,details:(title,html)=>`<details><summary>${esc(title)}</summary>${html}</details>`});
 const out=path.join(root,'comet-plans');fs.mkdirSync(out,{recursive:true});
 const captures=['jvm','comet'].map(engine=>JSON.parse(fs.readFileSync(path.join(capture,`${engine}-capture.json`),'utf8')));
 const runtime=JSON.parse(fs.readFileSync(path.join(capture,'runtime.json'),'utf8'));
 const plans=captures.flatMap(c=>c.records.map(p=>{
  if(p.status!=='planned'||p.jobIdsDuringPlanning.length)throw Error(`Invalid planning record: ${p.key} ${p.status}`);
  const content=fs.readFileSync(path.join(capture,p.file),'utf8');if(hash(content)!==p.sha256)throw Error(`Hash mismatch: ${p.file}`);
  fs.copyFileSync(path.join(capture,p.file),path.join(out,p.file));return {...p,operators:operatorCounts(content),content};
 }));
 if(plans.length!==88)throw Error('Expected 88 plans');
 for(let q=1;q<=22;q++)fs.copyFileSync(path.join(capture,`q${q}.sql`),path.join(out,`q${q}.sql`));
 if(JSON.stringify(captures[0].snapshots)!==JSON.stringify(captures[1].snapshots))throw Error('Engine snapshot mismatch');
 const launchSettings={scope:'Explicit launcher settings, not an exhaustive dump of effective Spark defaults',driverMemory:'2g',javaMajor:17,catalogType:'hadoop',comet:{'spark.plugins':'org.apache.spark.CometPlugin','spark.shuffle.manager':'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager','spark.comet.enabled':'true','spark.comet.exec.enabled':'true','spark.comet.scan.icebergNative.enabled':'true','spark.comet.scan.icebergNative.dataFileConcurrencyLimit':'4','spark.comet.explain.fallback.enabled':'true'},jvm:{'spark.comet.enabled':'false',cometJarLoaded:false},shuffleMode:'not explicitly set; actual CometNativeShuffle exchange arguments are preserved in plans',forceShuffledHashJoin:'not enabled explicitly'};
 const manifest={runtime,launchSettings,queryRevision:pins.comet,capturedAt:captures.map(c=>c.capturedAt),snapshots:captures[0].snapshots,config:captures[0].config,master:captures[0].master,queryResultsExecuted:false,q15Adaptation:captures[0].q15Adaptation,modes:{normal:'autoBroadcastJoinThreshold=10485760', 'no-broadcast':'autoBroadcastJoinThreshold=-1; semantic special cases can still broadcast'},captures:plans.map(({content,...p})=>p)};
 fs.writeFileSync(path.join(out,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
 const countSummary=p=>Object.entries(p.operators).filter(([k])=>/Join|Exchange|ColumnarToRow|Aggregate/.test(k)).map(([k,v])=>`${k}: ${v}`).join('; ');
 pages.push({file:'comet-tpch-plan-atlas.html',number:29,title:'TPC-H plan atlas: all 22 queries, Spark and Comet',intro:'88 actual EXPLAIN FORMATTED captures—not invented universal plans. Expand a query, compare four variants, and follow each operator’s inputs, outputs, keys, predicates and build side.',sections:[
 section('scope','What was captured—and what was not',`<p>All 22 CometBench-H query texts were loaded from pinned Comet source. Each was planned against the same eight Iceberg snapshots with Spark JVM and Comet, first at a 10 MiB automatic broadcast threshold, then at −1. AQE was disabled and shuffle partitions fixed at eight. Runtime: Spark 3.5.3, Iceberg 1.8.1, local Comet 1.1.0-SNAPSHOT. The exact Comet binary source is not established; JAR SHA-256 values are in the manifest.</p><p><strong>Planning-only:</strong> all 88 plans succeeded, no Spark job IDs were observed during capture, and no query result action was run. There are no new correctness or timing claims. Local fixture statistics drive these choices; larger TPC data, refreshed statistics, different snapshots, hints, Spark versions and AQE can produce different plans. The fixture’s directory name is not a verified scale factor.</p><p>Snapshot IDs are strings to avoid JavaScript integer precision loss. Q15 replaces persistent CREATE VIEW with a session temporary view; its SELECT is unchanged. SQL retains its original TPC-derived workload notices. <a href="comet-plans/manifest.json">Download complete provenance, snapshot IDs, schemas, plan hashes and operator inventories</a>.</p><p>The source walkthrough uses newer pinned development source; <a href="comet-iceberg-columnar.html#versions">read the version boundary</a>. The native/JVM-columnar shuffle comparison describes inspected code; the atlas is not a three-shuffle-mode benchmark.</p>`),
 section('read','How to read these trees',`<p>Data flows from leaves upward toward the root. A parent consumes its indented children. The numbered blocks below the tree contain exact keys, types, filters and join types. Expression IDs identify attributes, not partitions. “codegen id” is not a task/stage ID. ReusedExchange references existing materialized work; scalar subquery sections are part of the query too.</p><p>Counts below count printed operator definition blocks, including subquery sections, not Spark stages or distinct network transfers. Full plans are authoritative. In particular, Q16 retains a broadcast anti-join at threshold −1; the mode name does not promise zero broadcasts.</p><p><a href="comet-iceberg-columnar.html#operators">Operator dictionary</a> · <a href="comet-shuffle-joins.html#q3">Q3 visual walkthrough</a> · <a href="comet-iceberg-columnar.html#aggregation">Q1/Q6 aggregate walkthrough</a></p><label for="query-filter">Find a query, table or concept</label><input id="query-filter" type="search" placeholder="Q3, anti, lineitem, revenue…"><p id="query-count" aria-live="polite">22 queries</p><div class="query-links">${queries.map((_,i)=>`<a href="#q${i+1}">Q${i+1}</a>`).join(' ')}</div>`),
 ...queries.map(([title,shape,note],i)=>{
  const q=i+1,items=plans.filter(p=>p.query===q).sort((a,b)=>a.mode.localeCompare(b.mode)||a.engine.localeCompare(b.engine));
  const sql=fs.readFileSync(path.join(capture,`q${q}.sql`),'utf8');
  return section('q'+q,`Q${q} · ${title}`,`<p>${esc(shape)}</p><p><strong>Watch:</strong> ${esc(note)}</p><details class="code"><summary>Query SQL and literal parameters</summary><p><a href="comet-plans/q${q}.sql">Download SQL</a></p>${code(sql)}</details>${table(['Engine / mode','Printed join, exchange, aggregate and conversion nodes'],items.map(p=>[`${p.engine==='jvm'?'Spark JVM':'Comet'} / ${p.mode==='normal'?'10 MiB threshold':'threshold −1'}`,countSummary(p)]))}${items.map(p=>`<details class="code plan-capture" data-plan="${p.key}"><summary>${p.engine==='jvm'?'Spark JVM':'Comet'} · ${p.mode==='normal'?'10 MiB broadcast threshold':'broadcast threshold −1'} · full physical plan</summary><p><a href="comet-plans/${p.file}">Download exact plan</a> · SHA-256 <code>${p.sha256}</code></p>${code(p.content)}</details>`).join('')}`);
 }),
 section('experiment','From a plan to an honest performance experiment',`<p>These captures answer “what physical plan did this fixture produce?” To answer “which implementation is faster?”, execute paired queries on identical snapshots and verify schema, count and a stable multiset digest. Capture initial and final AQE plans plus actual SQL/stage metrics. Keep JVM-only, Comet-native-shuffle and Comet-JVM-shuffle runs separate. If forcing SHJ, record that as a further independent variant.</p><p>Expect materialization and stage behavior not obvious from one EXPLAIN node: broadcast collection, top-K candidate merging, subquery execution and reused exchanges. A static plan is not a profiler. Inspect task distributions, not just total time, and do not generalize a tiny or empty fixture to production TPC-scale throughput.</p>`)
 ]});
 const navPattern=/<nav aria-label="Chapters">([\s\S]*?)<\/nav>/;
 let nav=fs.readFileSync(path.join(root,'index.html'),'utf8').match(navPattern)?.[1];if(!nav)throw Error('No nav');
 for(const p of pages)if(!nav.includes(`data-nav="${p.file}"`))nav+=`<a href="${p.file}" data-nav="${p.file}"><span>${p.number}</span>${esc(p.title)}</a>`;
 for(const p of pages){
  const used=new Set([...p.sections.map(s=>s.html).join('\n').matchAll(/href="#ev-([^"]+)"/g)].map(m=>m[1]));
  if(used.size)p.sections.push(section('evidence','Pinned implementation and inspected tests',`<p>Excerpts are from immutable local Git pins, not dirty working files. Spark links use its source pin. A Comet public link is included only when the complete file matches the public upstream parent <code>${publicComet}</code> byte-for-byte; otherwise the local pinned excerpt and file SHA-256 remain the evidence. The local merge pin is not assumed published. Apache source excerpts are under <a href="https://www.apache.org/licenses/LICENSE-2.0">Apache License 2.0</a>.</p>${records.filter(e=>used.has(e.id)).map(e=>`<details class="code" id="ev-${e.id}"><summary>${esc(e.id)} · ${esc(e.file.split('/').at(-1))}:${e.line}</summary><p class="path">${e.url?`<a href="${e.url}">${esc(e.repo+'/'+e.file)}</a>`:esc(e.repo+'/'+e.file)}</p><p>Source pin <code>${e.revision}</code> · file SHA-256 <code>${e.fileSha256}</code></p>${code(e.excerpt)}</details>`).join('')}<p>Inspected tests include nullable-key sort-merge joins, native shuffle plan serialization and Iceberg filter pushdown. They were not executed here. The source excerpt anchors support this walkthrough but are not an exhaustive feature certification. <a href="comet-guide-validation.json">Evidence and artifact validation manifest</a>.</p><p>Supporting official documentation: <a href="https://datafusion.apache.org/comet/contributor-guide/native_shuffle.html">native shuffle</a>, <a href="https://datafusion.apache.org/comet/contributor-guide/jvm_shuffle.html">JVM shuffle</a>, <a href="https://datafusion.apache.org/comet/user-guide/latest/understanding-comet-plans.html">reading Comet plans</a>. These are moving documentation; pinned code takes precedence.</p>`));
  const html=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(p.title)} · Distributed systems field guide</title><link rel="stylesheet" href="styles.css"><link rel="stylesheet" href="kafka-rpc-atlas.css"><link rel="stylesheet" href="comet-guide.css"><script defer src="app.js"></script><script defer src="comet-guide.js"></script></head><body data-page="${p.file}"><a class="skip" href="#main">Skip to content</a><header><a href="index.html">Distributed systems / source to sink</a><span>Iceberg · Arrow · Spark · Comet</span></header><div class="layout"><aside class="sidebar"><label for="chapter-filter">Find a chapter</label><input id="chapter-filter" type="search" placeholder="Comet, join, shuffle…"><nav aria-label="Chapters">${nav}</nav></aside><main id="main"><div class="hero"><p class="eyebrow">Chapter ${p.number} · Source-backed batch execution</p><h1>${esc(p.title)}</h1><p class="intro">${esc(p.intro)}</p><nav class="toc" aria-label="On this page">${p.sections.map(s=>`<a href="#${s.id}">${esc(s.title)}</a>`).join('')}</nav></div>${p.sections.map(s=>`<section id="${s.id}"${/^q\d+$/.test(s.id)?' class="query-section"':''}><h2>${esc(s.title)}</h2>${s.html}</section>`).join('\n')}<footer><a href="index.html">Field guide home</a>${pages.filter(o=>o!==p).map(o=>`<a href="${o.file}">${esc(o.title)}</a>`).join('')}</footer></main></div></body></html>\n`;
  fs.writeFileSync(path.join(root,p.file),html);
 }
 for(const file of fs.readdirSync(root).filter(f=>f.endsWith('.html'))){const dest=path.join(root,file),html=fs.readFileSync(dest,'utf8');if(navPattern.test(html))fs.writeFileSync(dest,html.replace(navPattern,()=>`<nav aria-label="Chapters">${nav}</nav>`));}
 const index=path.join(root,'index.html'),html=fs.readFileSync(index,'utf8');
 const entry='<!-- comet-deep-entry --><section id="comet-iceberg-deep"><h2>Comet, Spark joins and the Iceberg columnar pipeline</h2><p>Follow <a href="comet-iceberg-columnar.html">files through columnar operators and row boundaries</a>, compare <a href="comet-shuffle-joins.html">JVM/native shuffle and joins</a>, then inspect <a href="comet-tpch-plan-atlas.html">all 22 TPC-H queries across 88 captured plans</a>. Twelve focused diagrams connect process boundaries to pinned code.</p></section><!-- /comet-deep-entry -->';
 fs.writeFileSync(index,html.includes('<!-- comet-deep-entry -->')?html.replace(/<!-- comet-deep-entry -->[\s\S]*?<!-- \/comet-deep-entry -->/,()=>entry):html.replace('<section',entry+'<section'));
 for(const item of diagrams)fs.writeFileSync(path.join(root,'diagrams',item.id+'.mmd'),item.source);
 const validation={sourceRevisions:pins,sourceTestsExecuted:false,queryResultsExecuted:false,capturedPlans:88,queries:22,pages:pages.map(p=>p.file),diagrams:diagrams.map(d=>d.id),evidence:records.map(({excerpt,...e})=>e)};
 fs.writeFileSync(path.join(root,'comet-guide-validation.json'),JSON.stringify(validation,null,2)+'\n');
 return {pages:pages.length,plans:plans.length,diagrams:diagrams.length,evidence:records.length};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
 console.log(buildCometGuide(root,process.argv[2]||'/private/tmp/comet-plan-atlas.0Gfq1u',{comet:process.argv[3]||'/Users/srajak/Documents/repos/oss/apache/datafusion-comet',spark:process.argv[4]||'/Users/srajak/Documents/repos/oss/apache/spark'}));
}
