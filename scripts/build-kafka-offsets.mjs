import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {esc} from './build-kafka-guide.mjs';
import {readEvidence,table,code} from './build-kafka-recovery.mjs';
import {evidence} from './kafka-offset-evidence.mjs';
import {diagrams} from './kafka-offset-diagrams.mjs';
import {chapter} from './kafka-offset-content.mjs';

export function buildOffsets(root,sources){
 const revisions=JSON.parse(fs.readFileSync(path.join(root,'validation.json'),'utf8')).sourceRevisions;
 const records=[...new Set(evidence.map(e=>e.repo))].flatMap(repo=>{
  const revision=revisions[repo];if(!/^[a-f0-9]{40}$/.test(revision))throw Error(`Unpinned ${repo}`);
  return readEvidence(path.join(sources,repo.toLowerCase()),revision,evidence.filter(e=>e.repo===repo)).map(r=>({...r,revision,url:r.url.replace('/apache/kafka/','/apache/'+repo.toLowerCase()+'/')}));
 });
 const byId=new Map(records.map(r=>[r.id,r]));
 const refs=(...ids)=>`<div class="refs">Source: ${ids.map(id=>{if(!byId.has(id))throw Error(`Unknown evidence ${id}`);return `<a class="cite" href="#ev-${id}">${esc(id)}</a>`;}).join(' ')}</div>`;
 const figure=id=>{
  const d=diagrams.find(d=>d.id===id);if(!d)throw Error(`Unknown diagram ${id}`);
  return `<figure class="diagram" id="${id}"><figcaption><h3>${esc(d.title)}</h3><p>${esc(d.caption)}</p></figcaption><div class="diagram-tools"><button data-zoom="in" aria-label="Zoom in diagram">Zoom +</button><button data-zoom="out" aria-label="Zoom out diagram">Zoom −</button><button data-zoom="fit">Fit width</button><button data-zoom="actual">Actual size</button><a href="diagrams/${id}.svg" target="_blank">Full-size SVG</a><a href="diagrams/${id}.mmd">Mermaid source</a><span class="diagram-hint">Scroll within the diagram or open full size.</span></div><div class="diagram-scroll" tabindex="0" aria-label="Scrollable diagram: ${esc(d.title)}"><img src="diagrams/${id}.svg" alt="${esc(d.title+'. '+d.caption)}" loading="lazy"></div></figure>`;
 };
 const p=chapter({section:(id,title,html)=>({id,title,html}),table,code,refs,figure,details:(title,html)=>`<details class="qa"><summary>${esc(title)}</summary>${html}</details>`});
 const sourceHtml=`<p>Pinned source snapshots: ${[...new Set(records.map(e=>e.repo))].map(repo=>`<a href="https://github.com/apache/${repo.toLowerCase()}/tree/${revisions[repo]}">${esc(repo)} ${revisions[repo].slice(0,12)}</a>`).join(' · ')}.</p><p>Engine tests, snippets, benchmarks and live fault injection were not run. Test bodies were inspected as source evidence. Component placements, offsets and crash timelines are illustrative. Apache source excerpts are licensed under <a href="https://www.apache.org/licenses/LICENSE-2.0">Apache License 2.0</a>.</p>${records.map(r=>`<details class="code" id="ev-${r.id}"><summary>${esc(r.id)} · ${esc(r.file.split('/').at(-1))}:${r.line}</summary><p>${esc(r.claim)}</p><p class="path"><a href="${r.url}">${esc(r.repo+'/'+r.file)}</a></p>${code(r.excerpt)}</details>`).join('\n')}<p>Supporting published API references: <a href="https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html">Kafka 4.1 consumer API</a>, <a href="https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html">Spark 4.0.2 Structured Streaming Kafka integration</a>, and <a href="https://spark.apache.org/docs/4.0.2/streaming-kafka-0-10-integration.html">legacy Spark DStream integration</a>. These releases differ from the pinned development snapshots; source wins where details differ. Historical wording about Kafka lacking transactions must not be read as a claim about modern Kafka.</p><p><a href="kafka-offset-validation.json">Source manifest and artifact validation</a></p>`;
 const sections=[...p.sections,{id:'evidence',title:'Pinned code, inspected tests and validation scope',html:sourceHtml}];
 const navPattern=/<nav aria-label="Chapters">([\s\S]*?)<\/nav>/;
 let nav=fs.readFileSync(path.join(root,'index.html'),'utf8').match(navPattern)?.[1];if(!nav)throw Error('Missing navigation');
 if(!nav.includes(`data-nav="${p.file}"`))nav+=`<a href="${p.file}" data-nav="${p.file}"><span>${p.number}</span>${esc(p.title)}</a>`;
 const html=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(p.title)} · Distributed systems field guide</title><link rel="stylesheet" href="styles.css"><link rel="stylesheet" href="kafka-rpc-atlas.css"><script defer src="app.js"></script></head><body data-page="${p.file}"><a class="skip" href="#main">Skip to content</a><header><a href="index.html">Distributed systems / source to sink</a><span>Offsets · checkpoints · callbacks · recovery</span></header><div class="layout"><aside class="sidebar"><label for="chapter-filter">Find a chapter</label><input id="chapter-filter" type="search" placeholder="Commit, checkpoint, failure…"><nav aria-label="Chapters">${nav}</nav></aside><main id="main"><div class="hero"><p class="eyebrow">Chapter ${p.number} · Kafka + Flink + Spark</p><h1>${esc(p.title)}</h1><p class="intro">${esc(p.intro)}</p><p class="guide-footer-note">Read the summary first, follow one runtime, then expand its pinned code evidence.</p><nav class="toc" aria-label="On this page">${sections.map(s=>`<a href="#${s.id}">${esc(s.title)}</a>`).join('')}</nav></div>${sections.map(s=>`<section id="${s.id}"><h2>${esc(s.title)}</h2>${s.html}</section>`).join('\n')}<footer><a href="index.html">Field guide home</a><a href="kafka-group-share-coordinators.html">Group and share coordinator recovery</a></footer></main></div></body></html>\n`;
 fs.writeFileSync(path.join(root,p.file),html);
 for(const file of fs.readdirSync(root).filter(f=>f.endsWith('.html'))){
  const dest=path.join(root,file),text=fs.readFileSync(dest,'utf8');
  if(navPattern.test(text))fs.writeFileSync(dest,text.replace(navPattern,()=>`<nav aria-label="Chapters">${nav}</nav>`));
 }
 const landing=path.join(root,'index.html'),index=fs.readFileSync(landing,'utf8');
 const entry=`<!-- kafka-offset-entry --><section id="offset-commits-deep-dive"><h2>commitSync, commitAsync, Flink and Spark</h2><p><a href="${p.file}">Read the offset-commit deep dive</a>: nine diagrams trace caller threads, coordinator RPCs, failure windows, Flink checkpoints, Spark checkpoint logs and legacy DStreams.</p></section><!-- /kafka-offset-entry -->`;
 fs.writeFileSync(landing,index.includes('<!-- kafka-offset-entry -->')?index.replace(/<!-- kafka-offset-entry -->[\s\S]*?<!-- \/kafka-offset-entry -->/,()=>entry):index.replace('<section',entry+'<section'));
 for(const d of diagrams)fs.writeFileSync(path.join(root,'diagrams',d.id+'.mmd'),d.source);
 const rawPath=path.join(root,'diagrams.md'),raw=fs.readFileSync(rawPath,'utf8');
 const block='<!-- kafka-offset-diagrams -->\n\n# Offset commits: Kafka, Flink and Spark\n\n'+diagrams.map(d=>`## ${d.title}\n\n${d.caption}\n\n\`\`\`mermaid\n${d.source}\`\`\`\n`).join('\n')+'<!-- /kafka-offset-diagrams -->';
 fs.writeFileSync(rawPath,raw.includes('<!-- kafka-offset-diagrams -->')?raw.replace(/<!-- kafka-offset-diagrams -->[\s\S]*?<!-- \/kafka-offset-diagrams -->/,()=>block):raw+'\n'+block+'\n');
 const validation={sourceRevisions:Object.fromEntries([...new Set(records.map(r=>r.repo))].map(repo=>[repo,revisions[repo]])),sourceInspectionOnly:true,engineTestsRun:false,snippetsRun:false,liveFaultInjectionRun:false,pages:[p.file],diagrams:diagrams.map(d=>d.id),evidence:records.map(({excerpt,...r})=>r)};
 fs.writeFileSync(path.join(root,'kafka-offset-validation.json'),JSON.stringify(validation,null,2)+'\n');
 return {pages:1,diagrams:diagrams.length,evidence:records.length};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))console.log(buildOffsets(path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),path.resolve(process.argv[2]||'..')));
