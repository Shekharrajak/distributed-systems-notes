import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {esc} from './build-kafka-guide.mjs';
import {evidence} from './kafka-recovery-evidence.mjs';
import {diagrams} from './kafka-recovery-diagrams.mjs';
import {chapters} from './kafka-recovery-content.mjs';

export function readEvidence(source,revision,specs=evidence){
 const cache=new Map();
 const read=file=>{if(!cache.has(file))cache.set(file,execFileSync('git',['show',`${revision}:${file}`],{cwd:source,encoding:'utf8',maxBuffer:12*1024*1024}));return cache.get(file);};
 return specs.map(item=>{
  const text=read(item.file),at=text.indexOf(item.needle);
  if(at<0)throw Error(`Missing evidence ${item.id}: ${item.needle}`);
  const line=text.slice(0,at).split('\n').length;
  const excerpt=text.split('\n').slice(line-1,line+27).map((value,i)=>`${line+i}  ${value}`.trimEnd()).join('\n');
  return {...item,line,excerpt,url:`https://github.com/apache/kafka/blob/${revision}/${item.file}#L${line}`};
 });
}
export const table=(heads,rows)=>`<div class="table-scroll"><table><thead><tr>${heads.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(x=>`<td>${esc(x)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
export const code=value=>`<pre class="guide-pre"><code>${esc(value)}</code></pre>`;
export function buildRecovery(root,source){
 const revision=JSON.parse(fs.readFileSync(path.join(root,'validation.json'),'utf8')).sourceRevisions.Kafka;
 if(!/^[a-f0-9]{40}$/.test(revision))throw Error('Missing pinned Kafka revision');
 const records=readEvidence(source,revision),byId=new Map(records.map(e=>[e.id,e]));
 const refs=(...ids)=>`<div class="refs">Source: ${ids.map(id=>{if(!byId.has(id))throw Error(`Unknown evidence ${id}`);return `<a class="cite" href="#ev-${id}">${esc(id)}</a>`;}).join(' ')}</div>`;
 const figure=id=>{
  const d=diagrams.find(d=>d.id===id);if(!d)throw Error(`Unknown diagram ${id}`);
  return `<figure class="diagram" id="${id}"><figcaption><h3>${esc(d.title)}</h3><p>${esc(d.caption)}</p></figcaption><div class="diagram-tools"><button data-zoom="in" aria-label="Zoom in diagram">Zoom +</button><button data-zoom="out" aria-label="Zoom out diagram">Zoom −</button><button data-zoom="fit">Fit width</button><button data-zoom="actual">Actual size</button><a href="diagrams/${id}.svg" target="_blank">Full-size SVG</a><a href="diagrams/${id}.mmd">Mermaid source</a><span class="diagram-hint">Scroll within the diagram or open full size.</span></div><div class="diagram-scroll" tabindex="0" aria-label="Scrollable diagram: ${esc(d.title)}"><img src="diagrams/${id}.svg" alt="${esc(d.title+'. '+d.caption)}" loading="lazy"></div></figure>`;
 };
 const pages=chapters({section:(id,title,html)=>({id,title,html}),table,figure,refs,code,details:(title,html)=>`<details class="qa"><summary>${esc(title)}</summary>${html}</details>`});
 const navPattern=/<nav aria-label="Chapters">([\s\S]*?)<\/nav>/;
 const index=fs.readFileSync(path.join(root,'index.html'),'utf8');
 let nav=index.match(navPattern)?.[1];if(!nav)throw Error('Missing chapter nav');
 for(const p of pages)if(!nav.includes(`data-nav="${p.file}"`))nav+=`<a href="${p.file}" data-nav="${p.file}"><span>${p.number}</span>${esc(p.title)}</a>`;
 for(const p of pages){
  const used=new Set([...p.sections.map(s=>s.html).join('').matchAll(/href="#ev-([^"]+)"/g)].map(m=>m[1]));
  const sourceHtml=`<p>Source baseline: <a href="https://github.com/apache/kafka/tree/${revision}">Kafka ${revision}</a>. Facts below were traced from this source snapshot. Test bodies were inspected as evidence; <strong>Kafka engine tests and live fault injection were not run</strong>. The validation report concerns the generated documentation, diagrams and links. Example placements and offsets are illustrative, not an observed deployment.</p><p>This is a modern KRaft/source-snapshot guide, not a guarantee for every Kafka release or feature level. It covers ordinary producer transactions and share acknowledgements, not the separate transactional-share development branch or every distributed-transaction API extension. Apache source excerpts are under the <a href="https://www.apache.org/licenses/LICENSE-2.0">Apache License 2.0</a>.</p>${records.filter(e=>used.has(e.id)).map(e=>`<details class="code" id="ev-${e.id}"><summary>${esc(e.id)} · ${esc(e.file.split('/').at(-1))}:${e.line}</summary><p>${esc(e.claim)}</p><p class="path"><a href="${e.url}">${esc(e.file)}</a></p>${code(e.excerpt)}</details>`).join('\n')}<p>Supporting versioned references: <a href="https://kafka.apache.org/41/design/protocol/">Kafka 4.1 protocol reference</a> (older than this source snapshot), and <a href="https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255070434/KIP-932%2BQueues%2Bfor%2BKafka">KIP-932 share-group design</a>. Source wins where versioned documentation or proposals differ.</p><p><a href="kafka-recovery-validation.json">Artifact validation and source manifest</a></p>`;
  const sections=[...p.sections,{id:'evidence',title:'Source evidence, scope and validation gaps',html:sourceHtml}];
  const html=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(p.title)} · Distributed systems field guide</title><link rel="stylesheet" href="styles.css"><link rel="stylesheet" href="kafka-rpc-atlas.css"><script defer src="app.js"></script></head><body data-page="${p.file}"><a class="skip" href="#main">Skip to content</a><header><a href="index.html">Distributed systems / source to sink</a><span>Ownership · durable state · fencing · recovery</span></header><div class="layout"><aside class="sidebar"><label for="chapter-filter">Find a chapter</label><input id="chapter-filter" type="search" placeholder="Election, coordinator, marker…"><nav aria-label="Chapters">${nav}</nav></aside><main id="main"><div class="hero"><p class="eyebrow">Chapter ${p.number} · source-backed Kafka deep dive</p><h1>${esc(p.title)}</h1><p class="intro">${esc(p.intro)}</p><p class="guide-footer-note">Pinned source, not a live cluster inspection. Start with the summary; expand the code at the end.</p><nav class="toc" aria-label="On this page">${sections.map(s=>`<a href="#${s.id}">${esc(s.title)}</a>`).join('')}</nav></div>${sections.map(s=>`<section id="${s.id}"><h2>${esc(s.title)}</h2>${s.html}</section>`).join('\n')}<footer><a href="index.html">Field guide home</a><a href="kafka-rpc-atlas.html">Complete Kafka RPC schema atlas</a></footer></main></div></body></html>\n`;
  fs.writeFileSync(path.join(root,p.file),html);
 }
 for(const file of fs.readdirSync(root).filter(f=>f.endsWith('.html'))){
  const filePath=path.join(root,file),html=fs.readFileSync(filePath,'utf8');
  if(navPattern.test(html))fs.writeFileSync(filePath,html.replace(navPattern,()=>`<nav aria-label="Chapters">${nav}</nav>`));
 }
 const marker='<!-- kafka-recovery-entry -->';
 const landing=fs.readFileSync(path.join(root,'index.html'),'utf8');
 const entry=`${marker}<section id="kafka-recovery-deep-dive"><h2>Kafka failure recovery: go deeper</h2><p>Three source-backed chapters, from cluster ownership down to replay and marker completion.</p><ol>${pages.map(p=>`<li><a href="${p.file}">${esc(p.title)}</a></li>`).join('')}</ol></section><!-- /kafka-recovery-entry -->`;
 fs.writeFileSync(path.join(root,'index.html'),landing.includes(marker)?landing.replace(/<!-- kafka-recovery-entry -->[\s\S]*?<!-- \/kafka-recovery-entry -->/,()=>entry):landing.replace('<section',entry+'<section'));
 for(const diagram of diagrams)fs.writeFileSync(path.join(root,'diagrams',diagram.id+'.mmd'),diagram.source);
 const rawFile=path.join(root,'diagrams.md'),raw=fs.readFileSync(rawFile,'utf8').split('\n<!-- kafka-recovery-diagrams -->')[0];
 fs.writeFileSync(rawFile,raw+'\n<!-- kafka-recovery-diagrams -->\n\n# Kafka leadership and coordinator recovery\n\n'+diagrams.map(d=>`## ${d.title}\n\n${d.caption}\n\n\`\`\`mermaid\n${d.source}\`\`\`\n`).join('\n'));
 const schemas=execFileSync('git',['ls-tree','-r','--name-only',revision,'clients/src/main/resources/common/message'],{cwd:source,encoding:'utf8'});
 const states=execFileSync('git',['show',`${revision}:server/src/main/java/org/apache/kafka/server/share/fetch/RecordState.java`],{cwd:source,encoding:'utf8'});
 if(schemas.includes('TxnShareAcknowledgeRequest.json')||states.includes('TX_PENDING'))throw Error('Share baseline changed: revisit scope before publishing');
 const validation={sourceRevision:revision,sourceInspectionOnly:true,kafkaTestsRun:false,liveFaultInjectionRun:false,pages:pages.map(p=>p.file),diagrams:diagrams.map(d=>d.id),evidence:records.map(({excerpt,...r})=>r),scopeChecks:{transactionalShareApiAbsent:true,txPendingAbsent:true}};
 fs.writeFileSync(path.join(root,'kafka-recovery-validation.json'),JSON.stringify(validation,null,2)+'\n');
 return {pages:pages.length,diagrams:diagrams.length,evidence:records.length,revision};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))console.log(JSON.stringify(buildRecovery(path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),path.resolve(process.argv[2]||'../Kafka')),null,2));
