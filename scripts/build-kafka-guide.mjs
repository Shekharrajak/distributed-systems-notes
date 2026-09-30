import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {rpcDescriptions} from './kafka-rpc-descriptions.mjs';
import {evidenceSpecs} from './kafka-guide-evidence.mjs';
import {diagrams} from './kafka-guide-diagrams.mjs';
import {walkthrough} from './kafka-guide-content.mjs';

export const esc=v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
export const parseSchema=text=>JSON.parse(text.replace(/^\s*\/\/.*$/gm,''));
export function pairSchemas(messages){
  const pairs=new Map();
  for(const message of messages){
    const schema=message.schema;
    if(!['request','response'].includes(schema.type))continue;
    if(!Number.isInteger(schema.apiKey))throw Error(`Missing API key: ${schema.name}`);
    const pair=pairs.get(schema.apiKey)||{id:schema.apiKey};
    if(pair[schema.type])throw Error(`Duplicate ${schema.type}: ${schema.apiKey}`);
    pair[schema.type]=message;pairs.set(schema.apiKey,pair);
  }
  return [...pairs.values()].sort((a,b)=>a.id-b.id).map(pair=>{
    if(!pair.request||!pair.response)throw Error(`Unpaired API: ${pair.id}`);
    if(pair.request.schema.name.replace(/Request$/,'')!==pair.response.schema.name.replace(/Response$/,''))throw Error(`Name mismatch: ${pair.id}`);
    return pair;
  });
}

const code=text=>`<pre class="guide-pre"><code>${esc(text)}</code></pre>`;
const table=(heads,rows)=>`<div class="table-scroll"><table><thead><tr>${heads.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(cell=>`<td>${esc(cell)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
const section=(id,title,html)=>({id,title,html});
const details=(title,html)=>`<details class="qa"><summary>${esc(title)}</summary>${html}</details>`;
const chapter='kafka-flink-distributed-runtime.html';
const atlas='kafka-rpc-atlas.html';
const newNav=`<a href="${chapter}" data-nav="${chapter}"><span>19</span>Kafka + Flink: pods, threads and RPCs</a><a href="${atlas}" data-nav="${atlas}"><span>20</span>Kafka RPC schema atlas</a>`;

export function buildKafkaGuide(repository,sourceRoot){
  const root=path.resolve(repository);
  const revisions=JSON.parse(fs.readFileSync(path.join(root,'validation.json'),'utf8')).sourceRevisions;
  const read=(repo,file)=>{
    if(!/^[a-f0-9]{40}$/.test(revisions[repo]||''))throw Error(`Missing revision: ${repo}`);
    return execFileSync('git',['show',`${revisions[repo]}:${file}`],{cwd:path.join(sourceRoot,repo),encoding:'utf8',maxBuffer:8*1024*1024});
  };
  const sourceUrl=(repo,file,line)=>`https://github.com/apache/${repo.toLowerCase()}/blob/${revisions[repo]}/${file}${line?'#L'+line:''}`;
  const evidence=new Map(evidenceSpecs.map(([id,repo,file,needle,claim])=>{
    const text=read(repo,file);const at=text.indexOf(needle);
    if(at<0)throw Error(`Missing source marker ${id}: ${needle}`);
    const line=text.slice(0,at).split('\n').length;
    const snippet=text.split('\n').slice(Math.max(0,line-3),line+17).map((v,i)=>`${String(Math.max(1,line-2)+i).padStart(5)}  ${v}`).join('\n');
    return [id,{id,repo,file,line,claim,snippet,url:sourceUrl(repo,file,line)}];
  }));
  const refs=(...ids)=>`<div class="refs">Source: ${ids.map(id=>{
    if(!evidence.has(id))throw Error(`Unknown source reference: ${id}`);
    return `<a class="cite" href="${chapter}#${id}">${esc(id)}</a>`;
  }).join(' ')}</div>`;
  const figure=id=>{
    const item=diagrams.find(d=>d.id===id);if(!item)throw Error(`Missing diagram: ${id}`);
    return `<figure class="diagram" id="${id}"><figcaption><h3>${esc(item.title)}</h3><p>${esc(item.caption)}</p></figcaption><div class="diagram-tools"><button data-zoom="in" aria-label="Zoom in diagram">Zoom +</button><button data-zoom="out" aria-label="Zoom out diagram">Zoom −</button><button data-zoom="fit">Fit width</button><button data-zoom="actual">Actual size</button><a href="diagrams/${id}.svg" target="_blank">Full-size SVG</a><a href="diagrams/${id}.mmd">Mermaid source</a><span class="diagram-hint">Text stays readable. Scroll or open the full-size diagram.</span></div><div class="diagram-scroll" tabindex="0" aria-label="Scrollable diagram: ${esc(item.title)}"><img src="diagrams/${id}.svg" alt="${esc(item.title+'. '+item.caption)}" loading="lazy"></div></figure>`;
  };
  const specs='clients/src/main/resources/common/message';
  const schemaFiles=execFileSync('git',['ls-tree','--name-only',revisions.Kafka,`${specs}/`],{cwd:path.join(sourceRoot,'Kafka'),encoding:'utf8'}).trim().split('\n').filter(f=>f.endsWith('.json'));
  const messages=schemaFiles.map(file=>{const raw=read('Kafka',file);return {file,raw,schema:parseSchema(raw),url:sourceUrl('Kafka',file)};});
  const pairs=pairSchemas(messages);
  for(const pair of pairs){if(!rpcDescriptions.has(pair.id))throw Error(`Missing explanation for API ${pair.id}`);}
  const headers=messages.filter(m=>m.schema.type==='header');
  if(headers.length!==2)throw Error('Expected RequestHeader and ResponseHeader');
  const active=pairs.filter(p=>p.request.schema.validVersions!=='none').length;
  const baseIndex=fs.readFileSync(path.join(root,'index.html'),'utf8');
  let nav=baseIndex.match(/<nav aria-label="Chapters">([\s\S]*?)<\/nav>/)?.[1];
  if(!nav)throw Error('Missing chapter navigation');
  if(!nav.includes(`data-nav="${chapter}"`))nav+=newNav;
  const document=(file,title,intro,sections,extra='')=>`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(title)} · Distributed systems field guide</title><link rel="stylesheet" href="styles.css"><link rel="stylesheet" href="kafka-rpc-atlas.css"><script defer src="app.js"></script>${extra}</head>
<body data-page="${file}"><a class="skip" href="#main">Skip to content</a><header><a href="index.html">Distributed systems / source to sink</a><span>Architecture · messages · code · failures</span></header><div class="layout"><aside class="sidebar"><label for="chapter-filter">Find a chapter</label><input id="chapter-filter" type="search" placeholder="Kafka, RPC, threads…"><nav aria-label="Chapters">${nav}</nav></aside><main id="main"><div class="hero"><p class="eyebrow">Source-backed deep dive · pinned repository revisions</p><h1>${esc(title)}</h1><p class="intro">${esc(intro)}</p><nav class="toc" aria-label="On this page">${sections.map(s=>`<a href="#${s.id}">${esc(s.title)}</a>`).join('')}</nav></div>${sections.map(s=>`<section id="${s.id}"><h2>${esc(s.title)}</h2>${s.html}</section>`).join('\n')}<footer><a href="index.html">Field guide home</a><a href="${file===chapter?atlas:chapter}">${file===chapter?'Open the complete Kafka RPC atlas':'Back to pods, threads and recovery'}</a></footer></main></div></body></html>\n`;
  const evidenceHtml=`${table(['Repository','Inspected commit'],['Kafka','flink','flink-connector-kafka'].map(repo=>[repo,revisions[repo]]))}<p class="guide-footer-note">Source excerpts and schemas originate in Apache projects under Apache License 2.0. <a href="https://www.apache.org/licenses/LICENSE-2.0">License</a>. Full schema source retains upstream license headers.</p>${[...evidence.values()].map(e=>`<details class="code" id="${e.id}"><summary>${esc(e.id)} · ${esc(e.file.split('/').at(-1))}:${e.line}</summary><p>${esc(e.claim)}</p><p class="path"><a href="${e.url}">${esc(e.repo+'/'+e.file)}</a> · ${revisions[e.repo].slice(0,12)}</p>${code(e.snippet)}</details>`).join('\n')}`;
  const sections=walkthrough({section,table,figure,refs,code,details});
  for(const s of sections)s.html=s.html.replace('<div id="source-evidence"></div>',evidenceHtml);
  fs.writeFileSync(path.join(root,chapter),document(chapter,'Kafka + Flink: pods, threads, RPCs and recovery','Start at the cluster boundary, follow one record into the implementation, then practice the failure and presentation mental models.',sections));
  const schemaBlock=message=>`<h4>${esc(message.schema.name)}</h4><p><a href="${message.url}">Pinned upstream schema</a> · valid versions <code>${esc(message.schema.validVersions)}</code> · flexible <code>${esc(message.schema.flexibleVersions||'not applicable')}</code></p>${code(message.raw)}`;
  const shape=schema=>(schema.fields||[]).map(f=>`${f.name}: ${f.type} [v${f.versions}]`).join('\n')||'(no active fields; retired schema)';
  const atlasSections=[
    section('scope','Scope: all schemas in this checkout',`<p><strong>${pairs.length} API pairs</strong>: ${active} with valid versions and ${pairs.length-active} retired entries, plus both common headers. Every request/response schema below is extracted from <code>${specs}</code> at Kafka <code>${revisions.Kafka}</code>; nested fields, common structures, defaults, version gates, tagged fields, comments and upstream license headers are retained.</p><p>This is a snapshot of the inspected code, not “all Kafka APIs ever released.” Retired IDs stay reserved; unsupported historical versions are not reconstructed here. The highest version in a schema may be unstable or disabled by feature configuration. ApiVersions on the actual endpoint plus client support determines what can be sent.</p><p>The atlas is deliberately broader than KafkaSource: it includes administrative, broker/controller, share-group, Streams-group and telemetry protocols. It does not claim that Flink emits all of them. <a href="kafka-rpc-schemas.json">Download normalized schema catalog</a> · <a href="${chapter}">Read the distributed runtime walkthrough</a>.</p>`),
    section('wire','The envelope: schema JSON is not the wire format',`${code(`TCP request frame:
  int32 size (bytes after this size field)
  RequestHeader(apiKey, apiVersion, correlationId, clientId, [tags])
  RequestBody selected by (apiKey, apiVersion)

TCP response frame:
  int32 size
  ResponseHeader(correlationId, [tags])
  ResponseBody for the corresponding request API/version

No normal Produce response when acks=0.`)}<p>Kafka’s message generator turns the JSON specification into Java message data classes with version-aware binary read/write methods. Big-endian fixed-width integers, strings/arrays, compact encodings and tagged fields are selected according to the schema/version. The response header does not repeat the API key/version: the client matches the correlation ID to its outstanding request.</p><p>Flexible versions use compact encodings and tagged-field sections; this does not make the protocol JSON. RequestHeader.ClientId retains its older string encoding even in the flexible header. Header-version selection also has exceptions: ApiVersions responses use response header v0 in this inspected generator. Do not infer every header solely from a rule of thumb about flexible versions.</p>${headers.map(schemaBlock).join('')}${refs('k-headers','t-schema')}<p>Supporting reference: <a href="https://cwiki.apache.org/confluence/spaces/KAFKA/pages/120722234/KIP-482+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields">Apache KIP-482: flexible/tagged fields</a>. The pinned schema remains the authority for the version shown here.</p>`),
    section('record-batch','Inside Produce.Records and Fetch.Records',`<p>The <code>records</code> field is not just your JSON business event. Modern record batches have their own binary layout: base offset, length, partition leader epoch, magic, CRC, attributes, last offset delta, timestamps, producer ID, producer epoch, base sequence, record count and encoded records. Attributes include compression and transactional/control information. Individual records contain timestamp/offset deltas, key bytes, value bytes and headers.</p><p>The producer’s serializer owns the business value format (JSON, Avro, Protobuf, arbitrary bytes…). Kafka’s RPC schema owns the transport contract. Compression/batching amortizes work. Producer IDs/epochs/sequences support idempotence; control batches express transaction outcomes. None of these fields is a Kubernetes pod identity or Flink checkpoint ID.</p>${refs('k-record','k-producer')}`),
    section('examples','Concrete decoded request/response examples',`<p>These are intentionally partial, human-readable decoded examples—not literal JSON transmitted to Kafka and not a byte-complete client implementation. The expandable schemas include every field. Chosen versions avoid silently mixing fields from different API generations.</p>${table(['API/version and owner','Selected request fields','Selected response fields / meaning'],[
      ['Produce v9, partition leader','TransactionalId="orders-job-T42"; Acks=-1; TimeoutMs=30000; TopicData[{Name:"orders-out", PartitionData:[{Index:0, Records:<batch>}]}]','Responses[{Name:"orders-out", PartitionResponses:[{Index:0, ErrorCode:0, BaseOffset:900, ...}]}]; ThrottleTimeMs. Success is not transaction visibility.'],
      ['Fetch v12, input partition leader','ReplicaId=-1; MaxWaitMs=500; MinBytes=1; MaxBytes=1048576; IsolationLevel=1; SessionId=0; SessionEpoch=-1; Topics[{Topic:"orders", Partitions:[{Partition:0, FetchOffset:104, CurrentLeaderEpoch:-1, ...}]}]','Responses per partition include Records, HighWatermark, LastStableOffset, LogStartOffset, AbortedTransactions and ErrorCode. ReadCommitted is bounded by LSO and filters aborted transactions.'],
      ['OffsetCommit v8, group coordinator','GroupId="orders-job"; GenerationIdOrMemberEpoch=-1; MemberId=""; GroupInstanceId=null; Topics[{Name:"orders", Partitions:[{PartitionIndex:0, CommittedOffset:105, CommittedLeaderEpoch:-1, CommittedMetadata:null}]}]','Topics/Partitions with ErrorCode, plus throttle time. An offset-only, manually assigned client is not a classic subscribed member.'],
      ['EndTxn v3, transaction coordinator','TransactionalId="orders-job-T42"; ProducerId=7001; ProducerEpoch=2; Committed=true','ErrorCode and ThrottleTimeMs. Output markers/LSO, not this response alone, control downstream read_committed delivery.']
    ])}<p>Example offsets, IDs and limits are illustrative. Empty initial Fetch sessions versus incremental sessions, coordinator epochs and transaction versions alter subsequent requests. Never blindly copy one request example across API versions.</p>`),
    section('families','Who talks to whom: API families',`${table(['Family','Owner / use','Typical examples'],[
      ['Discovery','A reachable broker/controller endpoint, as supported; return actual owners','ApiVersions, Metadata, FindCoordinator, DescribeCluster'],
      ['Record data','Partition-serving broker: leaders normally; follower reads can be configured','Produce, Fetch, ListOffsets, OffsetForLeaderEpoch'],
      ['Consumer groups','Group coordinator broker, with list/discovery exceptions','OffsetCommit/Fetch; classic Join/Sync/Heartbeat; newer ConsumerGroupHeartbeat'],
      ['Transactions','Transaction coordinator plus participant partition leaders','InitProducerId, AddPartitionsToTxn, EndTxn, WriteTxnMarkers, TxnOffsetCommit'],
      ['Admin/security','Target broker or controller according to resource and API scope','CreateTopics, AlterConfigs, ACLs, SASL, SCRAM, delegation tokens'],
      ['KRaft/control','Controller quorum and broker/controller lifecycle','Vote, FetchSnapshot, BrokerHeartbeat, AlterPartition, Envelope'],
      ['Share/Streams groups','Their relevant broker coordinator/partition owner','ShareFetch/Acknowledge, share state APIs, StreamsGroupHeartbeat'],
      ['Telemetry','Client to broker','GetTelemetrySubscriptions, PushTelemetry']
    ])}<p>“listeners: broker/controller” means where the server may accept an API; it does not mean any such node owns every resource. Metadata, key-based coordinator lookup, resource-specific routing and controller forwarding still matter. APIs with multiple caller roles (especially Fetch) use versioned body fields and authorization to disambiguate.</p>`),
    section('index','Index: every API key',`<div class="table-scroll rpc-index"><table><thead><tr><th>ID</th><th>API</th><th>Versions / flexible</th><th>Family</th><th>Listener scope</th></tr></thead><tbody>${pairs.map(({id,request})=>{const s=request.schema;return `<tr><td>${id}</td><td><a href="#rpc-${id}">${esc(s.name.replace(/Request$/,''))}</a></td><td>${esc(s.validVersions)} / ${esc(s.flexibleVersions||'n/a')}${s.latestVersionUnstable?' · latest unstable':''}</td><td>${esc(rpcDescriptions.get(id).family)}</td><td>${esc((s.listeners||[]).join(', ')||'retired')}</td></tr>`;}).join('')}</tbody></table></div>`),
    section('schemas','Searchable request and response schemas',`<div class="rpc-controls"><label for="rpc-search">Find API, field or concept<input type="search" id="rpc-search" placeholder="Fetch, 26, TransactionalId, epoch…"></label><label for="rpc-family">Family<select id="rpc-family"><option value="">All families</option>${[...new Set([...rpcDescriptions.values()].map(v=>v.family))].sort().map(f=>`<option>${esc(f)}</option>`).join('')}</select></label><button id="rpc-reset" type="button">Clear filters</button></div><p id="rpc-count" role="status" aria-live="polite">${pairs.length} API pairs</p><p id="rpc-empty" class="rpc-empty" hidden>No matching API. Clear the filters and try another field name.</p>${pairs.map(({id,request,response})=>{const info=rpcDescriptions.get(id),s=request.schema;return `<details class="rpc-schema" id="rpc-${id}" data-family="${esc(info.family)}"><summary>${id} · ${esc(s.name.replace(/Request$/,''))} <span class="rpc-meta">— ${esc(info.family)} · v${esc(s.validVersions)}</span></summary><div class="rpc-body"><p>${esc(info.purpose)}</p><p class="rpc-meta">Typical caller/owner: ${esc(info.route)}. Declared listeners: ${esc((s.listeners||[]).join(', ')||'none (retired)')}. ${s.latestVersionUnstable?'Latest request version is marked unstable.':''}</p>${table(['Request top-level shape','Response top-level shape'],[[shape(s),shape(response.schema)]])}${schemaBlock(request)}${schemaBlock(response)}</div></details>`;}).join('\n')}`),
    section('use-it','How to use this atlas during a presentation',`<p>Start with the runtime walkthrough, not the 94-row index. For a packet-level question, identify the owner, API key and negotiated version; then open one request/response pair. Read <code>versions</code>, <code>nullableVersions</code>, <code>flexibleVersions</code> and <code>taggedVersions</code> before treating any field as present on the wire. “Field exists somewhere in this JSON” does not mean it exists in every version.</p><p>For an error, inspect its scope: request-wide, topic-wide or partition-specific. Retry only under the API/client’s error rules. A connection loss can leave an operation’s outcome unknown. Fencing protects ownership; blindly retrying with a new identity can create a new effect rather than finish the old one.</p><p>Schema completeness is checked against the pinned repository tree, including paired IDs and duplicate detection. This is static documentation validation—not a packet capture, broker compatibility test or execution of all APIs.</p>`)
  ];
  fs.writeFileSync(path.join(root,atlas),document(atlas,'Kafka RPC schema atlas','Every request/response pair in the inspected Kafka source, with routing, version gates, examples and full expandable schemas.',atlasSections,'<script defer src="kafka-rpc-atlas.js"></script>'));
  fs.writeFileSync(path.join(root,'kafka-rpc-schemas.json'),JSON.stringify({repository:'apache/kafka',revision:revisions.Kafka,license:'Apache-2.0',scope:specs,apiPairs:pairs.length,active,retired:pairs.length-active,headers:headers.map(m=>({schema:m.schema,url:m.url})),apis:pairs.map(({id,request,response})=>({id,...rpcDescriptions.get(id),request:request.schema,response:response.schema,requestSource:request.url,responseSource:response.url}))},null,2)+'\n');
  fs.mkdirSync(path.join(root,'diagrams'),{recursive:true});
  for(const d of diagrams)fs.writeFileSync(path.join(root,'diagrams',d.id+'.mmd'),d.source+'\n');
  const diagramDoc=path.join(root,'diagrams.md');
  const marker='<!-- kafka-rpc-deep-dive -->';
  const original=fs.readFileSync(diagramDoc,'utf8').split(marker)[0].trimEnd();
  fs.writeFileSync(diagramDoc,original+'\n\n'+marker+'\n\n# Kafka RPC and Flink runtime deep dive\n\n'+diagrams.map(d=>`## ${d.title}\n\n${d.caption}\n\n\`\`\`mermaid\n${d.source}\n\`\`\`\n`).join('\n'));
  for(const file of fs.readdirSync(root).filter(f=>f.endsWith('.html'))){
    const target=path.join(root,file);let html=fs.readFileSync(target,'utf8');
    if(html.includes('aria-label="Chapters"')&&!html.includes(`data-nav="${chapter}"`))html=html.replace(/(<nav aria-label="Chapters">[\s\S]*?)(<\/nav>)/,`$1${newNav}$2`);
    if(file==='index.html'&&!html.includes('id="rpc-guide-links"'))html=html.replace('<section id="section-1">',`<section id="rpc-guide-links"><h2>New deep dive: Kafka RPCs and Flink execution</h2><p><a href="${chapter}">Start high level: pods → threads → messages → storage → recovery → presentation mental model</a></p><p><a href="${atlas}">Look up all ${pairs.length} Kafka request/response API schemas</a></p></section><section id="section-1">`);
    fs.writeFileSync(target,html.replace(/[\t ]+$/gm,'').replace(/\n*$/,'\n'));
  }
  const result={sourceRevisions:Object.fromEntries(['Kafka','flink','flink-connector-kafka'].map(repo=>[repo,revisions[repo]])),apiPairs:pairs.length,active,retired:pairs.length-active,headers:headers.length,sourceAnchors:evidence.size,newPages:[chapter,atlas],diagrams:diagrams.map(d=>d.id),engineTestsExecuted:false,runtimeObserved:false};
  fs.writeFileSync(path.join(root,'kafka-guide-validation.json'),JSON.stringify(result,null,2)+'\n');
  return result;
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
  if(!process.argv[2])throw Error('Usage: node scripts/build-kafka-guide.mjs <directory containing Kafka, flink, flink-connector-kafka checkouts>');
  console.log(JSON.stringify(buildKafkaGuide(root,path.resolve(process.argv[2])),null,2));
}
