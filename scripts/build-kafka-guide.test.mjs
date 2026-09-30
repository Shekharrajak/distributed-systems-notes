import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {parseSchema,pairSchemas,esc} from './build-kafka-guide.mjs';
import {rpcDescriptions} from './kafka-rpc-descriptions.mjs';
import {diagrams} from './kafka-guide-diagrams.mjs';
import {isSiteFile} from './build-site.mjs';

const message=(type,id=1,name='Fetch')=>({schema:{type,apiKey:id,name:name+(type==='request'?'Request':'Response')}});
test('parses Kafka comment lines without changing URLs or field descriptions',()=>{
  assert.equal(parseSchema('// license\n{ "about": "https://example.org/a//b", "apiKey": 1 }').about,'https://example.org/a//b');
});
test('requires a unique request/response pair with matching API identity',()=>{
  assert.throws(()=>pairSchemas([message('request')]),/Unpaired/);
  assert.throws(()=>pairSchemas([message('request'),message('request'),message('response')]),/Duplicate/);
  assert.throws(()=>pairSchemas([message('request'),message('response',1,'Produce')]),/Name mismatch/);
  assert.deepEqual(pairSchemas([message('request',2),message('response',2),message('request'),message('response')]).map(p=>p.id),[1,2]);
});
test('escapes schema content for HTML',()=>assert.equal(esc('<script>"&'), '&lt;script&gt;&quot;&amp;'));
test('all generated API pairs have descriptions and nested fields are retained',()=>{
  const catalog=JSON.parse(fs.readFileSync(new URL('../kafka-rpc-schemas.json',import.meta.url),'utf8'));
  assert.equal(catalog.apiPairs,94);assert.equal(catalog.active,90);assert.equal(catalog.retired,4);
  assert.equal(catalog.headers.length,2);
  assert.equal(new Set(catalog.apis.map(a=>a.id)).size,94);
  for(const api of catalog.apis){assert(rpcDescriptions.has(api.id));assert.equal(api.request.apiKey,api.response.apiKey);}
  const produce=catalog.apis.find(a=>a.id===0);
  assert.equal(produce.request.fields.find(f=>f.name==='TopicData').fields.find(f=>f.name==='PartitionData').fields.find(f=>f.name==='Records').type,'records');
  assert.equal(catalog.apis.find(a=>a.id===22).request.latestVersionUnstable,true);
});
test('every diagram has a theme and unique ID; flowcharts use curved routes',()=>{
  assert.equal(new Set(diagrams.map(d=>d.id)).size,diagrams.length);
  for(const d of diagrams){assert(d.source.startsWith('%%{init:'));assert(d.source.includes('Ubuntu, Arial, sans-serif'));if(d.source.includes('flowchart TB'))assert(d.source.includes('"curve":"basis"'));}
});
test('static publisher includes the atlas and validation assets, not generator sources',()=>{
  for(const file of ['kafka-rpc-atlas.html','kafka-rpc-atlas.js','kafka-rpc-atlas.css','kafka-rpc-schemas.json','kafka-guide-validation.json'])assert(isSiteFile(file));
  assert(!isSiteFile('scripts/build-kafka-guide.mjs'));
});
