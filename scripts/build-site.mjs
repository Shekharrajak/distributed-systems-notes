import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {addReadingGuide} from './reading-guides.mjs';

const repositories=new Set(['Kafka','flink','flink-connector-kafka','spark','iceberg','datafusion','datafusion-iceberg','datafusion-comet','arrow-rs']);
export function isSiteFile(file){
  return /^[^/]+\.(?:html|css|js|woff2)$/.test(file)
    || ['diagrams.md','validation.json','validation.md','Ubuntu-LICENSE.txt','kafka-rpc-schemas.json','kafka-guide-validation.json','kafka-recovery-validation.json','kafka-offset-validation.json','spark-guide-validation.json','comet-guide-validation.json'].includes(file)
    || /^comet-plans\/(?:manifest\.json|q(?:[1-9]|1[0-9]|2[0-2])\.sql|(?:normal|no-broadcast)-q(?:[1-9]|1[0-9]|2[0-2])-(?:jvm|comet)\.plan\.txt)$/.test(file)
    || /^diagrams\/[^/]+\.(?:svg|mmd)$/.test(file)
    || /^stream-processing\/distributed-streaming-systems-guide\/[^/]+\.html$/.test(file);
}

export function publicSourceLinks(html,revisions){
  return html.replace(/href=(["'])(?:\.\.\/)+oss\/tmp\/([^/]+)\/([^"']+)\1/g,(_,quote,repo,file)=>{
    const revision=revisions[repo];
    if(!repositories.has(repo) || !/^[a-f0-9]{40}$/.test(revision||''))throw Error(`Missing source revision for ${repo}`);
    const encoded=file.split('/').map(encodeURIComponent).join('/');
    return `href=${quote}https://github.com/apache/${repo.toLowerCase()}/blob/${revision}/${encoded}${quote}`;
  });
}

function walk(directory,prefix=''){
  return fs.readdirSync(directory,{withFileTypes:true}).flatMap(entry=>{
    const file=path.posix.join(prefix,entry.name);
    if(entry.isSymbolicLink())throw Error(`Symlink in site: ${file}`);
    return entry.isDirectory()?walk(path.join(directory,entry.name),file):[file];
  });
}

export function validateSite(directory){
  const root=path.resolve(directory);
  const files=walk(root);
  const available=new Set(files);
  if(!available.has('index.html'))throw Error('Missing index.html entry point');
  let checkedLinks=0;
  const ids=new Map();
  for(const file of files.filter(f=>f.endsWith('.html'))){
    const html=fs.readFileSync(path.join(root,file),'utf8');
    ids.set(file,new Set([...html.matchAll(/\bid=["']([^"']+)["']/g)].map(m=>m[1])));
  }
  const check=(file,raw)=>{
    const value=raw.trim().replaceAll('&amp;','&');
    if(/^(?:https?:|data:|mailto:)/i.test(value))return;
    if(value.startsWith('/') || /^[a-z]+:/i.test(value))throw Error(`Non-portable URL in ${file}: ${value}`);
    const [resource,fragment]=value.split('#');
    const target=resource?path.posix.normalize(path.posix.join(path.posix.dirname(file),decodeURIComponent(resource.split('?')[0]))):file;
    checkedLinks++;
    if(!available.has(target))throw Error(`Missing asset in ${file}: ${value}`);
    if(fragment && target.endsWith('.html') && !ids.get(target)?.has(decodeURIComponent(fragment)))throw Error(`Missing anchor in ${file}: ${value}`);
  };
  for(const file of files.filter(f=>/\.(?:html|css)$/.test(f))){
    const text=fs.readFileSync(path.join(root,file),'utf8');
    if(text.includes('oss/tmp/'))throw Error(`Local source link remains in ${file}`);
    if(file.endsWith('.html')){
      for(const [,url] of text.matchAll(/\b(?:href|src)=["']([^"']+)["']/g))check(file,url);
      for(const [,url] of text.matchAll(/content=["']\d+;url=([^"']+)["']/g))check(file,url);
    }else{
      for(const [,url] of text.matchAll(/url\(["']?([^)'"\s]+)["']?\)/g))check(file,url);
    }
  }
  return {files:files.length,htmlPages:ids.size,diagrams:files.filter(f=>f.endsWith('.svg')).length,checkedLinks};
}

export function buildSite(repository){
  const root=path.resolve(repository);
  const output=path.join(root,'_site');
  if(fs.existsSync(output))throw Error('_site already exists; move it aside before rebuilding');
  const files=execFileSync('git',['ls-files','-z'],{cwd:root,encoding:'utf8'}).split('\0').filter(file=>file && isSiteFile(file));
  const revisions=JSON.parse(fs.readFileSync(path.join(root,'validation.json'),'utf8')).sourceRevisions;
  fs.mkdirSync(output);
  for(const file of files){
    const from=path.join(root,file);
    const to=path.join(output,file);
    if(!fs.lstatSync(from).isFile())throw Error(`Expected a regular file: ${file}`);
    fs.mkdirSync(path.dirname(to),{recursive:true});
    if(file.endsWith('.html'))fs.writeFileSync(to,publicSourceLinks(addReadingGuide(fs.readFileSync(from,'utf8'),file),revisions));
    else fs.copyFileSync(from,to);
  }
  fs.writeFileSync(path.join(output,'.nojekyll'),'');
  const validation=validateSite(output);
  const commit=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim();
  fs.writeFileSync(path.join(output,'build-info.json'),JSON.stringify({commit,entryPoint:'index.html',...validation},null,2)+'\n');
  return {output,commit,...validation};
}

if(process.argv[1] && path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  console.log(JSON.stringify(buildSite(path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..')),null,2));
}
