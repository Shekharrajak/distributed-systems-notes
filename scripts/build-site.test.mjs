import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {isSiteFile,publicSourceLinks,validateSite,buildSite} from './build-site.mjs';

const revision='a'.repeat(40);
function fixture(t){
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'streaming-pages-test-'));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  return root;
}
function write(root,file,content){
  const target=path.join(root,file);
  fs.mkdirSync(path.dirname(target),{recursive:true});
  fs.writeFileSync(target,content);
}

test('publishes only guide assets',()=>{
  for(const file of ['index.html','styles.css','app.js','diagrams/a.svg','diagrams/a.mmd','validation.json','stream-processing/distributed-streaming-systems-guide/index.html'])assert.equal(isSiteFile(file),true,file);
  for(const file of ['.git/config','.github/workflows/static.yml','scripts/build-site.mjs','spark/notes.html','secret.txt','README.md'])assert.equal(isSiteFile(file),false,file);
});

test('converts local source links to pinned public commits',()=>{
  const html='<a href="../../oss/tmp/Kafka/clients/Example.java">code</a><a href="index.html#main">home</a>';
  assert.equal(publicSourceLinks(html,{Kafka:revision}),`<a href="https://github.com/apache/kafka/blob/${revision}/clients/Example.java">code</a><a href="index.html#main">home</a>`);
  assert.throws(()=>publicSourceLinks(html,{}),/Missing source revision/);
});

test('validates page fragments, redirects and font assets',t=>{
  const root=fixture(t);
  write(root,'index.html','<main id="main"><a href="chapter.html#details">next</a><link href="styles.css"></main>');
  write(root,'chapter.html','<section id="details"><a href="index.html#main">home</a></section>');
  write(root,'old/index.html','<meta content="0;url=../index.html"><a href="../index.html">home</a>');
  write(root,'styles.css',"@font-face{src:url('font.woff2')}");
  write(root,'font.woff2','font');
  assert.deepEqual(validateSite(root),{files:5,htmlPages:3,diagrams:0,checkedLinks:6});
});

test('rejects broken links, fragments and project-root absolute URLs',t=>{
  const root=fixture(t);
  write(root,'index.html','<a href="missing.html">broken</a>');
  assert.throws(()=>validateSite(root),/Missing asset/);
  write(root,'index.html','<a href="#absent">broken</a>');
  assert.throws(()=>validateSite(root),/Missing anchor/);
  write(root,'index.html','<img src="/diagrams/a.svg">');
  assert.throws(()=>validateSite(root),/Non-portable URL/);
  write(root,'index.html','<a href="../../oss/tmp/Kafka/A.java">local</a>');
  assert.throws(()=>validateSite(root),/Local source link remains/);
});

test('builds from tracked assets without publishing unrelated files',t=>{
  const root=fixture(t);
  const git=args=>execFileSync('git',args,{cwd:root,stdio:'pipe'});
  git(['init','-q']);
  write(root,'index.html','<a href="../../oss/tmp/Kafka/A.java">code</a><img src="diagrams/a.svg">');
  write(root,'diagrams/a.svg','<svg xmlns="http://www.w3.org/2000/svg"/>');
  write(root,'validation.json',JSON.stringify({sourceRevisions:{Kafka:revision}}));
  write(root,'README.md','not a deployed asset');
  git(['add','index.html','diagrams/a.svg','validation.json','README.md']);
  git(['-c','user.name=Pages test','-c','user.email=pages-test@example.invalid','-c','commit.gpgsign=false','-c','core.hooksPath=/dev/null','commit','-qm','Test fixture']);
  write(root,'untracked.html','must not publish');
  const result=buildSite(root);
  assert.equal(result.htmlPages,1);
  assert.equal(result.diagrams,1);
  assert.equal(fs.existsSync(path.join(root,'_site','untracked.html')),false);
  assert.equal(fs.existsSync(path.join(root,'_site','README.md')),false);
  assert.equal(fs.existsSync(path.join(root,'_site','.git')),false);
  assert.match(fs.readFileSync(path.join(root,'_site','index.html'),'utf8'),/https:\/\/github.com\/apache\/kafka\/blob\//);
  assert.throws(()=>buildSite(root),/_site already exists/);
});
