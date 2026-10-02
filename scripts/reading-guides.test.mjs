import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {fileURLToPath} from 'node:url';
import {readingGuides,addReadingGuide} from './reading-guides.mjs';

const root=fileURLToPath(new URL('../',import.meta.url));
test('every published technical chapter has a topic-specific guide with valid code links',()=>{
 const files=fs.readdirSync(root).filter(file=>file.endsWith('.html') && file!=='reading-paths.html');
 for(const file of files){
  const html=fs.readFileSync(root+file,'utf8');
  if(!html.includes('<main'))continue;
  assert.ok(readingGuides[file],`Missing guide: ${file}`);
  for(const [,href] of readingGuides[file].steps){
   const [target,id]=href.split('#');
   const source=fs.readFileSync(root+(target||file),'utf8');
   assert.ok(source.includes(`id="${id}"`),`${file}: missing ${href}`);
  }
 }
});
test('rebuilding replaces the guide once and preserves original chapter content',()=>{
 for(const file of Object.keys(readingGuides)){
  const html=fs.readFileSync(root+file,'utf8');
  const original=html.replace(/<!-- reading-guide:start -->[\s\S]*?<!-- reading-guide:end -->/,'');
  const once=addReadingGuide(original,file);
  assert.equal(addReadingGuide(once,file),once,file);
  assert.equal(once.replace(/<!-- reading-guide:start -->[\s\S]*?<!-- reading-guide:end -->/,''),original,file);
  assert.equal((once.match(/id="reading-guide"/g)||[]).length,1,file);
 }
});
test('redirects and unrelated HTML remain unchanged',()=>{
 const redirect='<meta http-equiv="refresh" content="0;url=index.html">';
 assert.equal(addReadingGuide(redirect,'index.html'),redirect);
 assert.equal(addReadingGuide('<main>Other content</main>','other.html'),'<main>Other content</main>');
});
