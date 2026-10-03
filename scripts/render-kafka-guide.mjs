import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {diagrams as rpcDiagrams} from './kafka-guide-diagrams.mjs';
import {diagrams as recoveryDiagrams} from './kafka-recovery-diagrams.mjs';
import {diagrams as offsetDiagrams} from './kafka-offset-diagrams.mjs';
import {diagrams as sparkDiagrams} from './spark-guide-diagrams.mjs';
import {diagrams as cometDiagrams} from './comet-diagrams.mjs';

const recovery=process.argv.includes('--recovery');
const offsets=process.argv.includes('--offsets');
const spark=process.argv.includes('--spark');
const comet=process.argv.includes('--comet');
const diagrams=comet?cometDiagrams:spark?sparkDiagrams:offsets?offsetDiagrams:recovery?recoveryDiagrams:rpcDiagrams;

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const {default:puppeteer}=await import(process.env.GUIDE_PUPPETEER_MODULE||'puppeteer');
const bundle=process.env.GUIDE_MERMAID_BUNDLE||fileURLToPath(import.meta.resolve('mermaid/dist/mermaid.min.js'));
const fontCSS=['Regular','Medium'].map((face,i)=>`@font-face{font-family:Ubuntu;src:url(data:font/woff2;base64,${fs.readFileSync(path.join(root,`Ubuntu-${face}.woff2`)).toString('base64')}) format('woff2');font-weight:${i?'500 800':'400'};font-style:normal}`).join('\n')+'\nsvg text,svg tspan,svg foreignObject *{font-family:Ubuntu,Arial,sans-serif!important}';
const browser=await puppeteer.launch({executablePath:process.env.GUIDE_CHROME_BIN,headless:true,args:['--disable-gpu','--no-sandbox']});
const results=[];
try{
  const page=await browser.newPage();
  await page.setViewport({width:2000,height:1600,deviceScaleFactor:1});
  await page.setContent(`<html><head><style>${fontCSS}body{margin:0;background:white}#host{padding:24px;display:inline-block}</style></head><body><div id="host"></div></body></html>`);
  await page.addScriptTag({path:bundle});
  await page.evaluate(async()=>{await document.fonts.load('17px Ubuntu');await document.fonts.load('500 17px Ubuntu');await document.fonts.ready;mermaid.initialize({startOnLoad:false,securityLevel:'strict'});});
  for(const diagram of diagrams){
    const {svg,...result}=await page.evaluate(async diagram=>{
      const host=document.querySelector('#host');host.innerHTML='';
      const {svg}=await mermaid.render('m_'+diagram.id.replaceAll('-','_'),diagram.source);
      host.innerHTML=svg;
      const element=host.querySelector('svg'),box=element.viewBox.baseVal;
      element.style.maxWidth='none';element.setAttribute('width',Math.ceil(box.width));element.setAttribute('height',Math.ceil(box.height));
      const rect=el=>{const r=el.getBoundingClientRect();return {x:r.x,y:r.y,w:r.width,h:r.height,text:el.textContent.trim().slice(0,100)};};
      const intersects=(a,b)=>Math.min(a.x+a.w,b.x+b.w)-Math.max(a.x,b.x)>2&&Math.min(a.y+a.h,b.y+b.h)-Math.max(a.y,b.y)>2;
      const nodes=[...host.querySelectorAll('g.node')].map(rect);
      const labels=[...host.querySelectorAll('g.edgeLabel')].filter(e=>e.textContent.trim()).map(rect);
      const overlaps=[];
      for(const label of labels)for(const node of nodes)if(intersects(label,node))overlaps.push({label:label.text,node:node.text});
      for(let i=0;i<labels.length;i++)for(let j=i+1;j<labels.length;j++)if(intersects(labels[i],labels[j]))overlaps.push({label:labels[i].text,label2:labels[j].text});
      const texts=[...host.querySelectorAll('text')].filter(e=>e.textContent.trim()).map(rect),textOverlaps=[];
      for(let i=0;i<texts.length;i++)for(let j=i+1;j<texts.length;j++)if(intersects(texts[i],texts[j]))textOverlaps.push({a:texts[i].text,b:texts[j].text});
      const bounds=rect(element),clippedText=texts.filter(t=>t.x<bounds.x-1||t.y<bounds.y-1||t.x+t.w>bounds.x+bounds.w+1||t.y+t.h>bounds.y+bounds.h+1).map(t=>t.text);
      const labelFonts=[...new Set([...host.querySelectorAll('text,foreignObject p,foreignObject span')].filter(e=>e.textContent.trim()).map(e=>getComputedStyle(e).fontFamily))];
      return {id:diagram.id,width:box.width,height:box.height,ubuntuLoaded:document.fonts.check('17px Ubuntu'),renderedFont:getComputedStyle(element.querySelector('text')||element).fontFamily,labelFonts,overlaps,textOverlaps,clippedText,svg:new XMLSerializer().serializeToString(element)};
    },diagram);
    const embedded=svg.includes('<style>')?svg.replace('<style>',`<style>${fontCSS}\n`):svg.replace(/(<svg[^>]*>)/,`$1<style>${fontCSS}</style>`);
    fs.writeFileSync(path.join(root,'diagrams',diagram.id+'.svg'),embedded);
    results.push(result);console.log(JSON.stringify(result));
  }
}finally{await browser.close();}
const file=path.join(root,comet?'comet-guide-validation.json':spark?'spark-guide-validation.json':offsets?'kafka-offset-validation.json':recovery?'kafka-recovery-validation.json':'kafka-guide-validation.json');
const previous=JSON.parse(fs.readFileSync(file,'utf8'));
const passed=results.every(r=>r.ubuntuLoaded&&r.labelFonts.length&&r.labelFonts.every(f=>f.includes('Ubuntu'))&&!r.overlaps.length&&!r.textOverlaps.length&&!r.clippedText.length);
fs.writeFileSync(file,JSON.stringify({...previous,rendered:results,diagramGeometryPassed:passed},null,2)+'\n');
if(!passed)process.exitCode=2;
