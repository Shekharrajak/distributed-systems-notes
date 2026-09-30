import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {fileURLToPath,pathToFileURL} from 'node:url';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const shots=process.argv[2];if(!shots)throw Error('Provide a screenshot output directory outside the published site');
fs.mkdirSync(shots,{recursive:true});
const {default:puppeteer}=await import(process.env.GUIDE_PUPPETEER_MODULE||'puppeteer');
const browser=await puppeteer.launch({executablePath:process.env.GUIDE_CHROME_BIN,headless:true,args:['--disable-gpu','--no-sandbox']});
const report={pages:[],errors:[],interactions:[]};
const validationFile=path.join(root,process.argv.includes('--spark')?'spark-guide-validation.json':process.argv.includes('--offsets')?'kafka-offset-validation.json':'kafka-recovery-validation.json');
const previous=JSON.parse(fs.readFileSync(validationFile,'utf8'));
try{
 const page=await browser.newPage();page.on('pageerror',e=>report.errors.push(e.message));
 const files=fs.readdirSync(root).filter(f=>f.endsWith('.html')&&fs.readFileSync(path.join(root,f),'utf8').includes('aria-label="Chapters"'));
 for(const width of [1440,390]){
  await page.setViewport({width,height:1000,deviceScaleFactor:1});
  for(const file of files){
   await page.goto(pathToFileURL(path.join(root,file)).href,{waitUntil:'load'});
   await page.evaluate(async()=>{await document.fonts.ready;await Promise.all([...document.images].map(i=>{i.loading='eager';return i.decode().catch(()=>{});}));});
   const result=await page.evaluate(()=>({overflow:document.documentElement.scrollWidth>innerWidth+1,brokenImages:[...document.images].filter(i=>!i.naturalWidth).map(i=>i.getAttribute('src')),activeNav:document.querySelector('[aria-current="page"]')?.dataset.nav,fontLoaded:document.fonts.check('16px Ubuntu')}));
   report.pages.push({file,width,...result});
   assert(!result.overflow,`${file} overflows at ${width}`);assert.equal(result.brokenImages.length,0);assert.equal(result.activeNav,file);assert(result.fontLoaded);
   if(previous.pages.includes(file)){
    await page.screenshot({path:path.join(shots,`${file}-${width}.png`)});
    const before=await page.$eval('.diagram img',i=>i.getBoundingClientRect().width);
    await page.click('.diagram [data-zoom="in"]');assert((await page.$eval('.diagram img',i=>i.getBoundingClientRect().width))>before);
    await page.click('.diagram [data-zoom="fit"]');
    const id=await page.$eval('details.code',d=>d.id);
    await page.evaluate(id=>{location.hash=id;},id);await page.waitForFunction(id=>document.getElementById(id).open,{},id);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    report.interactions.push({file,width,zoom:true,sourceAnchorExpansion:true});
   }
  }
 }
 await page.setViewport({width:1600,height:1200,deviceScaleFactor:1});
 for(const id of previous.diagrams){
  await page.goto(pathToFileURL(path.join(root,'diagrams',id+'.svg')).href,{waitUntil:'load'});
  await page.evaluate(async()=>{await document.fonts.ready;});
  await (await page.$('svg')).screenshot({path:path.join(shots,id+'.png')});
 }
 assert.equal(report.errors.length,0);
}finally{await browser.close();}
fs.writeFileSync(validationFile,JSON.stringify({...previous,artifactChecks:report,artifactChecksPassed:true},null,2)+'\n');
console.log(JSON.stringify({pageLayouts:report.pages.length,interactionChecks:report.interactions.length,errors:report.errors,screenshots:shots},null,2));
