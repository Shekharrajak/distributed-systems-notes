const search = document.querySelector('#rpc-search');
const family = document.querySelector('#rpc-family');
const entries = [...document.querySelectorAll('.rpc-schema')].map(element=>({element,text:element.textContent.toLowerCase()}));
function filterSchemas(){
  const query=(search?.value||'').trim().toLowerCase();
  let count=0;
  for(const {element,text} of entries){
    const matches=/^\d+$/.test(query)?element.id===`rpc-${query}`:(!query||text.includes(query));
    const visible=matches&&(!family.value||element.dataset.family===family.value);
    element.hidden=!visible;
    if(visible)count++;
  }
  document.querySelector('#rpc-count').textContent=`${count} of ${entries.length} API pairs shown`;
  document.querySelector('#rpc-empty').hidden=count!==0;
}
search?.addEventListener('input',filterSchemas);
family?.addEventListener('change',filterSchemas);
document.querySelector('#rpc-reset')?.addEventListener('click',()=>{search.value='';family.value='';filterSchemas();});
function revealSchema(){
  const element=document.getElementById(location.hash.slice(1));
  if(element?.classList.contains('rpc-schema')){
    search.value='';family.value='';filterSchemas();element.open=true;element.scrollIntoView({block:'start'});
  }
}
if(entries.length){filterSchemas();revealSchema();window.addEventListener('hashchange',revealSchema);}
