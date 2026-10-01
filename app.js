const shareTransactionNav = document.createElement('a');
shareTransactionNav.href = 'kafka-share-transactions-multi-broker.html';
shareTransactionNav.dataset.nav = 'kafka-share-transactions-multi-broker.html';
shareTransactionNav.innerHTML = '<span>27</span>Share transactions across many brokers';
document.querySelector('.sidebar nav')?.append(shareTransactionNav);
document.querySelectorAll('[data-nav]').forEach(a=>{if(a.dataset.nav===document.body.dataset.page)a.setAttribute('aria-current','page');});
document.querySelector('#chapter-filter')?.addEventListener('input',e=>{document.querySelectorAll('[data-nav]').forEach(a=>{a.hidden=!a.textContent.toLowerCase().includes(e.target.value.toLowerCase());});});
document.querySelectorAll('.diagram-scroll img').forEach(img=>{
  const size=()=>{
    const available=img.parentElement.clientWidth-48;
    img.style.minWidth='0';
    img.style.width=Math.max(Math.min(img.naturalWidth,available),img.naturalWidth*.9)+'px';
  };
  if(img.complete && img.naturalWidth)size();else img.addEventListener('load',size,{once:true});
});
document.querySelectorAll('[data-zoom]').forEach(button=>button.addEventListener('click',()=>{
  const box=button.closest('.diagram').querySelector('.diagram-scroll');
  const img=box.querySelector('img');
  let width=img.getBoundingClientRect().width;
  if(button.dataset.zoom==='in')width*=1.25;
  if(button.dataset.zoom==='out')width/=1.25;
  if(button.dataset.zoom==='actual')width=img.naturalWidth;
  if(button.dataset.zoom==='fit')width=box.clientWidth-48;
  img.style.minWidth='0';img.style.width=Math.max(260,Math.min(width,6000))+'px';
}));
if(location.hash){const el=document.getElementById(location.hash.slice(1));if(el?.tagName==='DETAILS')el.open=true;}
window.addEventListener('hashchange',()=>{const el=document.getElementById(location.hash.slice(1));if(el?.tagName==='DETAILS')el.open=true;});
