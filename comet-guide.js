(() => {
 const input=document.querySelector('#query-filter');
 if(!input)return;
 const sections=[...document.querySelectorAll('.query-section')];
 const index=sections.map(section=>({section,text:(section.id+' '+section.querySelector('h2').textContent+' '+[...section.querySelectorAll(':scope > p')].map(p=>p.textContent).join(' ')).toLowerCase()}));
 const filter=()=>{const term=input.value.trim().toLowerCase();let visible=0;for(const {section,text} of index){section.hidden=term!==''&&!text.includes(term);if(!section.hidden)visible++;}document.querySelector('#query-count').textContent=`${visible} of 22 queries`;};
 input.addEventListener('input',filter);
 const reveal=()=>{const id=location.hash.slice(1);if(/^q\d+$/.test(id)){input.value='';filter();document.getElementById(id)?.scrollIntoView();}};
 addEventListener('hashchange',reveal);reveal();
})();
