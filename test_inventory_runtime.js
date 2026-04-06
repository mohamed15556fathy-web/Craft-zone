const fs = require('fs');
const vm = require('vm');
const html = fs.readFileSync('/mnt/data/proj/inventory.html','utf8');
const js = html.slice(html.indexOf('<script>')+8, html.lastIndexOf('</script>'));
const ids = [...html.matchAll(/id="([^"]+)"/g)].map(m=>m[1]);

function makeEl(id){
  return {
    id,
    value:'',
    innerHTML:'',
    innerText:'',
    textContent:'',
    style:{display:'',opacity:'',cursor:''},
    className:'',
    disabled:false,
    children:[],
    scrollTop:0,
    classList:{
      toggle(cls, force){
        const set = new Set((this._owner.className||'').split(/\s+/).filter(Boolean));
        const shouldAdd = force===undefined ? !set.has(cls) : !!force;
        if(shouldAdd) set.add(cls); else set.delete(cls);
        this._owner.className = [...set].join(' ');
      },
      contains(cls){ return (this._owner.className||'').split(/\s+/).includes(cls); }
    }
  };
}
const elements = Object.fromEntries(ids.map(id=>{
  const el = makeEl(id);
  el.classList._owner = el;
  return [id, el];
}));

const document = {
  getElementById(id){ return elements[id] || null; }
};

const localStore = new Map([
  ['token','abc'],
  ['user', JSON.stringify({username:'admin', perm_view_inventory:1, full_name:'Admin'})]
]);
const localStorage = {
  getItem(k){ return localStore.has(k) ? localStore.get(k) : null; },
  setItem(k,v){ localStore.set(k,String(v)); },
  clear(){ localStore.clear(); }
};

async function fetch(url, opts={}){
  let body;
  if (url === '/get-paper') body = [{id:1,length:120,width:86,grammage:90,color:'أبيض',paper_name:'ناعم',total_kg:10,total_sheets:100,min_kg:0,min_sheets:0,buy_price_kg:50,buy_price_sheet:5}];
  else if (url === '/get-bags') body = [];
  else if (url === '/cuttable-orders') body = [];
  else if (url === '/get-paper-history') body = [];
  else body = {};
  return {
    status:200,
    ok:true,
    async json(){ return body; }
  };
}

const sandbox = {
  console,
  document,
  localStorage,
  fetch,
  location:{href:''},
  alert:(msg)=>{ sandbox.__alerts.push(String(msg)); },
  confirm:()=>true,
  window:null,
  __alerts:[],
  scrollTo(){},
  setTimeout, clearTimeout
};
sandbox.window = sandbox;
for (const [id, el] of Object.entries(elements)) sandbox[id] = el;
vm.createContext(sandbox);
(async()=>{
  vm.runInContext(js, sandbox);
  await new Promise(r=>setTimeout(r,10));
  if (typeof sandbox.openCut !== 'function') throw new Error('openCut missing');
  sandbox.openCut(1);
  await new Promise(r=>setTimeout(r,10));
  if (elements.cutModal.style.display !== 'block') throw new Error('cut modal did not open');
  console.log('runtime test ok');
})().catch(err=>{ console.error(err); process.exit(1); });
