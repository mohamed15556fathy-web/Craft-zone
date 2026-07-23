(function(){
  if(window.__craftZoneIdleGuardLoaded) return;
  window.__craftZoneIdleGuardLoaded = true;
  const token = localStorage.getItem('token');
  if(!token) return;
  const apiHeaders = { Authorization:'Bearer '+token, 'Content-Type':'application/json' };
  let idleMs = 180000;
  let locationEnabled = true;
  let lastActivity = Date.now();
  let dirty = true;
  let loggedOut = false;
  function redirectLogin(){ if(loggedOut) return; loggedOut=true; localStorage.clear(); location.href='login.html'; }
  async function post(url, body){
    try{
      const r = await fetch(url,{method:'POST',headers:apiHeaders,body:JSON.stringify(body||{})});
      if(r.status===401) redirectLogin();
      return r.ok;
    }catch(_){ return false; }
  }
  function touch(){ lastActivity = Date.now(); dirty = true; }
  ['click','keydown','mousemove','scroll','touchstart','input'].forEach(ev=>document.addEventListener(ev,touch,{passive:true}));
  async function loadSettings(){
    try{
      const r = await fetch('/session-settings',{headers:{Authorization:'Bearer '+token}});
      if(r.status===401) return redirectLogin();
      if(r.ok){
        const d = await r.json().catch(()=>({}));
        const seconds = Math.max(60, Number(d.idle_timeout_seconds||180));
        idleMs = seconds*1000;
        locationEnabled = String(d.location_tracking_enabled||'1') !== '0';
      }
    }catch(_){ }
  }
  async function ping(){
    if(Date.now()-lastActivity >= idleMs){
      await post('/logout',{reason:'idle_timeout'});
      return redirectLogin();
    }
    if(dirty){
      await post('/session-ping',{active_at:new Date().toISOString()});
      dirty = false;
    }
  }
  function sendLocationOnce(){
    if(!locationEnabled || !navigator.geolocation) return;
    const key = 'cz_location_sent_' + token.slice(0,18);
    if(sessionStorage.getItem(key)==='1') return;
    navigator.geolocation.getCurrentPosition(function(pos){
      sessionStorage.setItem(key,'1');
      post('/session-location',{
        latitude: pos.coords.latitude,
        longitude: pos.coords.longitude,
        accuracy: pos.coords.accuracy,
        at: new Date().toISOString()
      });
    }, function(){ sessionStorage.setItem(key,'denied'); }, {enableHighAccuracy:true, timeout:15000, maximumAge:300000});
  }
  loadSettings().then(()=>{ sendLocationOnce(); ping(); });
  setInterval(ping, 30000);
})();

// إصلاح حركة + / - في صفحة الورق القديمة بدون تغيير باقي النظام.
(function installPaperQuickMoveRepair(){
  if(!document.getElementById('pBody') || typeof window.quickMovePaper !== 'function') return;
  window.quickMovePaper = async function quickMovePaperFixed(id, dir){
    const p = paperData.find(row => Number(row.id) === Number(id));
    if(!p) return;
    let kg = toNum(document.getElementById('k'+id)?.value || 0);
    let sheets = toNum(document.getElementById('s'+id)?.value || 0);
    if(kg <= 0 && sheets <= 0) return alert('اكتب كجم أو فرخ');
    if(kg <= 0 && sheets > 0) kg = toNum(sheetsToKg(sheets,p.length,p.width,p.grammage));
    if(sheets <= 0 && kg > 0) sheets = toNum(kgToSheets(kg,p.length,p.width,p.grammage));
    if(dir < 0){
      const sheetBalance = toNum(p.total_sheets);
      const kgBalance = toNum(p.total_kg);
      const enoughBySheets = sheets > 0 && sheetBalance > 0 && sheets <= sheetBalance + 0.0001;
      const enoughByKg = kg > 0 && kgBalance > 0 && kg <= kgBalance + 0.01;
      if(!enoughBySheets && !enoughByKg && ((sheets > 0 && sheets > sheetBalance + 0.0001) || (kg > 0 && kg > kgBalance + 0.01))){
        return alert('الكمية أكبر من الرصيد');
      }
    }
    try{
      const result = await authFetch('/add-paper',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          movement_id:p.id,
          length:p.length,
          width:p.width,
          grammage:p.grammage,
          color:p.color,
          paper_type:normalizePaperType(p.paper_type),
          paper_name:p.paper_name || '',
          kg:dir * kg,
          sheets:dir * sheets,
          min_kg:p.min_kg,
          min_sheets:p.min_sheets,
          buy_price_kg:p.buy_price_kg
        })
      });
      if(result?.paper){
        const index = paperData.findIndex(row => Number(row.id) === Number(result.paper.id));
        if(index >= 0) paperData[index] = {...paperData[index], ...result.paper};
      }
      await load();
      const kEl = document.getElementById('k'+id);
      const sEl = document.getElementById('s'+id);
      if(kEl) kEl.value = '';
      if(sEl) sEl.value = '';
    }catch(e){ alert(e.message); }
  };
})();
