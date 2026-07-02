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
