(function(){
  if (typeof authFetch !== 'function') return;
  const currentUser = JSON.parse(localStorage.getItem('user') || 'null') || {};
  let actorUsers = [];
  let actorCurrent = String(currentUser.username || '').trim();
  let canActAsOther = currentUser.role === 'admin' || Number(currentUser.perm_act_as_other_admin || 0) === 1;

  function e(v){ return typeof esc === 'function' ? esc(v) : String(v ?? ''); }
  function byId(id){ return document.getElementById(id); }
  async function loadActorUsers(){
    try {
      const data = await authFetch('/active-users-lite');
      actorUsers = Array.isArray(data.users) ? data.users : [];
      actorCurrent = String(data.current_user || actorCurrent || '').trim();
      canActAsOther = currentUser.role === 'admin' || Number(currentUser.perm_act_as_other_admin || 0) === 1 || Number(data.can_act_as_other || 0) === 1;
    } catch (e) {
      actorUsers = [];
    }
  }
  function actorOptions(selected=''){
    const sel = String(selected || '').trim();
    let list = canActAsOther ? actorUsers.slice() : actorUsers.filter(u => String(u.username || '') === actorCurrent);
    const selectedRow = actorUsers.find(u => String(u.username||'') === sel);
    if (sel && selectedRow && !list.some(u => String(u.username||'') === sel)) list.push(selectedRow);
    const opts = [`<option value="">على الشركة / بدون عهدة أدمن</option>`];
    for (const row of list){
      const username = String(row.username || '').trim();
      if (!username) continue;
      const label = String(row.full_name || row.username || '').trim() || username;
      opts.push(`<option value="${e(username)}" ${sel===username?'selected':''}>${e(label)}</option>`);
    }
    return opts.join('');
  }
  function directionRow(stepType=''){
    return Array.isArray(currentDirectionData?.steps) ? currentDirectionData.steps.find(r => String(r.step_type || '').trim() === String(stepType || '').trim()) : null;
  }
  function ensureDirectionActorSelect(prefix, stepType){
    const section = byId(prefix + '_section');
    if (!section) return;
    let wrap = byId(prefix + '_actor_wrap');
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.id = prefix + '_actor_wrap';
      wrap.innerHTML = `<label>على عهدة الأدمن</label><select id="${prefix}_actor"></select><small class="direction-note">اختياري. لو الأدمن هو اللي دفع تكلفة المرحلة من فلوسه اختاره هنا.</small>`;
      const amount = byId(prefix + '_amount');
      if (amount) amount.insertAdjacentElement('afterend', wrap);
      else section.appendChild(wrap);
    }
    const row = directionRow(stepType);
    const selected = row?.actor_username || '';
    const select = byId(prefix + '_actor');
    if (select) select.innerHTML = actorOptions(selected);
  }
  function injectAllDirectionActorSelects(){
    ensureDirectionActorSelect('dir_print', 'print');
    ensureDirectionActorSelect('dir_make', 'make');
    ensureDirectionActorSelect('dir_handle', 'handle');
  }

  const oldOpenDirectionModal = window.openDirectionModal;
  if (typeof oldOpenDirectionModal === 'function') {
    window.openDirectionModal = async function(id){
      await loadActorUsers();
      await oldOpenDirectionModal(id);
      injectAllDirectionActorSelects();
    };
  }
  const oldCollectDirectionPayload = window.collectDirectionPayload;
  if (typeof oldCollectDirectionPayload === 'function') {
    window.collectDirectionPayload = function(prefix, stepType){
      const body = oldCollectDirectionPayload(prefix, stepType);
      if (!body) return body;
      body.actor_username = byId(prefix + '_actor')?.value || '';
      return body;
    };
  }
})();
