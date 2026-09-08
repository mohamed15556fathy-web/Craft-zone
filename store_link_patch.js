/* ============================================================================
   ربط صفحة الأوردرات بموقع البيع (Craft Zone Store)
   - شريط حالة الربط + إعداد الرابط والمفتاح من نفس الصفحة
   - ترقيم الأوردرات القديمة برقم تعريفي موحد وترحيلها للموقع
   - زر "لينك دفع العميل" بيرجّع رسالة واتساب جاهزة
   ========================================================================== */
(function(){
  if (typeof authFetch !== 'function') return;
  const currentUser = JSON.parse(localStorage.getItem('user') || 'null') || {};
  const isAdmin = currentUser.username === 'admin' || currentUser.role === 'super_admin';
  const canEdit = isAdmin || Number(currentUser.perm_edit_order || 0) === 1;
  let lastStatus = null;

  function e(v){ return typeof esc === 'function' ? esc(v) : String(v ?? ''); }

  function ensureStyles(){
    if (document.getElementById('store-link-styles')) return;
    const style = document.createElement('style');
    style.id = 'store-link-styles';
    style.textContent = `
      .btn.gold{background:linear-gradient(135deg,#d59a38,#b8792b);color:#fff;border:none}
      .store-sync-bar{display:flex;flex-wrap:wrap;align-items:center;gap:10px;margin:10px 0;padding:10px 14px;border:1px solid rgba(213,154,56,.35);background:rgba(213,154,56,.08);border-radius:12px;font-size:13px}
      .store-sync-bar b{color:#f1b657}
      .store-sync-bar .pill{padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700}
      .store-sync-bar .pill.on{background:rgba(52,211,153,.16);color:#34d399}
      .store-sync-bar .pill.off{background:rgba(248,113,113,.16);color:#f87171}
      .store-sync-bar .pill.warn{background:rgba(251,191,36,.16);color:#fbbf24}
      .store-sync-bar .spacer{flex:1}
      .store-setup{width:100%;display:grid;gap:8px;margin-top:8px;padding:12px;border-radius:10px;background:rgba(3,10,20,.45);border:1px solid rgba(255,255,255,.1)}
      .store-setup .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
      .store-setup label{font-size:12px;color:#9fb3c8;min-width:110px}
      .store-setup input{flex:1;min-width:220px;padding:9px 11px;border-radius:8px;border:1px solid rgba(255,255,255,.16);background:#0a1626;color:#e6eef7;font-size:12px;direction:ltr;text-align:left}
      .store-setup .hint{font-size:11px;color:#9fb3c8;line-height:1.8}
      .store-link-modal{position:fixed;inset:0;background:rgba(3,10,20,.72);display:flex;align-items:center;justify-content:center;z-index:9999;padding:16px}
      .store-link-card{width:min(560px,100%);max-height:88vh;overflow:auto;background:#0f1d2f;border:1px solid rgba(213,154,56,.4);border-radius:16px;padding:20px;color:#e6eef7}
      .store-link-card h3{margin:0 0 6px;color:#f1b657}
      .store-link-card textarea{width:100%;min-height:170px;margin-top:10px;background:#0a1626;color:#e6eef7;border:1px solid rgba(255,255,255,.14);border-radius:10px;padding:12px;font-size:13px;line-height:1.8;direction:rtl}
      .store-link-card .links{display:grid;gap:8px;margin-top:12px}
      .store-link-card .links a{color:#7dd3fc;font-size:12px;word-break:break-all;direction:ltr;text-align:left}
      .store-link-actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}
    `;
    document.head.appendChild(style);
  }

  function closeStoreModal(){ document.querySelector('.store-link-modal')?.remove(); }

  function openStoreModal(result, order){
    ensureStyles();
    closeStoreModal();
    const phone = String(order?.custPhone || '').replace(/\D/g, '').replace(/^0/, '20');
    const waUrl = `https://wa.me/${phone}?text=${encodeURIComponent(result.whatsappText || '')}`;
    const wrap = document.createElement('div');
    wrap.className = 'store-link-modal';
    wrap.innerHTML = `
      <div class="store-link-card">
        <h3>🔗 لينك دفع العميل جاهز</h3>
        <div style="color:#9fb3c8;font-size:13px">رقم الأوردر في السيستم: <b style="color:#f1b657;direction:ltr;display:inline-block">#${e(result.displayOrderNo || order?.id || '')}</b></div>
        <textarea id="storeLinkText" readonly>${e(result.whatsappText || '')}</textarea>
        <div class="links">
          <a href="${e(result.payUrl)}" target="_blank" rel="noopener">💳 ${e(result.payUrl)}</a>
          <a href="${e(result.trackUrl)}" target="_blank" rel="noopener">📦 ${e(result.trackUrl)}</a>
        </div>
        <div class="store-link-actions">
          ${phone ? `<a class="btn green" href="${e(waUrl)}" target="_blank" rel="noopener">📲 إرسال واتساب للعميل</a>` : ''}
          <button class="btn gold" id="storePayCopy">💳 نسخ لينك الدفع</button>
          <button class="btn cyan" id="storeLinkCopy">📋 نسخ الرسالة</button>
          <button class="btn dark" id="storeLinkClose">إغلاق</button>
        </div>
      </div>`;
    document.body.appendChild(wrap);
    wrap.addEventListener('click', event => { if (event.target === wrap) closeStoreModal(); });
    wrap.querySelector('#storeLinkClose').onclick = closeStoreModal;
    wrap.querySelector('#storePayCopy').onclick = async () => {
      const value = String(result.payUrl || '');
      try { await navigator.clipboard.writeText(value); } catch (_) {
        const tmp=document.createElement('textarea');tmp.value=value;document.body.appendChild(tmp);tmp.select();document.execCommand('copy');tmp.remove();
      }
      wrap.querySelector('#storePayCopy').textContent = '✅ لينك الدفع اتنسخ';
    };
    wrap.querySelector('#storeLinkCopy').onclick = async () => {
      const box = wrap.querySelector('#storeLinkText');
      box.select();
      try { await navigator.clipboard.writeText(box.value); } catch (_) { document.execCommand('copy'); }
      wrap.querySelector('#storeLinkCopy').textContent = '✅ اتنسخت';
    };
  }

  window.sendStorePaymentLink = async function(orderId){
    if (!canEdit) return alert('ماعندكش صلاحية تعديل الأوردرات');
    // لو الربط لسه مش متظبط، نفتح لوحة الإعداد بدل رسالة خطأ مبهمة.
    if (lastStatus && !lastStatus.configured) {
      alert('لازم تظبط ربط موقع البيع الأول من صفحة الإعدادات ← ربط موقع البيع.');
      if (confirm('فتح صفحة إعدادات الربط الآن؟')) location.href='settings.html#store-link';
      return;
    }
    const order = (typeof ordersData !== 'undefined' ? ordersData : []).find(row => Number(row.id) === Number(orderId)) || {};
    if (!String(order.custPhone || '').trim()) {
      alert('ضيف رقم موبايل العميل في الأوردر الأول. رقم الموبايل مطلوب لتأمين صفحة الدفع ومطابقة الطلب.');
      return;
    }
    try {
      const data = await authFetch(`/store-payment-link/${Number(orderId)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_payment: 1 })
      });
      if (!data || data.error) throw new Error(data?.error || 'تعذر إرسال الأوردر للموقع');
      openStoreModal(data, order);
      if (typeof load === 'function') load();
    } catch (error) {
      alert(error?.message || 'تعذر إرسال الأوردر للموقع. راجع إعدادات الربط.');
    }
  };

  function setupPanelHtml(status){
    return `
      <div class="store-setup" id="storeSetupPanel" ${status.configured ? 'hidden' : ''}>
        <div class="row"><label>رابط الموقع</label><input id="storeBaseUrl" placeholder="https://craft-zone-packing-production.up.railway.app" value="${e(status.baseUrl || '')}"></div>
        <div class="row"><label>المفتاح المشترك</label><input id="storeKey" type="password" placeholder="${status.configured ? '•••••• محفوظ' : 'الصق CZ_INTEGRATION_KEY هنا'}"></div>
        <div class="row">
          <button class="btn green" id="storeSetupSave">💾 حفظ وتفعيل الربط</button>
          <span class="hint">نفس المفتاح لازم يكون متسجل على الموقع باسم <b>CZ_INTEGRATION_KEY</b>.</span>
        </div>
      </div>`;
  }

  async function renderSyncBar(){
    ensureStyles();
    let bar = document.getElementById('storeSyncBar');
    if (!bar) {
      const table = document.getElementById('ordersBody')?.closest('table');
      if (!table || !table.parentElement) return;
      bar = document.createElement('div');
      bar.id = 'storeSyncBar';
      bar.className = 'store-sync-bar';
      table.parentElement.insertBefore(bar, table);
    }
    let status = null;
    try { status = await authFetch('/store-sync-status'); } catch (_) { status = null; }
    if (!status || status.error) { bar.innerHTML = '<span class="pill off">الربط مع الموقع غير متاح</span>'; return; }
    lastStatus = status;
    const remoteOk = status.remote && status.remote.ok === true;
    const live = status.enabled && status.configured && remoteOk && !status.lastError;
    const noRef = Number(status.ordersWithoutRef || 0);
    const notPublished = Number(status.ordersNotPublished || 0);
    bar.innerHTML = `
      <span class="pill ${live ? 'on' : (status.configured ? 'warn' : 'off')}">${live ? '🔗 الربط شغّال' : (!status.configured ? '⛔ الربط لسه مش متظبط' : (!status.enabled ? '⛔ الربط متوقف — التفعيل مقفول' : (status.remote?.error ? '⚠️ الموقع رفض/تعذر الاتصال' : '⚠️ الربط متظبط بس فيه مشكلة')))}</span>
      <span>أوردرات لها رقم تعريفي: <b>${Number(status.linkedOrders || 0)}</b></span>
      ${noRef ? `<span class="pill warn">${noRef} أوردر من غير رقم</span>` : ''}
      ${notPublished ? `<span class="pill warn">${notPublished} لسه ما اترحّلش للموقع</span>` : ''}
      <span>بانتظار السحب: <b>${Number(status.remote?.pendingOrders || 0)}</b></span>
      <span>دفعات منتظرة: <b>${Number(status.remote?.pendingPayments || 0)}</b></span>
      ${status.remote?.error ? `<span class="pill off" title="${e(status.remote.error)}">${e(String(status.remote.error).slice(0, 70))}</span>` : ''}
      ${status.lastError ? `<span class="pill off" title="${e(status.lastError)}">${e(String(status.lastError).slice(0, 55))}</span>` : ''}
      <span class="spacer"></span>
      ${isAdmin ? '<button class="btn dark" id="storeSetupToggle">⚙️ إعداد الربط</button>' : ''}
      ${canEdit ? '<button class="btn gold" id="storeBackfill">🔢 ترقيم وترحيل الأوردرات القديمة</button>' : ''}
      <button class="btn cyan" id="storeSyncNow">🔄 مزامنة الآن</button>
      ${isAdmin ? setupPanelHtml(status) : ''}`;

    bar.querySelector('#storeSetupToggle')?.addEventListener('click', () => {
      const panel = bar.querySelector('#storeSetupPanel');
      if (panel) panel.hidden = !panel.hidden;
    });

    bar.querySelector('#storeSetupSave')?.addEventListener('click', async () => {
      const button = bar.querySelector('#storeSetupSave');
      const baseUrl = bar.querySelector('#storeBaseUrl').value.trim();
      const key = bar.querySelector('#storeKey').value.trim();
      if (!baseUrl) return alert('اكتب رابط الموقع الأول');
      if (!key && !status.configured) return alert('الصق المفتاح المشترك');
      button.disabled = true; button.textContent = '⏳ جاري الحفظ...';
      try {
        const result = await authFetch('/store-sync-settings', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ base_url: baseUrl, integration_key: key, enabled: 1 })
        });
        if (result?.remote?.error) alert('اتحفظ لكن اختبار الربط رجّع: ' + result.remote.error + '\n\nملحوظة: النسخة الجديدة تقبل CZ_INTEGRATION_KEY أو STORE_INTEGRATION_KEY في السيستم.');
        else alert('تم تفعيل الربط واختبار الاتصال بالموقع بنجاح ✅');
      } catch (error) {
        alert(error?.message || 'تعذر حفظ الإعدادات');
      } finally { renderSyncBar(); }
    });

    bar.querySelector('#storeBackfill')?.addEventListener('click', async () => {
      if (!confirm('هيتم إعطاء رقم تعريفي موحد لكل أوردر قديم مالوش رقم، وترحيلهم للموقع علشان العميل يقدر يتابع.\nتحب تكمل؟')) return;
      const button = bar.querySelector('#storeBackfill');
      button.disabled = true; button.textContent = '⏳ جاري الترقيم...';
      try {
        const result = await authFetch('/store-backfill-refs', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ publish: 1, publish_limit: 40 })
        });
        alert(`تم الترقيم ✅\nأوردرات اتعلمت برقم: ${result.assigned || 0}\nأرقام جديدة: ${result.groups || 0}\nاترحّلت للموقع دلوقتي: ${result.published || 0}\nالباقي هيترحّل تلقائيًا كل دورة مزامنة.`);
        if (typeof load === 'function') load();
      } catch (error) {
        alert(error?.message || 'تعذر ترقيم الأوردرات');
      } finally { renderSyncBar(); }
    });

    bar.querySelector('#storeSyncNow')?.addEventListener('click', async () => {
      const button = bar.querySelector('#storeSyncNow');
      button.disabled = true; button.textContent = '⏳ جاري المزامنة...';
      try {
        const result = await authFetch('/store-sync-now', { method: 'POST' });
        const rec = result.reconciled || {};
        alert([
          'تمت المزامنة ✅',
          `أوردرات جديدة من الموقع: ${result.importedOrders || 0}`,
          `دفعات اتسجلت: ${result.recordedPayments || 0}`,
          `تحديثات حالة: ${result.pushedStatuses || 0}`,
          `اترحّل للموقع: ${result.publishedOrders || 0}`,
          `اتحذف من الموقع فاتحذف هنا: ${result.deletedFromStore || 0}`,
          `اتحذف من هنا فاتحذف على الموقع: ${result.deletedOrders || 0}`,
          `ترقيم اتصحّح على الموقع: ${result.renumbered || 0}`,
          `أوردرات شبح اتفك ربطها: ${rec.unlinked || 0}`,
          `أوردرات ناقصة على الموقع اترجعت: ${rec.requeued || 0}`
        ].join('\n'));
        if (typeof load === 'function') load();
      } catch (error) {
        alert(error?.message || 'تعذر تشغيل المزامنة');
      } finally { renderSyncBar(); }
    });
  }

  async function boot(){
    // صفحة الأوردرات تفضل نظيفة؛ حالة الربط والمزامنة موجودة في settings.html فقط.
    // إزالة أي شريط قديم ربما أضافته نسخة JS محفوظة في Cache قبل تحديث الصفحة.
    document.querySelectorAll('#storeSyncBar,.store-sync-bar').forEach(el => el.remove());
    try { lastStatus = await authFetch('/store-sync-status'); } catch (_) { lastStatus = null; }
    document.querySelectorAll('#storeSyncBar,.store-sync-bar').forEach(el => el.remove());
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', () => setTimeout(boot, 1200), { once: true });
  else setTimeout(boot, 1200);
})();
