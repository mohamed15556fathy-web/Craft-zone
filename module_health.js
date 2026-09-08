'use strict';
/* ============================================================================
   Craft Zone — محرّك الفحص والإصلاح (v9.9)
   ----------------------------------------------------------------------------
   ليه اتعمل من الأول؟ الفحص القديم كان بيلاقي المشاكل بس الإصلاح مكانش بيشتغل:

     1. مشاكل معلّمة «error» ومالهاش أي طريق إصلاح أصلاً (خصم ورق بدون صنف).
     2. إصلاح الفروقات كان متقفول بالمفتاح `repairMismatch:false` في كل مكان،
        فمشكلة «فرق بين سعر التكلفة وحركة الخزنة» كانت بتفضل للأبد.
     3. إصلاحات كانت بتكتب في سجل المخزن من غير ما تعدّل الكمية الفعلية.
     4. لو الإصلاح اتخطى مشكلة، السبب مكانش بيوصل للمستخدم.
     5. اللوب كان بيقف بعد باسة واحدة لو الإصلاح ولّد مشكلة جديدة.
     6. زرار «إصلاح» في كل سطر كان بيشغّل كل حاجة على الأوردر بدل المشكلة نفسها.
     7. التوزيع بين تلقائي/يدوي مكانش متسق.
     8. الربط بين المشكلة والإصلاح كان بمطابقة نص عربي داخل العنوان.
     9. نتيجة الفحص كانت بتتمسح من الشاشة مع أول ضغطة على أي حاجة تانية.
    10. مفيش أي معاملة (transaction) حوالين الإصلاح.
    11. دالة مزامنة أعلام المخزون موجودة ومحدش بينده عليها.

   الحل هنا: كل مشكلة ليها مفتاح ثابت (repair_key) مربوط بدالة إصلاح مسجّلة في
   جدول واحد، والإصلاح بيرجّع بالظبط اتعمل إيه واتخطى إيه وليه، والفحص بيتعاد
   بعد كل إصلاح علشان النتيجة تبقى فورية وصادقة.
   ========================================================================== */

function register(ctx) {
  const {
    app, runAsync, getAsync, allAsync,
    authRequired, requirePerm, requireAnyPerm,
    num, roundMoney, recordAudit,
    legacyHealthCheck,          // runSystemHealthCheck الأصلية
    legacyRepairAll,            // repairDetectedSystemHealthIssues
    legacyRepairOrder,          // repairSystemUntilStable لأوردر واحد
    legacySyncInventoryFlags,   // syncInventoryDeductionFlagsFromHistories (كانت مش متندهة)
    legacyRebuildAdminCash,
    legacyRepairPaper, legacyRepairHandle, legacyRepairBag,
    legacyRepairCosts, legacySyncDueCosts,
    syncFixedAssetCash, syncCostSnapshots, syncSalesHistory, normalizeReturnedBalances,
    syncOrderPaymentCash, syncPurchasePaymentCash, syncExpenseCash,
    modules,                    // { workshop, traders, partners }
  } = ctx;

  const clean = (v) => String(v ?? '').trim();

  /* ===================== 1) فحوصات إضافية (حسابات ومخازن وخزنة) ===================== */
  async function coreExtraChecks() {
    const issues = [];
    const failures = [];
    const push = (severity, title, details, data = {}) => issues.push({ severity, title, details, data: { module: 'core', ...data } });
    /* استعلام آمن: لو الجدول أو العمود مش موجود في نسخة قاعدة بيانات قديمة،
       الفحص ما يقعش — بس الفشل بيتسجّل ويظهر للمستخدم بدل ما يختفي بالساكت. */
    const safe = async (label, promise, fallback) => {
      try { return await promise; }
      catch (e) { failures.push(`${label}: ${e.message}`); return fallback; }
    };

    /* --- المخازن: كميات سالبة --- */
    for (const [table, qtyField, nameField, label] of [
      ['paper', 'total_kg', 'paper_name', 'ورق (كجم)'],
      ['paper', 'total_sheets', 'paper_name', 'ورق (فرخ)'],
      ['handles', 'qty', 'color', 'يد'],
      ['bags', 'total_qty', 'color', 'شنط جاهزة'],
    ]) {
      try {
        const rows = await allAsync(`SELECT id, ${nameField} nm, ${qtyField} q FROM ${table} WHERE COALESCE(${qtyField},0) < 0 LIMIT 100`);
        if (rows.length) push('error', `رصيد ${label} بالسالب في المخزن`,
          `${rows.length} صنف رصيده سالب: ${rows.slice(0, 5).map(r => `${r.nm} (${roundMoney(num(r.q))})`).join('، ')}`,
          { repair_mode: 'manual', action_hint: `افتح مخزن الـ${label} وصحّح الرصيد أو راجع سجل الخصم.`, affected_count: rows.length });
      } catch (_) { /* الجدول مش موجود */ }
    }

    /* --- الأعلام مقابل السجلات (الدالة اللي كانت مهملة) --- */
    const flagDrift = await getAsync(
      `SELECT COUNT(*) c FROM orders o
       WHERE COALESCE(o.paper_cut_done,0)=1 AND NOT EXISTS (SELECT 1 FROM paper_history p WHERE p.order_id=o.id)`).catch(() => null);
    if (num(flagDrift?.c)) push('warn', 'أعلام خصم المخزون مش متطابقة مع السجلات',
      `${num(flagDrift.c)} أوردر معلّم إن الورق اتخصم منه بس مفيش سجل خصم.`,
      { repair_mode: 'auto', repair_key: 'core_sync_inventory_flags', affected_count: num(flagDrift.c) });

    /* --- الخزنة: أرصدة العهدة الجارية --- */
    const cashRows = await allAsync(`SELECT admin_username, id, delta, balance_after FROM admin_cash_ledger ORDER BY admin_username, date(entry_date) ASC, id ASC`);
    const running = {};
    let cashDrift = 0;
    for (const row of cashRows) {
      const u = clean(row.admin_username);
      running[u] = roundMoney(num(running[u]) + num(row.delta));
      if (Math.abs(running[u] - num(row.balance_after)) > 0.01) cashDrift++;
    }
    if (cashDrift) push('warn', 'أرصدة العهدة الجارية محتاجة إعادة بناء',
      `${cashDrift} سطر رصيده الجاري مش مظبوط في سجل العهدة.`,
      { repair_mode: 'auto', repair_key: 'core_rebuild_admin_cash', affected_count: cashDrift });

    /* --- عهدة لمستخدم محذوف --- */
    const ghostCash = await allAsync(
      `SELECT DISTINCT admin_username FROM admin_cash_ledger
       WHERE admin_username <> '__cashbox__' AND admin_username NOT IN (SELECT username FROM users) LIMIT 50`);
    if (ghostCash.length) push('error', 'عهدة مسجّلة لمستخدم غير موجود',
      `${ghostCash.length} مستخدم: ${ghostCash.map(g => g.admin_username).join('، ')}`,
      { repair_mode: 'manual', action_hint: 'رجّع المستخدم بنفس اسم الدخول أو حوّل عهدته لمستخدم تاني.' });

    /* --- دفعات مشتريات أكبر من قيمة الفاتورة --- */
    const overPaid = await safe('دفعات المشتريات', allAsync(
      `SELECT p.id, p.total_price, COALESCE(SUM(pp.amount),0) paid
       FROM purchases p LEFT JOIN purchase_payments pp ON pp.purchase_id=p.id
       GROUP BY p.id HAVING paid > COALESCE(p.total_price,0) + 0.01 LIMIT 100`), []);
    if (overPaid.length) push('warn', 'فاتورة مشتريات مدفوع فيها أكتر من قيمتها',
      `${overPaid.length} فاتورة: ${overPaid.slice(0, 5).map(r => `#${r.id} (دفع ${roundMoney(num(r.paid))} من ${roundMoney(num(r.total_price))})`).join('، ')}`,
      { repair_mode: 'manual', action_hint: 'راجع دفعات الفاتورة من صفحة المشتريات.' });

    /* --- أوردر مدفوع أكتر من إجماليه --- */
    const overPaidOrders = await safe('أوردرات مدفوعة بالزيادة', allAsync(
      `SELECT id, group_code, total_price, paid_amount FROM orders
       WHERE COALESCE(paid_amount,0) > COALESCE(total_price,0) + 0.01
         AND (TRIM(COALESCE(group_code,''))='' OR COALESCE(item_no,1)=1) LIMIT 100`), []);
    if (overPaidOrders.length) push('warn', 'أوردر مدفوع فيه أكتر من إجماليه',
      `${overPaidOrders.length} أوردر: ${overPaidOrders.slice(0, 5).map(r => `#${r.id}`).join('، ')}`,
      { repair_mode: 'manual', action_hint: 'راجع دفعات الأوردر أو صحّح الإجمالي.', affected_count: overPaidOrders.length });

    /* --- المتبقي مش مساوي الإجمالي ناقص المدفوع --- */
    const remainingDrift = await safe('المتبقي على الأوردرات', allAsync(
      `SELECT id, total_price, paid_amount, remaining_amount FROM orders
       WHERE ABS(COALESCE(remaining_amount,0) - (COALESCE(total_price,0) - COALESCE(paid_amount,0))) > 0.01
         AND TRIM(COALESCE(status,''))<>'مرتجع'
         AND (TRIM(COALESCE(group_code,''))='' OR COALESCE(item_no,1)=1) LIMIT 200`), []);
    if (remainingDrift.length) push('warn', 'المتبقي على الأوردر مش مظبوط',
      `${remainingDrift.length} أوردر المتبقي فيه مش بيساوي الإجمالي ناقص المدفوع.`,
      { repair_mode: 'auto', repair_key: 'core_fix_remaining', affected_count: remainingDrift.length });

    /* --- الأصول الثابتة وحركة الخزنة --- */
    const badAssets = await safe('حركات الأصول الثابتة', allAsync(
      `SELECT a.id,a.asset_name,a.purchase_price,COUNT(l.id) ledger_count,COALESCE(SUM(l.delta),0) ledger_delta
       FROM fixed_assets a LEFT JOIN admin_cash_ledger l ON l.source_type='fixed_asset' AND l.source_ref=CAST(a.id AS TEXT)
       GROUP BY a.id HAVING COUNT(l.id)<>1 OR ABS(COALESCE(SUM(l.delta),0)+COALESCE(a.purchase_price,0))>0.01 LIMIT 200`), []);
    if (badAssets.length) push('error', 'أصل ثابت بدون خصم صحيح أو بحركة مكررة',
      `${badAssets.length} أصل لا يطابق سعره حركة خزنة واحدة سالبة.`,
      { repair_mode:'auto', repair_key:'core_sync_fixed_assets', affected_count:badAssets.length, rows:badAssets.slice(0,20) });

    const orphanAssetCash = await safe('حركات أصول يتيمة', allAsync(
      `SELECT l.id,l.source_ref,l.delta FROM admin_cash_ledger l LEFT JOIN fixed_assets a ON CAST(a.id AS TEXT)=l.source_ref
       WHERE l.source_type='fixed_asset' AND a.id IS NULL LIMIT 100`), []);
    if (orphanAssetCash.length) push('error', 'حركة أصل ثابت يتيمة في الخزنة', `${orphanAssetCash.length} حركة مرتبطة بأصل محذوف.`,
      { repair_mode:'auto', repair_key:'core_sync_fixed_assets', affected_count:orphanAssetCash.length });

    /* --- الورق: الوحدة والتحويل والأسعار --- */
    const badPaperUnits = await safe('وحدات شراء الورق', allAsync(
      `SELECT id,unit,quantity,paper_length,paper_width,paper_grammage FROM purchases
       WHERE TRIM(COALESCE(item_type,''))='ورق' AND TRIM(COALESCE(unit,'')) NOT IN ('كجم','فرخ') LIMIT 100`), []);
    if (badPaperUnits.length) push('error', 'شراء ورق بوحدة غير منطقية', `${badPaperUnits.length} فاتورة ورق وحدتها ليست كجم أو فرخ.`,
      { repair_mode:'manual', action_hint:'صحّح وحدة الإدخال الأصلية؛ لا تحوّل عدد الأفراخ إلى كيلوجرامات يدويًا.', affected_count:badPaperUnits.length });
    const paperConversionDrift = await safe('تحويلات شراء الورق', allAsync(
      `SELECT id,unit,quantity,unit_price,total_price,paper_sheet_weight_kg,paper_weight_kg,paper_sheets_equivalent,paper_unit_price_kg,paper_unit_price_sheet
       FROM purchases WHERE TRIM(COALESCE(item_type,''))='ورق' AND (
         ABS(COALESCE(total_price,0)-COALESCE(quantity,0)*COALESCE(unit_price,0))>0.02 OR
         COALESCE(paper_sheet_weight_kg,0)<=0 OR
         (unit='فرخ' AND (ABS(COALESCE(paper_sheets_equivalent,0)-COALESCE(quantity,0))>0.001 OR ABS(COALESCE(paper_weight_kg,0)-COALESCE(quantity,0)*COALESCE(paper_sheet_weight_kg,0))>0.01)) OR
         (unit='كجم' AND ABS(COALESCE(paper_weight_kg,0)-COALESCE(quantity,0))>0.001)
       ) LIMIT 100`), []);
    if (paperConversionDrift.length) push('error', 'تحويل الفرخ والكيلو في المشتريات غير متوافق', `${paperConversionDrift.length} فاتورة لا تطابق المقاس والجرام ووحدة المستخدم.`,
      { repair_mode:'manual', action_hint:'راجع المقاس والجرام والوحدة؛ إعادة بناء المخزون لا يجوز أن تخمّن الوحدة.', affected_count:paperConversionDrift.length });
    const missingPaperPurchaseHistory = await safe('حركة شراء الورق في السجل', allAsync(
      `SELECT p.id,p.stock_ref_id FROM purchases p LEFT JOIN paper_history h ON h.source_type='purchase' AND h.source_ref=CAST(p.id AS TEXT)
       WHERE TRIM(COALESCE(p.item_type,''))='ورق' AND TRIM(COALESCE(p.stock_type,''))='paper'
         AND COALESCE(p.stock_applied,0)=1 AND h.id IS NULL LIMIT 100`), []);
    if (missingPaperPurchaseHistory.length) push('error', 'شراء ورق مطبق بدون حركة تاريخية مرتبطة', `${missingPaperPurchaseHistory.length} فاتورة تحتاج ربط حركة Paper History بمفتاح المصدر.`,
      { repair_mode:'manual', action_hint:'شغّل Reconciliation بعد التأكد من أن حركة الشراء القديمة موجودة؛ لا تعد إضافة المخزون.', affected_count:missingPaperPurchaseHistory.length });
    const paperPriceDrift = await safe('سعر كيلو وفرخ الورق', allAsync(
      `SELECT id,paper_name,length,width,grammage,buy_price_kg,buy_price_sheet FROM paper
       WHERE length>0 AND width>0 AND grammage>0 AND buy_price_kg>0
         AND ABS(COALESCE(buy_price_sheet,0)-(length*width*grammage/10000000.0)*buy_price_kg)>0.02 LIMIT 100`), []);
    if (paperPriceDrift.length) push('warn', 'سعر الكيلو وسعر الفرخ غير متوافقين', `${paperPriceDrift.length} صنف ورق سعره لا يطابق وزنه.`,
      { repair_mode:'manual', action_hint:'راجع آخر فاتورة شراء لهذا الصنف.', affected_count:paperPriceDrift.length });

    /* --- الفورم: التصنيف الفني وتصنيف الفرخ الصريح --- */
    const invalidForms = await safe('تصنيفات الفورم', allAsync(
      `SELECT id,form_code,product_type,form_family,sheet_class FROM forms
       WHERE TRIM(COALESCE(form_family,'')) NOT IN ('bag','box','pouch','other')
          OR TRIM(COALESCE(sheet_class,'')) NOT IN ('quarter','half','full') LIMIT 100`), []);
    if (invalidForms.length) push('error', 'فورمة بدون تصنيف فني صريح', `${invalidForms.length} فورمة تحتاج تحديد نوعها الفني وتصنيف الفرخ يدويًا.`,
      { repair_mode:'manual', action_hint:'افتح الفورمة واختر التصنيف الفني وربع/نصف/فرخ كامل؛ لا تعتمد على المقاس وحده.', affected_count:invalidForms.length });
    const formSnapshotDrift = await safe('تصنيف الفورمة داخل الأوردر', allAsync(
      `SELECT o.id,o.form_id,o.form_family_snapshot,f.form_family
       FROM orders o JOIN forms f ON f.id=o.form_id
       WHERE COALESCE(o.form_id,0)>0 AND (
         TRIM(COALESCE(o.form_family_snapshot,'')) NOT IN ('bag','box','pouch','other') OR
         TRIM(COALESCE(o.form_family_snapshot,''))<>TRIM(COALESCE(f.form_family,'')) OR
         TRIM(COALESCE(o.breaking_sheet_class,''))<>TRIM(COALESCE(f.sheet_class,''))
       ) LIMIT 200`), []);
    if (formSnapshotDrift.length) push('warn', 'Snapshot الفورمة داخل الأوردر غير متوافق', `${formSnapshotDrift.length} أوردر لا يحمل التصنيف الصريح الحالي للفورمة.`,
      { repair_mode:'manual', action_hint:'راجع الفورمة ثم احفظ تعديل الأوردر لإعادة مزامنة الـSnapshot بدون لمس التكلفة اليدوية.', affected_count:formSnapshotDrift.length });

    /* --- قواعد حالة الأوردر والمجموعة --- */
    const deliveredZero = await safe('تسليم بسعر صفر', allAsync(
      `SELECT id,group_code,item_no,total_price FROM orders WHERE TRIM(COALESCE(status,''))='تم التسليم'
       AND COALESCE(item_no,1)=1 AND COALESCE(total_price,0)<=0 LIMIT 100`), []);
    if (deliveredZero.length) push('error', 'أوردر مسلم بسعر صفر', `${deliveredZero.length} قائد أوردر/مجموعة تم تسليمه بلا سعر.`,
      { repair_mode:'manual', action_hint:'صحّح إجمالي البيع فقط؛ لا تنشئ دفعة وهمية.', affected_count:deliveredZero.length });
    const returnedDebt = await safe('مديونية المرتجعات', allAsync(
      `SELECT id,group_code,total_price,paid_amount,remaining_amount FROM orders WHERE TRIM(COALESCE(status,''))='مرتجع' AND ABS(COALESCE(remaining_amount,0))>0.01 LIMIT 200`), []);
    if (returnedDebt.length) push('error', 'مرتجع ما زالت عليه مديونية', `${returnedDebt.length} صنف مرتجع المتبقي عليه ليس صفرًا.`,
      { repair_mode:'auto', repair_key:'core_fix_returned_remaining', affected_count:returnedDebt.length });
    const childFinance = await safe('مالية الأصناف الفرعية', allAsync(
      `SELECT id,group_code,item_no,total_price,paid_amount,remaining_amount,shipping_cost FROM orders
       WHERE TRIM(COALESCE(group_code,''))<>'' AND COALESCE(item_no,1)>1
         AND (ABS(COALESCE(total_price,0))+ABS(COALESCE(paid_amount,0))+ABS(COALESCE(remaining_amount,0))+ABS(COALESCE(shipping_cost,0)))>0.01 LIMIT 200`), []);
    if (childFinance.length) push('error', 'قيم مالية موجودة على صنف فرعي في مجموعة', `${childFinance.length} صنف فرعي يحمل بيعًا/دفعًا/شحنًا بدل قائد المجموعة.`,
      { repair_mode:'manual', action_hint:'راجع إجمالي المجموعة ثم شغّل توحيد مالية المجموعات.', affected_count:childFinance.length });

    /* --- Snapshot التكلفة الحالية --- */
    const costFields = ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix','cost_forme','cost_lamination','cost_breaking','cost_stretch'];
    const currentCosts = await safe('التكاليف الحالية', allAsync(`SELECT id,${costFields.join(',')} FROM orders ORDER BY id ASC`), []);
    const snapshots = await safe('لقطات التكاليف', allAsync(`SELECT order_id,cost_field,amount FROM cost_history WHERE source='snapshot' AND source_ref='current'`), []);
    const snapshotMap = new Map(snapshots.map(row => [`${num(row.order_id)}:${clean(row.cost_field)}`, num(row.amount)]));
    let costSnapshotDrift = 0;
    for (const order of currentCosts) for (const field of costFields) if (!snapshotMap.has(`${num(order.id)}:${field}`) || Math.abs(num(order[field])-num(snapshotMap.get(`${num(order.id)}:${field}`)))>0.01) costSnapshotDrift++;
    if (costSnapshotDrift) push('warn', 'آخر Cost Snapshot لا يطابق تكلفة الأوردر', `${costSnapshotDrift} قيمة تكلفة تحتاج مزامنة Snapshot (بدون أي خصم خزنة).`,
      { repair_mode:'auto', repair_key:'core_sync_cost_snapshots', affected_count:costSnapshotDrift });

    /* --- توافق Sales History مع المعادلات --- */
    const saleRows = await safe('سجل المبيعات', allAsync(`SELECT o.*,s.id sale_id,s.gross_sale s_gross,s.shipping_cost s_shipping,s.insurance_fee s_insurance,s.extra_cod_fee s_cod,s.other_shipping_fee s_other,s.total_deductions s_deductions,s.total_sale s_total_sale,s.total_cost s_total_cost,s.net_profit s_profit,s.remaining_amount s_remaining,s.status s_status FROM orders o LEFT JOIN sales_history s ON s.order_id=o.id WHERE TRIM(COALESCE(o.status,'')) IN ('تم التسليم','مرتجع') LIMIT 2000`), []);
    let salesDrift = 0;
    for (const row of saleRows) {
      if (!row.sale_id) { salesDrift++; continue; }
      const expectedGross = clean(row.status)==='تم التسليم' ? Math.max(0,num(row.total_price)) : 0;
      const expectedDeductions = roundMoney(num(row.s_shipping)+num(row.s_insurance)+num(row.s_cod)+num(row.s_other));
      const expectedSale = roundMoney(expectedGross-expectedDeductions);
      const rawCost = costFields.reduce((sum,field)=>sum+num(row[field]),0);
      const recovered = clean(row.status)==='مرتجع' && num(row.useReadyStock)===1 && clean(row.printType||'سادة')==='سادة' && num(row.bag_returned_to_stock)===1 ? num(row.cost_make) : 0;
      const expectedCost = roundMoney(Math.max(0,rawCost-recovered));
      if (Math.abs(num(row.s_gross)-expectedGross)>0.01 || Math.abs(num(row.s_deductions)-expectedDeductions)>0.01 || Math.abs(num(row.s_total_sale)-expectedSale)>0.01 || Math.abs(num(row.s_total_cost)-expectedCost)>0.01 || Math.abs(num(row.s_profit)-roundMoney(expectedSale-expectedCost))>0.01 || clean(row.s_status)!==clean(row.status) || (clean(row.status)==='مرتجع' && Math.abs(num(row.s_remaining))>0.01)) salesDrift++;
    }
    if (salesDrift) push('error', 'Sales History غير متوافق مع الأوردر', `${salesDrift} سجل بيع قديم أو معادلته غير صحيحة.`,
      { repair_mode:'auto', repair_key:'core_sync_sales_history', affected_count:salesDrift });

    /* --- اكتمال وربط حركات الخزنة --- */
    const missingOrderCash = await safe('دفعات أوردر بدون خزنة', allAsync(`SELECT p.id,p.order_id FROM order_payments p LEFT JOIN admin_cash_ledger l ON l.source_type='order_payment' AND l.source_ref=CAST(p.id AS TEXT) WHERE l.id IS NULL LIMIT 200`), []);
    const missingPurchaseCash = await safe('دفعات مشتريات بدون خزنة', allAsync(`SELECT p.id,p.purchase_id FROM purchase_payments p LEFT JOIN admin_cash_ledger l ON l.source_type='purchase_payment' AND l.source_ref=CAST(p.id AS TEXT) WHERE l.id IS NULL LIMIT 200`), []);
    const missingExpenseCash = await safe('مصاريف بدون خزنة', allAsync(`SELECT e.id,e.order_id FROM expenses e LEFT JOIN admin_cash_ledger l ON l.source_type='expense' AND l.source_ref=CAST(e.id AS TEXT) WHERE TRIM(COALESCE(e.actor_username,''))<>'' AND l.id IS NULL LIMIT 200`), []);
    if (missingOrderCash.length || missingPurchaseCash.length || missingExpenseCash.length) push('error', 'حركة مالية بدون Cash Ledger مقابل', `دفعات أوردر: ${missingOrderCash.length}، مشتريات: ${missingPurchaseCash.length}، مصروفات: ${missingExpenseCash.length}.`,
      { repair_mode:'auto', repair_key:'core_sync_financial_cash', affected_count:missingOrderCash.length+missingPurchaseCash.length+missingExpenseCash.length });
    const duplicateCashSources = await safe('حركات خزنة مكررة', allAsync(`SELECT source_type,source_ref,COUNT(*) c FROM admin_cash_ledger WHERE source_type IN ('order_payment','purchase_payment','expense','fixed_asset','order_cost','refund') AND TRIM(COALESCE(source_ref,''))<>'' GROUP BY source_type,source_ref HAVING COUNT(*)>1 LIMIT 200`), []);
    if (duplicateCashSources.length) push('error', 'Cash Ledger مكرر لنفس المصدر', `${duplicateCashSources.length} مفتاح مصدر مكرر.`,
      { repair_mode:'manual', action_hint:'لا تحذف يدويًا؛ شغّل إعادة التوفيق المالية التي تحتفظ بحركة واحدة صحيحة.', affected_count:duplicateCashSources.length });

    /* --- سجل مبيعات لأوردر محذوف --- */
    const orphanSales = await allAsync(
      `SELECT s.id, s.order_id FROM sales_history s LEFT JOIN orders o ON o.id=s.order_id WHERE o.id IS NULL LIMIT 200`);
    if (orphanSales.length) push('error', 'سجل مبيعات لأوردرات محذوفة',
      `${orphanSales.length} سطر في سجل المبيعات بدون أوردر.`,
      { repair_mode: 'auto', repair_key: 'core_orphan_sales', affected_count: orphanSales.length });

    /* --- دفعات أوردر لأوردر محذوف --- */
    const orphanPayments = await safe('دفعات بدون أوردر', allAsync(
      `SELECT p.id FROM order_payments p LEFT JOIN orders o ON o.id=p.order_id WHERE o.id IS NULL LIMIT 200`), []);
    if (orphanPayments.length) push('error', 'دفعات مرتبطة بأوردرات محذوفة',
      `${orphanPayments.length} دفعة بدون أوردر.`,
      { repair_mode: 'auto', repair_key: 'core_orphan_order_payments', affected_count: orphanPayments.length });

    /* --- مصاريف بمبلغ صفر أو سالب --- */
    const badExpenses = await allAsync(`SELECT id, amount FROM expenses WHERE COALESCE(amount,0) <= 0 LIMIT 100`);
    if (badExpenses.length) push('warn', 'مصاريف بمبلغ صفر أو بالسالب',
      `${badExpenses.length} مصروف قيمته مش موجبة.`,
      { repair_mode: 'auto', repair_key: 'core_zero_expenses', affected_count: badExpenses.length });

    /* --- عميل مكرر بنفس التليفون --- */
    const dupCustomers = await safe('العملاء المكرّرين', allAsync(
      `SELECT phone, COUNT(*) c FROM customers WHERE COALESCE(phone,'') <> '' GROUP BY phone HAVING c > 1 LIMIT 50`), []);
    if (dupCustomers.length) push('warn', 'عملاء مكرّرين بنفس رقم التليفون',
      `${dupCustomers.length} رقم متكرر في قائمة العملاء.`,
      { repair_mode: 'manual', action_hint: 'ادمج العملاء المكرّرين من صفحة العملاء.' });

    /* --- مستخدم نشط بدون أي صلاحية عرض --- */
    const blindUsers = await safe('صلاحيات المستخدمين', allAsync(
      `SELECT username, full_name FROM users WHERE COALESCE(is_active,1)=1 AND username <> 'admin'
         AND COALESCE(perm_view_orders,0)=0 AND COALESCE(perm_view_accounts,0)=0 AND COALESCE(perm_view_inventory,0)=0
         AND COALESCE(perm_orders,0)=0 AND COALESCE(perm_accounts,0)=0 AND COALESCE(perm_inventory,0)=0 LIMIT 50`), []);
    if (blindUsers.length) push('warn', 'مستخدم نشط من غير أي صلاحية عرض',
      `${blindUsers.length} مستخدم مش هيشوف أي صفحة: ${blindUsers.slice(0, 5).map(u => u.full_name || u.username).join('، ')}`,
      { repair_mode: 'manual', action_hint: 'افتح صفحة المستخدمين وادّيه صلاحياته أو وقّفه.' });

    if (failures.length) issues.push({ severity: 'warn', title: 'فحوصات ما قدرتش تشتغل', details: failures.join(' | '), data: { module: 'core', repair_mode: 'manual', action_hint: 'ابعت الرسالة دي علشان تتظبط — غالبًا عمود ناقص في قاعدة بيانات قديمة.' } });
    return issues;
  }

  /* ===================== 2) جدول الإصلاحات ===================== */
  const CORE_REPAIRS = {
    core_sync_inventory_flags: {
      label: 'مزامنة أعلام خصم المخزون مع السجلات',
      run: async () => { const r = await legacySyncInventoryFlags?.('system'); return [`مزامنة أعلام المخزون${r?.fixed ? ` (${r.fixed} أوردر)` : ''}`]; },
    },
    core_rebuild_admin_cash: {
      label: 'إعادة بناء أرصدة العهدة',
      run: async () => { await legacyRebuildAdminCash?.(); return ['إعادة بناء أرصدة العهدة الجارية']; },
    },
    core_fix_remaining: {
      label: 'تصحيح المتبقي على الأوردرات',
      run: async () => {
        const r = await runAsync(
          `UPDATE orders SET remaining_amount = ROUND(COALESCE(total_price,0) - COALESCE(paid_amount,0), 2)
           WHERE ABS(COALESCE(remaining_amount,0) - (COALESCE(total_price,0) - COALESCE(paid_amount,0))) > 0.01
             AND TRIM(COALESCE(status,''))<>'مرتجع'
             AND (TRIM(COALESCE(group_code,''))='' OR COALESCE(item_no,1)=1)`);
        return r.changes ? [`تصحيح المتبقي على ${r.changes} أوردر`] : [];
      },
    },
    core_sync_fixed_assets: {
      label: 'مزامنة الأصول الثابتة مع الخزنة',
      run: async (actor) => { const r = await syncFixedAssetCash?.(actor || 'system'); return [`مزامنة ${num(r?.synced)} أصل ثابت مع حركة خزنة واحدة لكل أصل`]; },
    },
    core_fix_returned_remaining: {
      label: 'تصفير مديونية المرتجعات',
      run: async (actor) => { const r = await normalizeReturnedBalances?.(actor || 'system'); return [`تصفير المتبقي في ${num(r?.fixed)} أوردر مرتجع دون مسح السعر أو الدفعات`]; },
    },
    core_sync_cost_snapshots: {
      label: 'مزامنة لقطات سجل التكلفة',
      run: async (actor) => { const r = await syncCostSnapshots?.(actor || 'system'); return [`مزامنة Cost History كـ Snapshot فقط (${num(r?.synced)} أوردر)`]; },
    },
    core_sync_sales_history: {
      label: 'إعادة احتساب سجل المبيعات',
      run: async () => { await syncSalesHistory?.(); return ['إعادة احتساب Sales History من قيم الأوردر الحالية']; },
    },
    core_sync_financial_cash: {
      label: 'مزامنة الحركات المالية مع الخزنة',
      run: async (actor) => {
        await syncOrderPaymentCash?.(actor || 'system');
        await syncPurchasePaymentCash?.(actor || 'system');
        await syncExpenseCash?.(actor || 'system');
        return ['ربط الدفعات والمشتريات والمصروفات بحركات خزنة Idempotent'];
      },
    },
    core_orphan_sales: {
      label: 'مسح سجل مبيعات بدون أوردر',
      run: async () => { const r = await runAsync(`DELETE FROM sales_history WHERE order_id NOT IN (SELECT id FROM orders)`); return r.changes ? [`مسح ${r.changes} سطر مبيعات بدون أوردر`] : []; },
    },
    core_orphan_order_payments: {
      label: 'مسح دفعات بدون أوردر',
      run: async () => { const r = await runAsync(`DELETE FROM order_payments WHERE order_id NOT IN (SELECT id FROM orders)`); return r.changes ? [`مسح ${r.changes} دفعة بدون أوردر`] : []; },
    },
    core_zero_expenses: {
      label: 'مسح مصاريف بصفر',
      run: async () => { const r = await runAsync(`DELETE FROM expenses WHERE COALESCE(amount,0) <= 0`); return r.changes ? [`مسح ${r.changes} مصروف بصفر`] : []; },
    },
    /* الإصلاحات القديمة، بس دلوقتي متندهة بمفتاح واضح */
    legacy_all: {
      label: 'الإصلاح العام للأوردرات والحسابات',
      run: async (actor) => { const r = await legacyRepairAll?.({ actor, maxPasses: 3 }); return r?.fixed ? [`إصلاح ${r.fixed} مشكلة في الأوردرات والحسابات`] : ['تشغيل الإصلاح العام']; },
    },
  };

  function moduleRepairs() {
    const map = {};
    for (const [name, mod] of Object.entries(modules || {})) {
      if (!mod?.repair) continue;
      map[name] = mod.repair;
    }
    return map;
  }

  /* ===================== 3) الفحص الشامل ===================== */
  let lastRunAt = null;

  async function runFullHealthCheck() {
    const buckets = [];
    const failed = [];
    const collect = async (label, fn) => {
      if (typeof fn !== 'function') return;
      try { const r = await fn(); if (Array.isArray(r)) buckets.push(...r); else if (Array.isArray(r?.issues)) buckets.push(...r.issues); }
      catch (e) { failed.push(`${label}: ${e.message}`); }
    };

    await collect('الأوردرات والحسابات', legacyHealthCheck);
    await collect('فحوصات عامة', coreExtraChecks);
    for (const [name, mod] of Object.entries(modules || {})) await collect(name, mod?.check);

    // مفتاح ثابت لكل مشكلة علشان نقدر نصلحها لوحدها ونتأكد إنها اتصلحت
    const issues = buckets.map((issue, index) => {
      const data = issue?.data || {};
      const orderId = num(data.order_id);
      const key = [
        clean(data.repair_key) || 'legacy',
        clean(data.module) || 'core',
        orderId || '',
        clean(data.field) || clean(data.expense_id) || clean(data.worker_id) || clean(data.batch_id) || '',
        clean(issue?.title).replace(/\s+/g, '_').slice(0, 40),
      ].filter(Boolean).join(':');
      return {
        ...issue,
        issue_key: key,
        index,
        data: {
          ...data,
          module: clean(data.module) || 'core',
          repair_mode: clean(data.repair_mode) || (orderId ? 'auto' : (clean(data.repair_key) ? 'auto' : 'auto')),
          repair_key: clean(data.repair_key) || (orderId ? 'legacy_order' : 'legacy_all'),
        },
      };
    });

    const totals = { error: 0, warn: 0, info: 0, total: issues.length };
    for (const i of issues) totals[i.severity] = num(totals[i.severity]) + 1;

    // تجميع حسب المجال علشان الشاشة تبقى مفهومة
    const byModule = {};
    for (const i of issues) {
      const m = i.data.module || 'core';
      byModule[m] = byModule[m] || { module: m, error: 0, warn: 0, total: 0 };
      byModule[m][i.severity] = num(byModule[m][i.severity]) + 1;
      byModule[m].total++;
    }

    lastRunAt = new Date().toISOString();
    return {
      ok: totals.error === 0 && totals.warn === 0,
      totals, issues,
      by_module: Object.values(byModule),
      scan_failed: failed,
      checked_at: lastRunAt,
      auto_fixable: issues.filter(i => i.data.repair_mode !== 'manual').length,
      manual_only: issues.filter(i => i.data.repair_mode === 'manual').length,
    };
  }

  /* ===================== 4) الإصلاح ===================== */
  /* بيصلّح مشكلة واحدة بالمفتاح بتاعها، أو كل المشاكل القابلة للإصلاح. */
  async function runRepair({ actor = 'system', issueKey = '', repairKey = '', orderId = 0, maxPasses = 3 } = {}) {
    const done = [];
    const skipped = [];
    const modRepairs = moduleRepairs();

    const runOne = async (key, oid) => {
      try {
        if (key === 'legacy_order' || (oid && !key)) {
          const r = await legacyRepairOrder?.({ orderId: num(oid), actor, maxPasses: 4 });
          done.push(`إصلاح شامل للأوردر #${oid}${r?.fixed ? ` (${r.fixed})` : ''}`);
          return true;
        }
        if (CORE_REPAIRS[key]) { done.push(...(await CORE_REPAIRS[key].run(actor) || [])); return true; }
        for (const [name, fn] of Object.entries(modRepairs)) {
          if (String(key).startsWith(name.slice(0, 6)) || String(key).startsWith(name)) {
            done.push(...(await fn(key) || []));
            return true;
          }
        }
        // مفاتيح الوحدات بالبادئة الصريحة
        if (key.startsWith('workshop_') && modRepairs.workshop) { done.push(...(await modRepairs.workshop(key) || [])); return true; }
        if (key.startsWith('trader_') && modRepairs.traders) { done.push(...(await modRepairs.traders(key) || [])); return true; }
        if (key.startsWith('partner_') && modRepairs.partners) { done.push(...(await modRepairs.partners(key) || [])); return true; }
        if (key.startsWith('core_') && CORE_REPAIRS[key]) { done.push(...(await CORE_REPAIRS[key].run(actor) || [])); return true; }
        skipped.push({ key, reason: 'المشكلة دي محتاجة قرار منك — مفيش إصلاح تلقائي ليها.' });
        return false;
      } catch (e) {
        skipped.push({ key, reason: `فشل الإصلاح: ${e.message}` });
        return false;
      }
    };

    /* ---- إصلاح مشكلة واحدة ---- */
    if (issueKey || repairKey || orderId) {
      const before = await runFullHealthCheck();
      const target = issueKey ? before.issues.find(i => i.issue_key === issueKey) : null;
      const key = repairKey || target?.data?.repair_key || (orderId ? 'legacy_order' : '');
      const oid = num(orderId) || num(target?.data?.order_id);

      if (target && target.data.repair_mode === 'manual') {
        return {
          ok: false, manual: true,
          message: target.data.action_hint || 'المشكلة دي محتاجة قرار منك، مش إصلاح تلقائي.',
          before: before.totals, after: before.totals, done: [], skipped: [{ key, reason: target.data.action_hint || 'تحتاج تدخل يدوي' }],
          health: before,
        };
      }

      await runOne(key, oid);
      const after = await runFullHealthCheck();
      const stillThere = issueKey ? after.issues.some(i => i.issue_key === issueKey) : false;
      return {
        ok: !stillThere,
        fixed_this: !stillThere,
        message: stillThere
          ? 'الإصلاح اتنفّذ بس المشكلة لسه ظاهرة — اضغط «إصلاح شامل» أو راجع التفاصيل.'
          : 'تمام، المشكلة دي اتصلحت.',
        before: before.totals, after: after.totals, done, skipped, health: after,
      };
    }

    /* ---- إصلاح شامل: لوب لحد ما الأرقام تستقر ---- */
    const first = await runFullHealthCheck();
    let health = first;
    let pass = 0;
    let previousSignature = '';
    while (pass < Math.max(1, num(maxPasses, 3))) {
      pass++;
      const fixable = health.issues.filter(i => i.data.repair_mode !== 'manual');
      if (!fixable.length) break;

      // كل مفتاح مرة واحدة في الباسة
      const keys = new Set();
      const orderIds = new Set();
      for (const i of fixable) {
        const k = clean(i.data.repair_key);
        const oid = num(i.data.order_id);
        if (k === 'legacy_order' || (!k && oid)) { if (oid) orderIds.add(oid); }
        else if (k) keys.add(k);
      }
      for (const k of keys) await runOne(k, 0);
      for (const oid of orderIds) await runOne('legacy_order', oid);

      health = await runFullHealthCheck();
      const signature = health.issues.map(i => i.issue_key).sort().join('|');
      // لو نفس المشاكل بالظبط رجعت تاني يبقى مفيش فايدة من باسة كمان
      if (signature === previousSignature) break;
      previousSignature = signature;
    }

    return {
      ok: health.totals.error === 0,
      passes: pass,
      before: first.totals, after: health.totals,
      done, skipped, health,
      message: health.totals.total === 0
        ? 'تمام — مفيش أي مشاكل في السيستم دلوقتي.'
        : (health.manual_only === health.totals.total
          ? 'كل اللي اتصلح تلقائيًا اتصلح. الباقي محتاج قرار منك.'
          : 'اتصلح اللي ينفع يتصلح تلقائيًا. راجع الباقي في القائمة.'),
    };
  }

  /* ===================== 5) المسارات ===================== */
  const canConfig = requirePerm('perm_system_config');

  app.get('/system-health-full', authRequired, canConfig, async (req, res) => {
    try { res.json(await runFullHealthCheck()); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/system-health-fix-all', authRequired, canConfig, async (req, res) => {
    try {
      const actor = String(req.user?.full_name || req.user?.username || 'system');
      const result = await runRepair({ actor, maxPasses: num(req.body?.max_passes, 3) });
      await recordAudit({ req, action: 'system-health-fix-all', entity_type: 'system', entity_id: 0, details: `إصلاح شامل: ${result.before.total} → ${result.after.total}` });
      res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/system-health-fix-one', authRequired, canConfig, async (req, res) => {
    try {
      const actor = String(req.user?.full_name || req.user?.username || 'system');
      const result = await runRepair({
        actor,
        issueKey: clean(req.body?.issue_key),
        repairKey: clean(req.body?.repair_key),
        orderId: num(req.body?.order_id),
      });
      await recordAudit({ req, action: 'system-health-fix-one', entity_type: 'system', entity_id: num(req.body?.order_id), details: `إصلاح مشكلة: ${clean(req.body?.issue_key) || clean(req.body?.repair_key)}` });
      res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  return { runFullHealthCheck, runRepair, coreExtraChecks };
}

module.exports = { register };
