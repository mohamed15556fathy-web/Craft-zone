'use strict';
/* ============================================================================
   Craft Zone — حسابات التجار المعزولة (v9.9)
   ----------------------------------------------------------------------------
   قرار: أوردرات التجار ليها حساباتها لوحدها جوّه صفحة التجار، ومالهاش أي دعوة
   بصفحة الحسابات العامة. عشان كده:
     • كل أوردر عليه order_scope='trader' بيتشال من كل تجميعات المصنع
       (المبيعات، التكاليف، الأرباح، الخزنة، التقارير، أرباح الشركاء).
     • الفلوس اللي بتيجي من التاجر بتتسجل في trader_payments (تحصيل/مرتجع)
       ومش بتلمس الخزنة العامة.
     • تكاليف الطلبية = تكاليف الأوردرات المربوطة + تكاليف مباشرة على الطلبية
       (نقل، عمولة، تغليف، عينات...) في trader_batch_costs.

   صافي ربح الطلبية = قيمة البيع − (تكاليف الأوردرات + التكاليف المباشرة)
   رصيد التاجر      = رصيد افتتاحي + قيمة الطلبيات − التحصيل + المرتجعات
   ========================================================================== */

function register(ctx) {
  const {
    app, runAsync, getAsync, allAsync, addColumnIfMissing,
    authRequired, requireAnyPerm,
    num, roundMoney, recordAudit, hasPerm,
  } = ctx;

  const clean = (v) => String(v ?? '').trim();
  const today = () => new Date().toISOString().slice(0, 10);
  const actorName = (req) => String(req?.user?.full_name || req?.user?.username || 'system');

  const canView = requireAnyPerm('perm_view_traders', 'perm_customers', 'perm_view_orders');
  const canManage = requireAnyPerm('perm_manage_traders', 'perm_customers');
  const canPay = requireAnyPerm('perm_manage_trader_payments', 'perm_manage_debts', 'perm_manage_expenses');

  /* ===================== 1) الجداول ===================== */
  async function ensureTraderFinanceSchema() {
    for (const [n, d] of [
      ['opening_balance', 'REAL DEFAULT 0'],
      ['credit_limit', 'REAL DEFAULT 0'],
      ['payment_terms_days', 'INTEGER DEFAULT 0'],
      ['default_discount_percent', 'REAL DEFAULT 0'],
    ]) await addColumnIfMissing('traders', n, d);

    for (const [n, d] of [
      ['discount_amount', 'REAL DEFAULT 0'],
      ['extra_charge_amount', 'REAL DEFAULT 0'],
      ['settled_at', "TEXT DEFAULT ''"],
    ]) await addColumnIfMissing('trader_batches', n, d);

    // اتجاه الدفعة: in = تحصيل من التاجر، out = مرتجع/خصم مدفوع له
    for (const [n, d] of [
      ['direction', "TEXT DEFAULT 'in'"],
      ['signed_amount', 'REAL DEFAULT 0'],
    ]) await addColumnIfMissing('trader_payments', n, d);
    await runAsync(`UPDATE trader_payments SET direction='in' WHERE COALESCE(direction,'')=''`);
    await runAsync(`UPDATE trader_payments SET signed_amount = CASE WHEN direction='out' THEN -ABS(amount) ELSE ABS(amount) END`);

    await runAsync(`CREATE TABLE IF NOT EXISTS trader_batch_costs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id INTEGER DEFAULT 0,
      trader_id INTEGER DEFAULT 0,
      cost_type TEXT DEFAULT 'أخرى',
      amount REAL DEFAULT 0,
      cost_date TEXT DEFAULT CURRENT_DATE,
      note TEXT DEFAULT '',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['trader_id', 'INTEGER DEFAULT 0'], ['cost_type', "TEXT DEFAULT 'أخرى'"], ['amount', 'REAL DEFAULT 0'],
      ['cost_date', 'TEXT DEFAULT CURRENT_DATE'], ['note', "TEXT DEFAULT ''"], ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'],
      ['created_by', "TEXT DEFAULT ''"]]) await addColumnIfMissing('trader_batch_costs', n, d);
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_trader_batch_costs_batch ON trader_batch_costs(batch_id)`);
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_trader_payments_trader ON trader_payments(trader_id, batch_id)`);

    // مرآة order_scope على sales_history علشان نقدر نفلتر الأرباح بسرعة
    await addColumnIfMissing('sales_history', 'order_scope', "TEXT DEFAULT 'customer'");
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_sales_history_scope ON sales_history(order_scope)`);
    await backfillSalesHistoryScope();
  }

  /* مزامنة مرآة order_scope — بتتنادى في الفحص وبعد أي ربط طلبية. */
  async function backfillSalesHistoryScope() {
    await runAsync(
      `UPDATE sales_history SET order_scope = COALESCE((SELECT COALESCE(o.order_scope,'customer') FROM orders o WHERE o.id = sales_history.order_id), 'customer')
       WHERE COALESCE(order_scope,'') = ''
          OR order_scope <> COALESCE((SELECT COALESCE(o.order_scope,'customer') FROM orders o WHERE o.id = sales_history.order_id), 'customer')`);
  }

  /* ===================== 2) الحسابات ===================== */
  async function batchDirectCosts(batchId) {
    const row = await getAsync(`SELECT COALESCE(SUM(amount),0) v FROM trader_batch_costs WHERE batch_id=?`, [batchId]);
    return roundMoney(num(row?.v));
  }

  async function batchOrderTotals(batchId) {
    const rows = await allAsync(
      `SELECT o.id, o.total_price, o.shipping_cost, o.status,
              COALESCE(s.total_sale, 0) hist_sale, COALESCE(s.total_cost, 0) hist_cost,
              (s.id IS NOT NULL) has_hist
       FROM trader_batch_orders tbo
       JOIN orders o ON o.id = tbo.order_id
       LEFT JOIN sales_history s ON s.order_id = o.id
       WHERE tbo.batch_id = ?`, [batchId]);
    let sale = 0, cost = 0, delivered = 0;
    for (const r of rows) {
      if (num(r.has_hist)) { sale += num(r.hist_sale); cost += num(r.hist_cost); delivered++; }
      else { sale += num(r.total_price); }   // لسه متسلمش: بنحسب البيع المتوقع
    }
    return { orders_count: rows.length, delivered_count: delivered, sale: roundMoney(sale), order_cost: roundMoney(cost), order_ids: rows.map(r => r.id) };
  }

  async function batchFinance(batch) {
    const t = await batchOrderTotals(batch.id);
    const direct = await batchDirectCosts(batch.id);
    let sale = t.sale;
    let orderCost = t.order_cost;
    if (!sale && num(batch.sale_total_manual) > 0) sale = roundMoney(num(batch.sale_total_manual));
    if (!orderCost && num(batch.cost_total_manual) > 0) orderCost = roundMoney(num(batch.cost_total_manual));
    const discount = roundMoney(num(batch.discount_amount));
    const extra = roundMoney(num(batch.extra_charge_amount));
    const netSale = roundMoney(sale - discount + extra);
    const totalCost = roundMoney(orderCost + direct);
    // مش بنعتمد على signed_amount لوحده — لو صف قديم لسه مش متحدّث بنحسبه من الاتجاه
    const payRow = await getAsync(`SELECT COALESCE(SUM(CASE WHEN COALESCE(signed_amount,0) <> 0 THEN signed_amount WHEN direction='out' THEN -ABS(amount) ELSE ABS(amount) END),0) v FROM trader_payments WHERE batch_id=?`, [batch.id]);
    const collected = roundMoney(num(payRow?.v));
    return {
      orders_count: t.orders_count, delivered_count: t.delivered_count, order_ids: t.order_ids,
      sale, discount, extra, net_sale: netSale,
      order_cost: orderCost, direct_cost: direct, total_cost: totalCost,
      net_profit: roundMoney(netSale - totalCost),
      collected, remaining: roundMoney(netSale - collected),
      margin_percent: netSale > 0 ? roundMoney((netSale - totalCost) / netSale * 100) : 0,
    };
  }

  async function traderFinance(traderId, { from = '', to = '' } = {}) {
    const trader = await getAsync(`SELECT * FROM traders WHERE id=?`, [traderId]);
    if (!trader) return null;
    const params = [traderId];
    let where = '';
    if (from) { where += ` AND date(order_date) >= date(?)`; params.push(from); }
    if (to) { where += ` AND date(order_date) <= date(?)`; params.push(to); }
    const batches = await allAsync(`SELECT * FROM trader_batches WHERE trader_id=?${where} ORDER BY date(order_date) DESC, id DESC`, params);
    let netSale = 0, totalCost = 0, collected = 0, openCount = 0;
    const batchRows = [];
    for (const b of batches) {
      const f = await batchFinance(b);
      netSale += f.net_sale; totalCost += f.total_cost; collected += f.collected;
      if (!['مغلقة', 'تمت التسوية', 'تم التحصيل'].includes(clean(b.status))) openCount++;
      batchRows.push({ ...b, finance: f });
    }
    const opening = roundMoney(num(trader.opening_balance));
    return {
      trader,
      batches: batchRows,
      totals: {
        batches_count: batches.length, open_batches: openCount,
        opening_balance: opening,
        net_sale: roundMoney(netSale), total_cost: roundMoney(totalCost),
        net_profit: roundMoney(netSale - totalCost),
        collected: roundMoney(collected),
        remaining: roundMoney(opening + netSale - collected),
        margin_percent: netSale > 0 ? roundMoney((netSale - totalCost) / netSale * 100) : 0,
      },
    };
  }

  /* ===================== 3) المسارات ===================== */

  /* ملخص مالي معزول لكل التجار — ده اللي بيغذي كروت صفحة التجار. */
  app.get('/traders-finance-summary', authRequired, canView, async (req, res) => {
    try {
      const from = clean(req.query.from), to = clean(req.query.to);
      const showTotals = hasPerm(req.user, 'perm_view_financial_totals') || hasPerm(req.user, 'perm_manage_traders') || hasPerm(req.user, 'perm_view_traders');
      const traders = await allAsync(`SELECT * FROM traders ORDER BY is_active DESC, name ASC`);
      const rows = [];
      const totals = { traders: 0, batches: 0, open_batches: 0, net_sale: 0, total_cost: 0, net_profit: 0, collected: 0, remaining: 0, direct_costs: 0 };
      for (const t of traders) {
        const f = await traderFinance(t.id, { from, to });
        if (!f) continue;
        rows.push({
          id: t.id, name: t.name, phone: t.phone, is_active: num(t.is_active, 1),
          opening_balance: roundMoney(num(t.opening_balance)),
          credit_limit: roundMoney(num(t.credit_limit)),
          ...f.totals,
        });
        totals.batches += f.totals.batches_count;
        totals.open_batches += f.totals.open_batches;
        totals.net_sale = roundMoney(totals.net_sale + f.totals.net_sale);
        totals.total_cost = roundMoney(totals.total_cost + f.totals.total_cost);
        totals.net_profit = roundMoney(totals.net_profit + f.totals.net_profit);
        totals.collected = roundMoney(totals.collected + f.totals.collected);
        totals.remaining = roundMoney(totals.remaining + f.totals.remaining);
      }
      totals.traders = traders.filter(t => num(t.is_active, 1) === 1).length;
      const dc = await getAsync(`SELECT COALESCE(SUM(amount),0) v FROM trader_batch_costs`);
      totals.direct_costs = roundMoney(num(dc?.v));
      if (!showTotals) for (const k of Object.keys(totals)) if (!['traders', 'batches', 'open_batches'].includes(k)) totals[k] = 0;
      res.json({ traders: rows, totals, can: { manage: hasPerm(req.user, 'perm_manage_traders') || hasPerm(req.user, 'perm_customers'), pay: hasPerm(req.user, 'perm_manage_trader_payments') || hasPerm(req.user, 'perm_manage_debts') || hasPerm(req.user, 'perm_manage_expenses') } });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* كشف حساب التاجر برصيد جاري. */
  app.get('/trader-statement/:id', authRequired, canView, async (req, res) => {
    try {
      const id = num(req.params.id);
      const trader = await getAsync(`SELECT * FROM traders WHERE id=?`, [id]);
      if (!trader) return res.status(404).json({ error: 'التاجر مش موجود' });
      const from = clean(req.query.from), to = clean(req.query.to);

      const opening = roundMoney(num(trader.opening_balance));
      let openingRunning = opening;
      if (from) {
        const prevBatches = await allAsync(`SELECT * FROM trader_batches WHERE trader_id=? AND date(order_date) < date(?)`, [id, from]);
        for (const b of prevBatches) openingRunning = roundMoney(openingRunning + (await batchFinance(b)).net_sale);
        const prevPay = await getAsync(`SELECT COALESCE(SUM(CASE WHEN COALESCE(signed_amount,0) <> 0 THEN signed_amount WHEN direction='out' THEN -ABS(amount) ELSE ABS(amount) END),0) v FROM trader_payments WHERE trader_id=? AND date(payment_date) < date(?)`, [id, from]);
        openingRunning = roundMoney(openingRunning - num(prevPay?.v));
      }

      const bp = [id]; let bw = '';
      if (from) { bw += ` AND date(order_date) >= date(?)`; bp.push(from); }
      if (to) { bw += ` AND date(order_date) <= date(?)`; bp.push(to); }
      const batches = await allAsync(`SELECT * FROM trader_batches WHERE trader_id=?${bw}`, bp);

      const pp = [id]; let pw = '';
      if (from) { pw += ` AND date(payment_date) >= date(?)`; pp.push(from); }
      if (to) { pw += ` AND date(payment_date) <= date(?)`; pp.push(to); }
      const payments = await allAsync(`SELECT * FROM trader_payments WHERE trader_id=?${pw}`, pp);

      const rows = [];
      for (const b of batches) {
        const f = await batchFinance(b);
        rows.push({ date: b.order_date, kind: 'طلبية', ref: b.batch_code || `#${b.id}`, detail: b.batch_name || '', debit: f.net_sale, credit: 0, id: b.id, sort: 1 });
      }
      for (const p of payments) {
        const signed = num(p.signed_amount) || (clean(p.direction) === 'out' ? -Math.abs(num(p.amount)) : Math.abs(num(p.amount)));
        rows.push({
          date: p.payment_date, kind: signed >= 0 ? 'تحصيل' : 'مرتجع/خصم', ref: p.reference_no || `#${p.id}`,
          detail: `${clean(p.payment_method) || 'نقدي'}${p.note ? ' — ' + p.note : ''}`,
          debit: signed >= 0 ? 0 : Math.abs(signed), credit: signed >= 0 ? signed : 0, id: p.id, sort: 2,
        });
      }
      rows.sort((a, b) => String(a.date).localeCompare(String(b.date)) || a.sort - b.sort || num(a.id) - num(b.id));
      let running = openingRunning;
      for (const r of rows) { running = roundMoney(running + num(r.debit) - num(r.credit)); r.balance = running; }

      res.json({
        trader: { id: trader.id, name: trader.name, phone: trader.phone, opening_balance: opening, credit_limit: roundMoney(num(trader.credit_limit)) },
        period_from: from, period_to: to,
        opening_balance: roundMoney(openingRunning), rows, closing_balance: roundMoney(running),
        totals: {
          sales: roundMoney(rows.reduce((a, r) => a + num(r.debit), 0)),
          collected: roundMoney(rows.reduce((a, r) => a + num(r.credit), 0)),
        },
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* تفاصيل طلبية واحدة: أوردراتها + تكاليفها المباشرة + ربحها. */
  app.get('/trader-batch-detail/:id', authRequired, canView, async (req, res) => {
    try {
      const id = num(req.params.id);
      const batch = await getAsync(`SELECT * FROM trader_batches WHERE id=?`, [id]);
      if (!batch) return res.status(404).json({ error: 'الطلبية مش موجودة' });
      const trader = await getAsync(`SELECT * FROM traders WHERE id=?`, [batch.trader_id]);
      const finance = await batchFinance(batch);
      const orders = await allAsync(
        `SELECT o.id, o.custName, o.orderDate, o.status, o.qty, o.total_price, o.paid_amount, o.remaining_amount,
                COALESCE(s.total_sale,0) hist_sale, COALESCE(s.total_cost,0) hist_cost, COALESCE(s.net_profit,0) hist_profit
         FROM trader_batch_orders tbo JOIN orders o ON o.id=tbo.order_id
         LEFT JOIN sales_history s ON s.order_id=o.id
         WHERE tbo.batch_id=? ORDER BY o.id DESC`, [id]);
      const costs = await allAsync(`SELECT * FROM trader_batch_costs WHERE batch_id=? ORDER BY date(cost_date) DESC, id DESC`, [id]);
      const payments = await allAsync(`SELECT * FROM trader_payments WHERE batch_id=? ORDER BY date(payment_date) DESC, id DESC`, [id]);
      res.json({ batch, trader, finance, orders, costs, payments });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* أوردرات التاجر اللي لسه مش مربوطة بطلبية — عشان الربط يبقى بالاختيار مش بكتابة أرقام. */
  app.get('/trader-linkable-orders', authRequired, canView, async (req, res) => {
    try {
      const traderId = num(req.query.trader_id);
      const batchId = num(req.query.batch_id);
      const search = clean(req.query.q);
      const params = [];
      let sql = `SELECT o.id, o.custName, o.orderDate, o.status, o.qty, o.total_price, o.trader_id, o.trader_batch_id,
                        (SELECT COUNT(*) FROM trader_batch_orders x WHERE x.order_id=o.id AND x.batch_id=?) linked
                 FROM orders o WHERE 1=1`;
      params.push(batchId);
      if (traderId) { sql += ` AND (o.trader_id=? OR COALESCE(o.trader_id,0)=0)`; params.push(traderId); }
      if (search) { sql += ` AND (CAST(o.id AS TEXT) LIKE ? OR o.custName LIKE ?)`; params.push(`%${search}%`, `%${search}%`); }
      sql += ` ORDER BY o.id DESC LIMIT 400`;
      const rows = await allAsync(sql, params);
      res.json({ orders: rows });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* تكاليف مباشرة على الطلبية */
  app.post('/save-trader-batch-cost', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const batchId = num(req.body?.batch_id);
      const batch = await getAsync(`SELECT * FROM trader_batches WHERE id=?`, [batchId]);
      if (!batch) return res.status(400).json({ error: 'اختار طلبية صحيحة' });
      const amount = roundMoney(num(req.body?.amount));
      if (!(amount > 0)) return res.status(400).json({ error: 'اكتب مبلغ التكلفة' });
      const vals = [batchId, num(batch.trader_id), clean(req.body?.cost_type) || 'أخرى', amount, clean(req.body?.cost_date) || today(), clean(req.body?.note)];
      if (id) {
        await runAsync(`UPDATE trader_batch_costs SET batch_id=?,trader_id=?,cost_type=?,amount=?,cost_date=?,note=? WHERE id=?`, [...vals, id]);
        await recordAudit({ req, action: 'save-trader-batch-cost', entity_type: 'trader_batch_cost', entity_id: id, details: `تعديل تكلفة طلبية: ${amount}` });
        return res.json({ success: true, id });
      }
      const ins = await runAsync(`INSERT INTO trader_batch_costs (batch_id,trader_id,cost_type,amount,cost_date,note,created_by) VALUES (?,?,?,?,?,?,?)`, [...vals, actorName(req)]);
      await recordAudit({ req, action: 'save-trader-batch-cost', entity_type: 'trader_batch_cost', entity_id: ins.lastID, details: `تكلفة طلبية ${batch.batch_name || batchId}: ${amount}` });
      res.json({ success: true, id: ins.lastID });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-trader-batch-cost/:id', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.params.id);
      await runAsync(`DELETE FROM trader_batch_costs WHERE id=?`, [id]);
      await recordAudit({ req, action: 'delete-trader-batch-cost', entity_type: 'trader_batch_cost', entity_id: id, details: 'حذف تكلفة طلبية' });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* تعديل دفعة تاجر (كان ناقص — كان في حفظ وحذف بس) */
  app.post('/update-trader-payment/:id', authRequired, canPay, async (req, res) => {
    try {
      const id = num(req.params.id);
      const row = await getAsync(`SELECT * FROM trader_payments WHERE id=?`, [id]);
      if (!row) return res.status(404).json({ error: 'الدفعة مش موجودة' });
      const amount = roundMoney(Math.abs(num(req.body?.amount)));
      if (!(amount > 0)) return res.status(400).json({ error: 'اكتب المبلغ' });
      const direction = clean(req.body?.direction) === 'out' ? 'out' : 'in';
      const signed = roundMoney(direction === 'out' ? -amount : amount);
      await runAsync(
        `UPDATE trader_payments SET trader_id=?,batch_id=?,order_id=?,amount=?,direction=?,signed_amount=?,payment_date=?,payment_method=?,reference_no=?,note=? WHERE id=?`,
        [num(req.body?.trader_id) || num(row.trader_id), num(req.body?.batch_id), num(req.body?.order_id), amount, direction, signed,
          clean(req.body?.payment_date) || row.payment_date, clean(req.body?.payment_method) || 'نقدي', clean(req.body?.reference_no), clean(req.body?.note), id]);
      await recordAudit({ req, action: 'update-trader-payment', entity_type: 'trader_payment', entity_id: id, details: `تعديل دفعة تاجر: ${amount}` });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* رصيد افتتاحي وحدود ائتمان للتاجر */
  app.post('/save-trader-finance-settings', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const trader = await getAsync(`SELECT * FROM traders WHERE id=?`, [id]);
      if (!trader) return res.status(404).json({ error: 'التاجر مش موجود' });
      await runAsync(`UPDATE traders SET opening_balance=?, credit_limit=?, payment_terms_days=?, default_discount_percent=? WHERE id=?`,
        [roundMoney(num(req.body?.opening_balance)), roundMoney(num(req.body?.credit_limit)),
          num(req.body?.payment_terms_days), roundMoney(num(req.body?.default_discount_percent)), id]);
      await recordAudit({ req, action: 'save-trader-finance-settings', entity_type: 'trader', entity_id: id, details: `ضبط حسابات التاجر: ${trader.name}` });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ===================== 4) فحوصات التجار ===================== */
  async function traderHealthChecks() {
    const issues = [];
    const push = (severity, title, details, data = {}) => issues.push({ severity, title, details, data: { module: 'traders', ...data } });

    const orphanLinks = await allAsync(
      `SELECT tbo.id, tbo.order_id, tbo.batch_id FROM trader_batch_orders tbo
       LEFT JOIN orders o ON o.id=tbo.order_id WHERE o.id IS NULL LIMIT 200`);
    if (orphanLinks.length) push('error', 'ربط طلبية بأوردر محذوف', `${orphanLinks.length} سطر ربط بأوردرات مش موجودة.`, { repair_mode: 'auto', repair_key: 'trader_orphan_links', affected_count: orphanLinks.length });

    const orphanBatchLinks = await allAsync(
      `SELECT tbo.id FROM trader_batch_orders tbo LEFT JOIN trader_batches b ON b.id=tbo.batch_id WHERE b.id IS NULL LIMIT 200`);
    if (orphanBatchLinks.length) push('error', 'ربط أوردر بطلبية محذوفة', `${orphanBatchLinks.length} سطر ربط بطلبيات مش موجودة.`, { repair_mode: 'auto', repair_key: 'trader_orphan_batch_links', affected_count: orphanBatchLinks.length });

    const orphanPayments = await allAsync(
      `SELECT p.id FROM trader_payments p LEFT JOIN traders t ON t.id=p.trader_id WHERE t.id IS NULL LIMIT 200`);
    if (orphanPayments.length) push('error', 'دفعات تجار بدون تاجر', `${orphanPayments.length} دفعة مرتبطة بتاجر محذوف.`, { repair_mode: 'manual', action_hint: 'راجع الدفعات دي وحدد التاجر الصح أو امسحها.' });

    const orphanCosts = await allAsync(
      `SELECT c.id FROM trader_batch_costs c LEFT JOIN trader_batches b ON b.id=c.batch_id WHERE b.id IS NULL LIMIT 200`);
    if (orphanCosts.length) push('error', 'تكاليف طلبيات بدون طلبية', `${orphanCosts.length} تكلفة مرتبطة بطلبية محذوفة.`, { repair_mode: 'auto', repair_key: 'trader_orphan_costs', affected_count: orphanCosts.length });

    // أوردر متعلم تاجر بس مش مربوط بطلبية
    const unlinked = await allAsync(
      `SELECT o.id, o.trader_id, o.trader_batch_id FROM orders o
       WHERE COALESCE(o.order_scope,'customer')='trader' AND COALESCE(o.trader_batch_id,0)>0
         AND NOT EXISTS (SELECT 1 FROM trader_batch_orders x WHERE x.order_id=o.id AND x.batch_id=o.trader_batch_id) LIMIT 200`);
    if (unlinked.length) push('warn', 'أوردر تاجر مش مربوط بطلبيته', `${unlinked.length} أوردر عليه رقم طلبية بس مش موجود في جدول الربط.`, { repair_mode: 'auto', repair_key: 'trader_relink_orders', affected_count: unlinked.length });

    // العكس: مربوط بطلبية بس order_scope مش trader
    const wrongScope = await allAsync(
      `SELECT o.id FROM trader_batch_orders tbo JOIN orders o ON o.id=tbo.order_id
       WHERE COALESCE(o.order_scope,'customer') <> 'trader' LIMIT 200`);
    if (wrongScope.length) push('error', 'أوردر داخل طلبية تاجر لكنه محسوب على المصنع', `${wrongScope.length} أوردر مربوط بطلبية تاجر لكن order_scope بتاعه مش trader — يعني فلوسه داخلة غلط في حسابات المصنع.`, { repair_mode: 'auto', repair_key: 'trader_fix_scope', affected_count: wrongScope.length });

    // مرآة scope على sales_history
    const scopeDrift = await getAsync(
      `SELECT COUNT(*) c FROM sales_history s JOIN orders o ON o.id=s.order_id
       WHERE COALESCE(s.order_scope,'') <> COALESCE(o.order_scope,'customer')`);
    if (num(scopeDrift?.c)) push('warn', 'نطاق الأوردر مش متزامن في سجل المبيعات', `${num(scopeDrift.c)} سطر في سجل المبيعات نطاقه مختلف عن الأوردر — ممكن يخلي أرباح التجار تدخل في أرباح المصنع.`, { repair_mode: 'auto', repair_key: 'trader_sync_sales_scope', affected_count: num(scopeDrift.c) });

    // تاجر عدّى حد الائتمان
    const traders = await allAsync(`SELECT id, name, credit_limit FROM traders WHERE COALESCE(credit_limit,0) > 0 AND COALESCE(is_active,1)=1`);
    for (const t of traders) {
      const f = await traderFinance(t.id, {});
      if (f && f.totals.remaining > num(t.credit_limit) + 0.01)
        push('warn', 'تاجر عدّى حد الائتمان', `${t.name}: عليه ${f.totals.remaining.toLocaleString('en-US')} والحد ${num(t.credit_limit).toLocaleString('en-US')}.`, { repair_mode: 'manual', action_hint: 'حصّل من التاجر أو ارفع حد الائتمان من صفحة التجار.' });
    }

    // طلبية مقفولة وعليها متبقي
    const closed = await allAsync(`SELECT * FROM trader_batches WHERE status IN ('مغلقة','تمت التسوية','تم التحصيل')`);
    for (const b of closed) {
      const f = await batchFinance(b);
      if (Math.abs(f.remaining) > 0.5)
        push('warn', 'طلبية مقفولة وعليها متبقي', `طلبية «${b.batch_name || b.id}» حالتها ${b.status} ولسه عليها ${f.remaining.toLocaleString('en-US')}.`, { repair_mode: 'manual', action_hint: 'سجّل التحصيل الناقص أو رجّع حالة الطلبية.', batch_id: b.id });
    }

    return issues;
  }

  async function traderRepair(repairKey) {
    const done = [];
    if (!repairKey || repairKey === 'trader_orphan_links') {
      const r = await runAsync(`DELETE FROM trader_batch_orders WHERE order_id NOT IN (SELECT id FROM orders)`);
      if (r.changes) done.push(`مسح ${r.changes} ربط بأوردر محذوف`);
    }
    if (!repairKey || repairKey === 'trader_orphan_batch_links') {
      const r = await runAsync(`DELETE FROM trader_batch_orders WHERE batch_id NOT IN (SELECT id FROM trader_batches)`);
      if (r.changes) done.push(`مسح ${r.changes} ربط بطلبية محذوفة`);
    }
    if (!repairKey || repairKey === 'trader_orphan_costs') {
      const r = await runAsync(`DELETE FROM trader_batch_costs WHERE batch_id NOT IN (SELECT id FROM trader_batches)`);
      if (r.changes) done.push(`مسح ${r.changes} تكلفة بدون طلبية`);
    }
    if (!repairKey || repairKey === 'trader_relink_orders') {
      const rows = await allAsync(
        `SELECT o.id, o.trader_batch_id FROM orders o
         WHERE COALESCE(o.order_scope,'customer')='trader' AND COALESCE(o.trader_batch_id,0)>0
           AND NOT EXISTS (SELECT 1 FROM trader_batch_orders x WHERE x.order_id=o.id AND x.batch_id=o.trader_batch_id)`);
      let n = 0;
      for (const row of rows) {
        const batch = await getAsync(`SELECT id FROM trader_batches WHERE id=?`, [num(row.trader_batch_id)]);
        if (!batch) continue;
        await runAsync(`INSERT OR IGNORE INTO trader_batch_orders (batch_id, order_id, note) VALUES (?,?,?)`, [num(row.trader_batch_id), num(row.id), 'ربط تلقائي من الفحص']);
        n++;
      }
      if (n) done.push(`ربط ${n} أوردر بطلبيته`);
    }
    if (!repairKey || repairKey === 'trader_fix_scope') {
      const r = await runAsync(
        `UPDATE orders SET order_scope='trader',
           trader_id = COALESCE((SELECT b.trader_id FROM trader_batch_orders tbo JOIN trader_batches b ON b.id=tbo.batch_id WHERE tbo.order_id=orders.id LIMIT 1), trader_id),
           trader_batch_id = COALESCE((SELECT tbo.batch_id FROM trader_batch_orders tbo WHERE tbo.order_id=orders.id LIMIT 1), trader_batch_id)
         WHERE id IN (SELECT tbo.order_id FROM trader_batch_orders tbo) AND COALESCE(order_scope,'customer') <> 'trader'`);
      if (r.changes) done.push(`تصحيح نطاق ${r.changes} أوردر تاجر`);
      await backfillSalesHistoryScope();
    }
    if (!repairKey || repairKey === 'trader_sync_sales_scope') {
      await backfillSalesHistoryScope();
      done.push('مزامنة نطاق الأوردرات في سجل المبيعات');
    }
    return done;
  }

  return { ensureTraderFinanceSchema, backfillSalesHistoryScope, traderHealthChecks, traderRepair, batchFinance, traderFinance, batchDirectCosts };
}

module.exports = { register };
