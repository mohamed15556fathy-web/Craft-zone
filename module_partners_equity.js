'use strict';
/* ============================================================================
   Craft Zone — حقوق الشركاء (v9.9)
   ----------------------------------------------------------------------------
   المشكلة اللي كانت موجودة:
     • النسبة كانت رقم يدوي مالوش أي علاقة بفلوس الشريك، ومحدش بيتأكد إن
       المجموع 100% — يعني ممكن توزّع 180% من الربح.
     • الربح كان بيتحسب من الصفر في كل مرة، فلو قفلت تسوية يناير وبعدين
       فلترت يناير–ديسمبر، ربح يناير بيتوزّع تاني.
     • رأس المال اللي بيدخله الشريك مكانش بيزوّد سيولة الشركة خالص.
     • مفيش «رصيد الشريك» الحقيقي — كان مجرد طرح أرقام في الشاشة.

   الحل: دفتر حقوق شريك حقيقي (partner_account_ledger) كل حركة فيه ليها اتجاه:
       + الشركة بقت مديونة للشريك (رأس مال داخل، نصيبه من الربح)
       − الشريك أخد فلوس (سحب، مصروف اتصرف من حسابه)
     رصيد الشريك = مجموع الحركات. والربح مش بيدخل الدفتر غير لما تقفل تسوية،
     فمستحيل يتوزّع مرتين.

   النسبة: تلقائي من رأس المال (نصيب الشريك ÷ إجمالي رأس المال) مع إمكانية
   تثبيت نسبة يدوية لأي شريك، والباقي بيتوزّع على اللي نسبتهم تلقائية.
   ========================================================================== */

const LEDGER_KINDS = {
  opening:      { sign: +1, label: 'رصيد افتتاحي' },
  capital_in:   { sign: +1, label: 'رأس مال داخل' },
  capital_out:  { sign: -1, label: 'استرداد رأس مال' },
  profit:       { sign: +1, label: 'نصيب من الأرباح' },
  loss:         { sign: -1, label: 'نصيب من الخسارة' },
  withdrawal:   { sign: -1, label: 'سحب شخصي' },
  expense:      { sign: -1, label: 'مصروف من حسابه' },
  adjust_in:    { sign: +1, label: 'تسوية بالزيادة' },
  adjust_out:   { sign: -1, label: 'تسوية بالنقص' },
};

function register(ctx) {
  const {
    app, runAsync, getAsync, allAsync, addColumnIfMissing,
    authRequired, requireAnyPerm, requirePerm,
    num, roundMoney, recordAudit, hasPerm,
  } = ctx;

  const clean = (v) => String(v ?? '').trim();
  const today = () => new Date().toISOString().slice(0, 10);
  const actorName = (req) => String(req?.user?.full_name || req?.user?.username || 'system');

  const canView = requireAnyPerm('perm_view_partners', 'perm_view_accounts');
  const canManage = requirePerm('perm_manage_financial_partners');
  const canTx = requireAnyPerm('perm_manage_partner_transactions', 'perm_manage_expenses');

  /* ===================== 1) الجداول ===================== */
  async function ensurePartnerEquitySchema() {
    for (const [n, d] of [
      ['opening_capital', 'REAL DEFAULT 0'],
      ['opening_balance', 'REAL DEFAULT 0'],
      ['share_mode', "TEXT DEFAULT 'auto'"],
      ['join_date', "TEXT DEFAULT ''"],
      ['exit_date', "TEXT DEFAULT ''"],
    ]) await addColumnIfMissing('financial_partners', n, d);

    // الشركاء القدام: اللي عنده نسبة متكتوبة يفضل يدوي علشان أرقامه ما تتغيرش فجأة
    await runAsync(`UPDATE financial_partners SET share_mode='manual'
                    WHERE COALESCE(share_mode,'')='' AND COALESCE(share_percent,0) > 0`);
    await runAsync(`UPDATE financial_partners SET share_mode='auto' WHERE COALESCE(share_mode,'')=''`);

    await addColumnIfMissing('partner_withdrawals', 'kind', "TEXT DEFAULT 'withdrawal'");
    await runAsync(`UPDATE partner_withdrawals SET kind='withdrawal' WHERE COALESCE(kind,'')=''`);

    await addColumnIfMissing('partner_settlements', 'is_posted', 'INTEGER DEFAULT 0');
    await addColumnIfMissing('partner_settlements', 'posted_at', "TEXT DEFAULT ''");

    await runAsync(`CREATE TABLE IF NOT EXISTS partner_account_ledger (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      partner_name TEXT DEFAULT '',
      entry_date TEXT DEFAULT CURRENT_DATE,
      entry_kind TEXT DEFAULT 'capital_in',
      amount REAL DEFAULT 0,
      delta REAL DEFAULT 0,
      note TEXT DEFAULT '',
      source_type TEXT DEFAULT 'manual',
      source_ref TEXT DEFAULT '',
      period_from TEXT DEFAULT '',
      period_to TEXT DEFAULT '',
      is_auto INTEGER DEFAULT 0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      created_by TEXT DEFAULT ''
    )`);
    for (const [n, d] of [['entry_kind', "TEXT DEFAULT 'capital_in'"], ['amount', 'REAL DEFAULT 0'], ['delta', 'REAL DEFAULT 0'],
      ['note', "TEXT DEFAULT ''"], ['source_type', "TEXT DEFAULT 'manual'"], ['source_ref', "TEXT DEFAULT ''"],
      ['period_from', "TEXT DEFAULT ''"], ['period_to', "TEXT DEFAULT ''"], ['is_auto', 'INTEGER DEFAULT 0'],
      ['created_at', 'TEXT DEFAULT CURRENT_TIMESTAMP'], ['created_by', "TEXT DEFAULT ''"]])
      await addColumnIfMissing('partner_account_ledger', n, d);
    await runAsync(`CREATE INDEX IF NOT EXISTS idx_partner_ledger_name ON partner_account_ledger(partner_name)`);
    await runAsync(`CREATE UNIQUE INDEX IF NOT EXISTS idx_partner_ledger_source ON partner_account_ledger(source_type, source_ref) WHERE source_type <> 'manual'`);

    await syncPartnerLedgerFromSources('system');
  }

  /* ===================== 2) مزامنة الدفتر من المصادر ===================== */
  async function upsertLedgerRow({ partnerName, date, kind, amount, note = '', sourceType, sourceRef, periodFrom = '', periodTo = '', createdBy = 'system' }) {
    const def = LEDGER_KINDS[kind];
    if (!def) return;
    const value = roundMoney(Math.abs(num(amount)));
    if (!(value > 0)) { await runAsync(`DELETE FROM partner_account_ledger WHERE source_type=? AND source_ref=?`, [sourceType, String(sourceRef)]); return; }
    const delta = roundMoney(def.sign * value);
    const existing = sourceType === 'manual' ? null : await getAsync(`SELECT id FROM partner_account_ledger WHERE source_type=? AND source_ref=?`, [sourceType, String(sourceRef)]);
    if (existing) {
      await runAsync(`UPDATE partner_account_ledger SET partner_name=?,entry_date=?,entry_kind=?,amount=?,delta=?,note=?,period_from=?,period_to=? WHERE id=?`,
        [clean(partnerName), clean(date) || today(), kind, value, delta, clean(note), clean(periodFrom), clean(periodTo), existing.id]);
      return existing.id;
    }
    const ins = await runAsync(
      `INSERT INTO partner_account_ledger (partner_name,entry_date,entry_kind,amount,delta,note,source_type,source_ref,period_from,period_to,is_auto,created_by)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
      [clean(partnerName), clean(date) || today(), kind, value, delta, clean(note), clean(sourceType), String(sourceRef),
        clean(periodFrom), clean(periodTo), sourceType === 'manual' ? 0 : 1, clean(createdBy)]);
    return ins.lastID;
  }

  /* بيبني الدفتر من: الرصيد الافتتاحي + رأس المال + السحوبات + التسويات المرحّلة. */
  async function syncPartnerLedgerFromSources(actor = 'system') {
    const partners = await allAsync(`SELECT * FROM financial_partners`);
    for (const p of partners) {
      const opening = roundMoney(num(p.opening_capital) + num(p.opening_balance));
      if (opening !== 0) {
        await upsertLedgerRow({
          partnerName: p.name, date: clean(p.join_date) || clean(p.created_at)?.slice(0, 10) || today(),
          kind: opening > 0 ? 'opening' : 'adjust_out', amount: Math.abs(opening),
          note: 'رصيد افتتاحي', sourceType: 'partner_opening', sourceRef: p.id, createdBy: actor,
        });
      } else {
        await runAsync(`DELETE FROM partner_account_ledger WHERE source_type='partner_opening' AND source_ref=?`, [String(p.id)]);
      }
    }

    const capital = await allAsync(`SELECT * FROM partner_capital_ledger`);
    for (const c of capital) {
      const isAdd = num(c.delta) >= 0;
      await upsertLedgerRow({
        partnerName: c.partner_name, date: c.entry_date, kind: isAdd ? 'capital_in' : 'capital_out',
        amount: Math.abs(num(c.delta) || num(c.amount)), note: c.note || (isAdd ? 'إضافة رأس مال' : 'استرداد رأس مال'),
        sourceType: 'partner_capital', sourceRef: c.id, createdBy: c.created_by || actor,
      });
    }

    const draws = await allAsync(`SELECT * FROM partner_withdrawals`);
    for (const w of draws) {
      await upsertLedgerRow({
        partnerName: w.partner_name, date: w.withdrawal_date, kind: 'withdrawal', amount: num(w.amount),
        note: w.note || 'سحب شخصي', sourceType: 'partner_withdrawal', sourceRef: w.id, createdBy: w.created_by || actor,
      });
    }

    // الأرباح: بس من التسويات المرحّلة (is_posted=1) — دي اللي بتمنع التوزيع المزدوج
    const posted = await allAsync(`SELECT * FROM partner_settlements WHERE COALESCE(is_posted,0)=1`);
    const postedIds = new Set(posted.map(s => String(s.id)));
    for (const s of posted) {
      const amount = roundMoney(num(s.profit_share));
      await upsertLedgerRow({
        partnerName: s.partner_name, date: s.period_to || s.created_at?.slice(0, 10) || today(),
        kind: amount >= 0 ? 'profit' : 'loss', amount: Math.abs(amount),
        note: `تسوية ${s.period_from} → ${s.period_to}`, sourceType: 'partner_settlement', sourceRef: s.id,
        periodFrom: s.period_from, periodTo: s.period_to, createdBy: s.created_by || actor,
      });
    }
    // تنضيف أرباح تسويات اتشالت أو اترجعت لغير مرحّلة
    const ledgerSettlements = await allAsync(`SELECT id, source_ref FROM partner_account_ledger WHERE source_type='partner_settlement'`);
    for (const row of ledgerSettlements) if (!postedIds.has(String(row.source_ref))) await runAsync(`DELETE FROM partner_account_ledger WHERE id=?`, [row.id]);

    // تنضيف حركات مصادرها اتمسحت
    await runAsync(`DELETE FROM partner_account_ledger WHERE source_type='partner_capital' AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM partner_capital_ledger)`);
    await runAsync(`DELETE FROM partner_account_ledger WHERE source_type='partner_withdrawal' AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM partner_withdrawals)`);
    await runAsync(`DELETE FROM partner_account_ledger WHERE source_type='partner_opening' AND CAST(source_ref AS INTEGER) NOT IN (SELECT id FROM financial_partners)`);
  }

  /* ===================== 3) النسب ===================== */
  /* النسبة التلقائية = رأس مال الشريك ÷ إجمالي رأس مال الشركاء التلقائيين،
     مضروبة في المتاح بعد النسب المثبتة يدويًا. */
  async function computePartnerShares() {
    const partners = await allAsync(`SELECT * FROM financial_partners WHERE COALESCE(is_active,1)=1 AND COALESCE(partner_type,'equity') <> 'murabaha'`);
    const capitalRows = await allAsync(
      `SELECT partner_name, COALESCE(SUM(delta),0) capital FROM partner_account_ledger
       WHERE entry_kind IN ('opening','capital_in','capital_out') GROUP BY partner_name`);
    const capitalOf = {};
    for (const r of capitalRows) capitalOf[clean(r.partner_name)] = roundMoney(num(r.capital));

    const manual = partners.filter(p => clean(p.share_mode) === 'manual');
    const auto = partners.filter(p => clean(p.share_mode) !== 'manual');
    const manualTotal = roundMoney(manual.reduce((a, p) => a + num(p.share_percent), 0));
    const remaining = roundMoney(Math.max(0, 100 - manualTotal));
    const autoCapitalTotal = roundMoney(auto.reduce((a, p) => a + Math.max(0, num(capitalOf[clean(p.name)])), 0));

    const out = {};
    for (const p of manual) out[clean(p.name)] = { percent: roundMoney(num(p.share_percent)), mode: 'manual', capital: num(capitalOf[clean(p.name)]) };
    for (const p of auto) {
      const cap = Math.max(0, num(capitalOf[clean(p.name)]));
      const percent = autoCapitalTotal > 0 ? roundMoney(remaining * cap / autoCapitalTotal)
        : (auto.length ? roundMoney(remaining / auto.length) : 0);
      out[clean(p.name)] = { percent, mode: 'auto', capital: cap };
    }
    const total = roundMoney(Object.values(out).reduce((a, v) => a + v.percent, 0));
    return {
      shares: out, manual_total: manualTotal, auto_count: auto.length,
      auto_capital_total: autoCapitalTotal, total,
      balanced: Math.abs(total - 100) < 0.01,
      warning: Math.abs(total - 100) < 0.01 ? '' :
        (manualTotal > 100 ? 'مجموع النسب اليدوية أكبر من 100% — صحّح نسب الشركاء.'
          : (!auto.length ? 'النسب اليدوية مجموعها مش 100% ومفيش شريك بنسبة تلقائية يستوعب الباقي.'
            : 'مفيش رأس مال مسجّل للشركاء التلقائيين، فالباقي اتقسم عليهم بالتساوي.')),
    };
  }

  /* فرق الكسور (قرش أو اتنين) بيتحطّ على أكبر نصيب علشان مجموع التوزيع
     يساوي الصافي بالظبط — من غير كده الأرقام بتفضل مش مقفولة. */
  function balanceRounding(lines, target) {
    const equity = lines.filter(l => l.partner_type !== 'murabaha');
    if (!equity.length) return;
    const sum = roundMoney(equity.reduce((a, l) => a + num(l.amount), 0));
    const diff = roundMoney(num(target) - sum);
    if (!diff) return;
    const biggest = equity.reduce((a, b) => (num(b.amount) > num(a.amount) ? b : a), equity[0]);
    biggest.amount = roundMoney(num(biggest.amount) + diff);
  }

  async function partnerEquityBalance(partnerName, upTo = '') {
    const params = [clean(partnerName)];
    let where = `partner_name=?`;
    if (clean(upTo)) { where += ` AND date(entry_date) <= date(?)`; params.push(clean(upTo)); }
    const row = await getAsync(`SELECT COALESCE(SUM(delta),0) v FROM partner_account_ledger WHERE ${where}`, params);
    return roundMoney(num(row?.v));
  }

  /* ===================== 4) المسارات ===================== */

  app.get('/partner-shares', authRequired, canView, async (req, res) => {
    try { await syncPartnerLedgerFromSources(actorName(req)); res.json(await computePartnerShares()); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* حقوق الشركاء الكاملة — الكارت الرئيسي في صفحة الشركاء. */
  app.get('/partners-equity', authRequired, canView, async (req, res) => {
    try {
      const to = clean(req.query.to);
      await syncPartnerLedgerFromSources(actorName(req));
      const shareInfo = await computePartnerShares();
      const partners = await allAsync(`SELECT * FROM financial_partners ORDER BY COALESCE(is_active,1) DESC, name ASC`);
      const rows = [];
      for (const p of partners) {
        const name = clean(p.name);
        const params = [name]; let where = `partner_name=?`;
        if (to) { where += ` AND date(entry_date) <= date(?)`; params.push(to); }
        const agg = await getAsync(
          `SELECT COALESCE(SUM(delta),0) equity,
                  COALESCE(SUM(CASE WHEN entry_kind IN ('opening','capital_in') THEN amount ELSE 0 END),0) capital_in,
                  COALESCE(SUM(CASE WHEN entry_kind='capital_out' THEN amount ELSE 0 END),0) capital_out,
                  COALESCE(SUM(CASE WHEN entry_kind='profit' THEN amount ELSE 0 END),0) profit_credited,
                  COALESCE(SUM(CASE WHEN entry_kind='loss' THEN amount ELSE 0 END),0) loss_charged,
                  COALESCE(SUM(CASE WHEN entry_kind IN ('withdrawal','expense') THEN amount ELSE 0 END),0) drawings
           FROM partner_account_ledger WHERE ${where}`, params);
        const share = shareInfo.shares[name] || { percent: 0, mode: clean(p.share_mode) || 'auto', capital: 0 };
        rows.push({
          id: p.id, name, partner_type: clean(p.partner_type) || 'equity',
          partner_account_type: clean(p.partner_account_type) || 'external',
          linked_admin_username: p.linked_admin_username, linked_admin_name: p.linked_admin_name,
          is_active: num(p.is_active, 1), phone: p.phone, notes: p.notes,
          share_mode: share.mode, share_percent: share.percent,
          manual_share_percent: roundMoney(num(p.share_percent)),
          profit_rate_percent: roundMoney(num(p.profit_rate_percent)),
          opening_capital: roundMoney(num(p.opening_capital) + num(p.opening_balance)),
          capital_net: roundMoney(num(agg?.capital_in) - num(agg?.capital_out)),
          capital_in: roundMoney(num(agg?.capital_in)), capital_out: roundMoney(num(agg?.capital_out)),
          profit_credited: roundMoney(num(agg?.profit_credited)), loss_charged: roundMoney(num(agg?.loss_charged)),
          drawings: roundMoney(num(agg?.drawings)),
          equity_balance: roundMoney(num(agg?.equity)),
        });
      }
      const totals = {
        partners: rows.filter(r => r.is_active === 1).length,
        capital_net: roundMoney(rows.reduce((a, r) => a + r.capital_net, 0)),
        profit_credited: roundMoney(rows.reduce((a, r) => a + r.profit_credited, 0)),
        drawings: roundMoney(rows.reduce((a, r) => a + r.drawings, 0)),
        equity_total: roundMoney(rows.reduce((a, r) => a + r.equity_balance, 0)),
      };
      res.json({ partners: rows, totals, shares: shareInfo, can: { manage: hasPerm(req.user, 'perm_manage_financial_partners'), tx: hasPerm(req.user, 'perm_manage_partner_transactions') || hasPerm(req.user, 'perm_manage_expenses') } });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* كشف حساب شريك برصيد جاري حقيقي. */
  app.get('/partner-statement/:name', authRequired, canView, async (req, res) => {
    try {
      const name = clean(decodeURIComponent(req.params.name));
      const from = clean(req.query.from), to = clean(req.query.to);
      const partner = await getAsync(`SELECT * FROM financial_partners WHERE name=?`, [name]);
      if (!partner) return res.status(404).json({ error: 'الشريك مش موجود' });
      await syncPartnerLedgerFromSources(actorName(req));

      let opening = 0;
      if (from) {
        const row = await getAsync(`SELECT COALESCE(SUM(delta),0) v FROM partner_account_ledger WHERE partner_name=? AND date(entry_date) < date(?)`, [name, from]);
        opening = roundMoney(num(row?.v));
      }
      const params = [name]; let where = `partner_name=?`;
      if (from) { where += ` AND date(entry_date) >= date(?)`; params.push(from); }
      if (to) { where += ` AND date(entry_date) <= date(?)`; params.push(to); }
      const raw = await allAsync(`SELECT * FROM partner_account_ledger WHERE ${where} ORDER BY date(entry_date) ASC, id ASC`, params);
      let running = opening;
      const rows = raw.map(r => {
        running = roundMoney(running + num(r.delta));
        return {
          id: r.id, date: r.entry_date, kind: LEDGER_KINDS[r.entry_kind]?.label || r.entry_kind,
          entry_kind: r.entry_kind, note: r.note,
          credit: num(r.delta) > 0 ? roundMoney(num(r.delta)) : 0,
          debit: num(r.delta) < 0 ? roundMoney(-num(r.delta)) : 0,
          balance: running, is_auto: num(r.is_auto), source_type: r.source_type,
        };
      });
      res.json({
        partner: { name, partner_type: clean(partner.partner_type) || 'equity' },
        period_from: from, period_to: to, opening_balance: opening, rows, closing_balance: roundMoney(running),
        totals: {
          credit: roundMoney(rows.reduce((a, r) => a + r.credit, 0)),
          debit: roundMoney(rows.reduce((a, r) => a + r.debit, 0)),
        },
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ضبط نمط النسبة والرصيد الافتتاحي. */
  app.post('/save-partner-equity-settings', authRequired, canManage, async (req, res) => {
    try {
      const id = num(req.body?.id);
      const partner = await getAsync(`SELECT * FROM financial_partners WHERE id=?`, [id]);
      if (!partner) return res.status(404).json({ error: 'الشريك مش موجود' });
      const shareMode = clean(req.body?.share_mode) === 'manual' ? 'manual' : 'auto';
      const manualShare = Math.min(100, Math.max(0, roundMoney(num(req.body?.share_percent))));
      await runAsync(`UPDATE financial_partners SET share_mode=?, share_percent=?, opening_capital=?, join_date=?, exit_date=? WHERE id=?`,
        [shareMode, shareMode === 'manual' ? manualShare : roundMoney(num(partner.share_percent)),
          roundMoney(num(req.body?.opening_capital)), clean(req.body?.join_date), clean(req.body?.exit_date), id]);
      await syncPartnerLedgerFromSources(actorName(req));
      const shares = await computePartnerShares();
      await recordAudit({ req, action: 'save-partner-equity-settings', entity_type: 'financial_partner', entity_id: id, details: `ضبط نسبة ${partner.name}: ${shareMode}` });
      res.json({ success: true, shares });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* معاينة التوزيع قبل ما تقفل التسوية. */
  app.get('/partner-settlement-preview', authRequired, canView, async (req, res) => {
    try {
      const from = clean(req.query.from), to = clean(req.query.to);
      if (!from || !to) return res.status(400).json({ error: 'حدد الفترة من وإلى' });
      await syncPartnerLedgerFromSources(actorName(req));
      const shareInfo = await computePartnerShares();

      // الربح القابل للتوزيع = ربح المصنع في الفترة (من غير التجار) ناقص المصاريف العامة
      const profitRow = await getAsync(
        `SELECT COALESCE(SUM(net_profit),0) v FROM sales_history
         WHERE NOT EXISTS (SELECT 1 FROM orders _so WHERE _so.id = sales_history.order_id AND COALESCE(_so.order_scope,'customer') = 'trader')
           AND date(sale_date) >= date(?) AND date(sale_date) <= date(?)`, [from, to]);
      const expRow = await getAsync(
        `SELECT COALESCE(SUM(amount),0) v FROM expenses
         WHERE linked_to_order=0 AND COALESCE(source_type,'') <> 'production_order'
           AND date(expense_date) >= date(?) AND date(expense_date) <= date(?)`, [from, to]);
      const grossProfit = roundMoney(num(profitRow?.v));
      const generalExpenses = roundMoney(num(expRow?.v));
      const netProfit = roundMoney(grossProfit - generalExpenses);

      // اللي اتوزّع قبل كده في فترات متداخلة
      const alreadyRow = await getAsync(
        `SELECT COALESCE(SUM(amount),0) v FROM partner_account_ledger
         WHERE entry_kind IN ('profit','loss') AND source_type='partner_settlement'
           AND NOT (date(period_to) < date(?) OR date(period_from) > date(?))`, [from, to]);
      const alreadyAllocated = roundMoney(num(alreadyRow?.v));
      const distributable = roundMoney(netProfit - alreadyAllocated);

      const partners = await allAsync(`SELECT * FROM financial_partners WHERE COALESCE(is_active,1)=1`);
      const lines = [];
      for (const p of partners) {
        const name = clean(p.name);
        const type = clean(p.partner_type) || 'equity';
        if (type === 'murabaha') {
          const capital = await partnerEquityBalance(name, to);
          const amount = roundMoney(Math.max(0, capital) * num(p.profit_rate_percent) / 100);
          lines.push({ name, partner_type: type, share_percent: 0, profit_rate_percent: roundMoney(num(p.profit_rate_percent)), basis: roundMoney(Math.max(0, capital)), amount, method: 'نسبة على رأس المال' });
        } else {
          const share = shareInfo.shares[name] || { percent: 0, mode: 'auto' };
          lines.push({ name, partner_type: type, share_percent: share.percent, share_mode: share.mode, profit_rate_percent: 0, basis: distributable, amount: roundMoney(distributable * share.percent / 100), method: share.mode === 'manual' ? 'نسبة يدوية' : 'نسبة من رأس المال' });
        }
      }
      balanceRounding(lines, distributable);
      const murabahaTotal = roundMoney(lines.filter(l => l.partner_type === 'murabaha').reduce((a, l) => a + l.amount, 0));
      const equityTotal = roundMoney(lines.filter(l => l.partner_type !== 'murabaha').reduce((a, l) => a + l.amount, 0));
      res.json({
        period_from: from, period_to: to,
        gross_profit: grossProfit, general_expenses: generalExpenses, net_profit: netProfit,
        already_allocated: alreadyAllocated, distributable,
        lines, equity_total: equityTotal, murabaha_total: murabahaTotal,
        shares: shareInfo,
        unallocated: roundMoney(distributable - equityTotal),
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ترحيل التسوية: بيكتب نصيب كل شريك في دفتر حقوقه — وبعدها مش ممكن يتوزّع تاني. */
  app.post('/post-partner-settlement', authRequired, canTx, async (req, res) => {
    try {
      const from = clean(req.body?.period_from), to = clean(req.body?.period_to);
      if (!from || !to) return res.status(400).json({ error: 'حدد الفترة من وإلى' });
      const note = clean(req.body?.note);

      const overlap = await getAsync(
        `SELECT COUNT(*) c FROM partner_settlements WHERE COALESCE(is_posted,0)=1
           AND NOT (date(period_to) < date(?) OR date(period_from) > date(?))`, [from, to]);
      if (num(overlap?.c) > 0 && !num(req.body?.force))
        return res.status(400).json({ error: 'في تسوية مرحّلة بتتقاطع مع الفترة دي. امسحها الأول أو غيّر الفترة.', overlap: true });

      // نجيب المعاينة بنفس المنطق
      const shareInfo = await computePartnerShares();
      if (!shareInfo.balanced && !num(req.body?.force))
        return res.status(400).json({ error: `مجموع النسب ${shareInfo.total}% مش 100%. ${shareInfo.warning}`, shares: shareInfo });

      const profitRow = await getAsync(
        `SELECT COALESCE(SUM(net_profit),0) v FROM sales_history
         WHERE NOT EXISTS (SELECT 1 FROM orders _so WHERE _so.id = sales_history.order_id AND COALESCE(_so.order_scope,'customer') = 'trader')
           AND date(sale_date) >= date(?) AND date(sale_date) <= date(?)`, [from, to]);
      const expRow = await getAsync(
        `SELECT COALESCE(SUM(amount),0) v FROM expenses
         WHERE linked_to_order=0 AND COALESCE(source_type,'') <> 'production_order'
           AND date(expense_date) >= date(?) AND date(expense_date) <= date(?)`, [from, to]);
      const netProfit = roundMoney(num(profitRow?.v) - num(expRow?.v));
      const alreadyRow = await getAsync(
        `SELECT COALESCE(SUM(amount),0) v FROM partner_account_ledger
         WHERE entry_kind IN ('profit','loss') AND source_type='partner_settlement'
           AND NOT (date(period_to) < date(?) OR date(period_from) > date(?))`, [from, to]);
      const distributable = roundMoney(netProfit - num(alreadyRow?.v));

      await runAsync(`DELETE FROM partner_settlements WHERE period_from=? AND period_to=?`, [from, to]);
      const partners = await allAsync(`SELECT * FROM financial_partners WHERE COALESCE(is_active,1)=1`);
      const planned = [];
      for (const p of partners) {
        const name = clean(p.name);
        const type = clean(p.partner_type) || 'equity';
        let sharePercent = 0, amount = 0;
        if (type === 'murabaha') {
          const capital = await partnerEquityBalance(name, to);
          amount = roundMoney(Math.max(0, capital) * num(p.profit_rate_percent) / 100);
        } else {
          sharePercent = (shareInfo.shares[name] || {}).percent || 0;
          amount = roundMoney(distributable * sharePercent / 100);
        }
        planned.push({ p, name, type, sharePercent, amount, partner_type: type });
      }
      balanceRounding(planned, distributable);
      const created = [];
      for (const row of planned) {
        const { p, name, type, sharePercent, amount } = row;
        if (!amount) continue;
        const equity = await partnerEquityBalance(name, to);
        const ins = await runAsync(
          `INSERT INTO partner_settlements (period_from,period_to,partner_name,share_percent,partner_type,profit_rate_percent,total_profit,profit_share,withdrawals_total,custody_expenses_total,fund_balance,capital_balance,final_amount,note,status,is_posted,posted_at,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          [from, to, name, sharePercent, type, roundMoney(num(p.profit_rate_percent)), netProfit, amount, 0, 0, 0, equity, amount, note, 'posted', 1, new Date().toISOString(), actorName(req)]);
        created.push({ id: ins.lastID, name, amount, share_percent: sharePercent });
      }
      await syncPartnerLedgerFromSources(actorName(req));
      await recordAudit({ req, action: 'post-partner-settlement', entity_type: 'partner_settlement', entity_id: 0, details: `ترحيل تسوية ${from} → ${to} بصافي ${netProfit}` });
      res.json({ success: true, period_from: from, period_to: to, net_profit: netProfit, distributable, lines: created });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post('/unpost-partner-settlement', authRequired, canTx, async (req, res) => {
    try {
      const from = clean(req.body?.period_from), to = clean(req.body?.period_to);
      if (!from || !to) return res.status(400).json({ error: 'حدد الفترة' });
      await runAsync(`DELETE FROM partner_settlements WHERE period_from=? AND period_to=?`, [from, to]);
      await syncPartnerLedgerFromSources(actorName(req));
      await recordAudit({ req, action: 'unpost-partner-settlement', entity_type: 'partner_settlement', entity_id: 0, details: `إلغاء ترحيل تسوية ${from} → ${to}` });
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* تسوية يدوية على حساب الشريك (نادرة بس لازمة). */
  app.post('/save-partner-ledger-adjust', authRequired, canTx, async (req, res) => {
    try {
      const name = clean(req.body?.partner_name);
      const partner = await getAsync(`SELECT * FROM financial_partners WHERE name=?`, [name]);
      if (!partner) return res.status(400).json({ error: 'اختار شريك صحيح' });
      const kind = clean(req.body?.entry_kind) === 'adjust_out' ? 'adjust_out' : 'adjust_in';
      const amount = roundMoney(num(req.body?.amount));
      if (!(amount > 0)) return res.status(400).json({ error: 'اكتب المبلغ' });
      const def = LEDGER_KINDS[kind];
      const ins = await runAsync(
        `INSERT INTO partner_account_ledger (partner_name,entry_date,entry_kind,amount,delta,note,source_type,source_ref,is_auto,created_by)
         VALUES (?,?,?,?,?,?,'manual','',0,?)`,
        [name, clean(req.body?.entry_date) || today(), kind, amount, roundMoney(def.sign * amount), clean(req.body?.note), actorName(req)]);
      await recordAudit({ req, action: 'save-partner-ledger-adjust', entity_type: 'partner_ledger', entity_id: ins.lastID, details: `${def.label} لـ ${name}: ${amount}` });
      res.json({ success: true, id: ins.lastID });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.delete('/delete-partner-ledger-adjust/:id', authRequired, canTx, async (req, res) => {
    try {
      const id = num(req.params.id);
      const row = await getAsync(`SELECT * FROM partner_account_ledger WHERE id=?`, [id]);
      if (!row) return res.status(404).json({ error: 'الحركة مش موجودة' });
      if (num(row.is_auto) === 1) return res.status(400).json({ error: 'الحركة دي تلقائية — امسحها من مكانها الأصلي (رأس مال/سحب/تسوية).' });
      await runAsync(`DELETE FROM partner_account_ledger WHERE id=?`, [id]);
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  /* ===================== 5) فحوصات الشركاء ===================== */
  async function partnerHealthChecks() {
    const issues = [];
    const push = (severity, title, details, data = {}) => issues.push({ severity, title, details, data: { module: 'partners', ...data } });

    const shareInfo = await computePartnerShares();
    if (!shareInfo.balanced)
      push('error', 'مجموع نسب الشركاء مش 100%', `المجموع الحالي ${shareInfo.total}%. ${shareInfo.warning}`, { repair_mode: 'manual', action_hint: 'من صفحة الشركاء خلّي شريك أو أكتر نسبته «تلقائي من رأس المال» علشان الباقي يتوزّع صح.' });

    for (const [name, s] of Object.entries(shareInfo.shares))
      if (s.mode === 'auto' && s.capital <= 0)
        push('warn', 'شريك بنسبة تلقائية بدون رأس مال', `${name}: نسبته محسوبة تلقائيًا من رأس المال بس مفيش رأس مال مسجّل ليه.`, { repair_mode: 'manual', action_hint: 'سجّل رأس مال الشريك أو ثبّت نسبته يدويًا.' });

    // حركات دفتر بأسماء شركاء مش موجودين
    const orphan = await allAsync(
      `SELECT DISTINCT partner_name FROM partner_account_ledger
       WHERE partner_name NOT IN (SELECT name FROM financial_partners) LIMIT 50`);
    if (orphan.length) push('error', 'حركات حقوق لشركاء محذوفين', `${orphan.length} اسم في دفتر الحقوق مش موجود في قائمة الشركاء: ${orphan.map(o => o.partner_name).join('، ')}`, { repair_mode: 'manual', action_hint: 'رجّع الشريك بنفس الاسم أو حوّل حركاته لشريك تاني.' });

    for (const table of ['partner_withdrawals', 'partner_capital_ledger', 'partner_fund_ledger']) {
      const rows = await allAsync(`SELECT DISTINCT partner_name FROM ${table} WHERE partner_name NOT IN (SELECT name FROM financial_partners) LIMIT 50`);
      if (rows.length) push('warn', 'سجلات مالية لأسماء شركاء غير موجودة', `جدول ${table}: ${rows.map(r => r.partner_name).join('، ')}`, { repair_mode: 'manual', action_hint: 'أضف الشريك بنفس الاسم أو عدّل السجلات.' });
    }

    // دفتر الحقوق مش متزامن
    const capitalCount = num((await getAsync(`SELECT COUNT(*) c FROM partner_capital_ledger`))?.c);
    const capitalLedger = num((await getAsync(`SELECT COUNT(*) c FROM partner_account_ledger WHERE source_type='partner_capital'`))?.c);
    const drawCount = num((await getAsync(`SELECT COUNT(*) c FROM partner_withdrawals`))?.c);
    const drawLedger = num((await getAsync(`SELECT COUNT(*) c FROM partner_account_ledger WHERE source_type='partner_withdrawal'`))?.c);
    if (capitalCount !== capitalLedger || drawCount !== drawLedger)
      push('warn', 'دفتر حقوق الشركاء محتاج مزامنة', `رأس المال ${capitalLedger}/${capitalCount}، السحوبات ${drawLedger}/${drawCount}.`, { repair_mode: 'auto', repair_key: 'partner_sync_ledger' });

    // شريك سحب أكتر من حقوقه
    const partners = await allAsync(`SELECT name FROM financial_partners WHERE COALESCE(is_active,1)=1`);
    for (const p of partners) {
      const bal = await partnerEquityBalance(p.name);
      if (bal < -0.5) push('warn', 'شريك سحب أكتر من حقوقه', `${p.name}: رصيده ${bal.toLocaleString('en-US')} — يعني مدين للشركة.`, { repair_mode: 'manual', action_hint: 'راجع سحوباته أو رحّل تسوية أرباح للفترة.' });
    }

    // تسويات متقاطعة
    const posted = await allAsync(`SELECT id, period_from, period_to FROM partner_settlements WHERE COALESCE(is_posted,0)=1 GROUP BY period_from, period_to`);
    for (let i = 0; i < posted.length; i++) for (let j = i + 1; j < posted.length; j++) {
      const a = posted[i], b = posted[j];
      if (!(String(a.period_to) < String(b.period_from) || String(a.period_from) > String(b.period_to)))
        push('error', 'تسويتان مرحّلتان بفترتين متقاطعتين', `«${a.period_from} → ${a.period_to}» و«${b.period_from} → ${b.period_to}» — الربح ممكن يكون اتوزّع مرتين.`, { repair_mode: 'manual', action_hint: 'ألغِ ترحيل واحدة منهم من صفحة الشركاء.' });
    }

    // تسويات قديمة غير مرحّلة (بالنظام القديم)
    const legacy = num((await getAsync(`SELECT COUNT(*) c FROM partner_settlements WHERE COALESCE(is_posted,0)=0`))?.c);
    if (legacy) push('warn', 'تسويات قديمة غير مرحّلة', `${legacy} تسوية اتقفلت بالنظام القديم ومش داخلة في حقوق الشركاء. رحّلها أو امسحها.`, { repair_mode: 'manual', action_hint: 'من صفحة الشركاء: راجع التسويات القديمة ورحّلها من جديد.' });

    return issues;
  }

  async function partnerRepair(repairKey) {
    const done = [];
    if (!repairKey || repairKey === 'partner_sync_ledger') {
      await syncPartnerLedgerFromSources('system');
      done.push('مزامنة دفتر حقوق الشركاء');
    }
    return done;
  }

  return { ensurePartnerEquitySchema, syncPartnerLedgerFromSources, computePartnerShares, partnerEquityBalance, partnerHealthChecks, partnerRepair };
}

module.exports = { register, LEDGER_KINDS };
