'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const sqlite3 = require('sqlite3').verbose();

const dbPath = path.resolve(process.argv[2] || process.env.DATABASE_PATH || 'data/database.db');
if (!fs.existsSync(dbPath)) {
  console.error(JSON.stringify({ ok: false, error: `Database not found: ${dbPath}` }, null, 2));
  process.exit(2);
}

const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY);
const all = (sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
const get = (sql, params = []) => new Promise((resolve, reject) => db.get(sql, params, (error, row) => error ? reject(error) : resolve(row)));
const close = () => new Promise(resolve => db.close(() => resolve()));
const num = value => Number.isFinite(Number(value)) ? Number(value) : 0;
const money = value => Math.round((num(value) + Number.EPSILON) * 100) / 100;
const near = (actual, expected, tolerance = 0.011) => Math.abs(num(actual) - num(expected)) <= tolerance;
const COST_FIELDS = ['cost_cut','cost_print','cost_zinc','cost_design','cost_make','cost_hand','cost_paper','cost_hand_fix','cost_forme','cost_lamination','cost_breaking','cost_stretch'];
const AUTO_CASH_SOURCES = ['order_payment','purchase_payment','expense','fixed_asset','order_cost','refund'];

const checks = [];
function check(name, pass, details = {}) {
  checks.push({ name, pass: Boolean(pass), details });
}

async function validateReferenceGroup(groupCode, expectedTotal, expectedPaid) {
  const rows = await all(`SELECT id,item_no,total_price,paid_amount,remaining_amount,shipping_cost FROM orders WHERE group_code=? ORDER BY item_no,id`, [groupCode]);
  const leader = rows.find(row => num(row.item_no) === 1) || rows[0];
  const children = rows.filter(row => num(row.id) !== num(leader?.id));
  const childrenZero = children.every(row => [row.total_price,row.paid_amount,row.remaining_amount,row.shipping_cost].every(value => near(value, 0)));
  check(`group:${groupCode}`, rows.length > 0 && near(leader?.total_price, expectedTotal) && near(leader?.paid_amount, expectedPaid) && near(leader?.remaining_amount, expectedTotal - expectedPaid) && childrenZero, {
    itemCount: rows.length,
    leader: leader ? { id: leader.id, total: leader.total_price, paid: leader.paid_amount, remaining: leader.remaining_amount } : null,
    childrenZero
  });
}

async function makeFingerprint() {
  const datasets = {};
  datasets.cash = await all(`SELECT admin_username,source_type,source_ref,ROUND(delta,2) delta,ROUND(balance_after,2) balance_after FROM admin_cash_ledger ORDER BY admin_username,id`);
  datasets.purchases = await all(`SELECT id,unit,ROUND(quantity,6) quantity,ROUND(unit_price,6) unit_price,ROUND(total_price,2) total_price,ROUND(paper_weight_kg,6) paper_weight_kg,ROUND(paper_sheets_equivalent,6) paper_sheets_equivalent FROM purchases ORDER BY id`);
  datasets.paper = await all(`SELECT id,ROUND(total_kg,6) total_kg,ROUND(total_sheets,6) total_sheets,ROUND(buy_price_kg,6) buy_price_kg,ROUND(buy_price_sheet,6) buy_price_sheet FROM paper ORDER BY id`);
  datasets.handles = await all(`SELECT id,ROUND(qty,6) qty FROM handles ORDER BY id`);
  datasets.bags = await all(`SELECT id,ROUND(total_qty,6) total_qty FROM bags ORDER BY id`);
  datasets.orders = await all(`SELECT id,status,group_code,item_no,ROUND(total_price,2) total_price,ROUND(paid_amount,2) paid_amount,ROUND(remaining_amount,2) remaining_amount,ROUND(shipping_cost,2) shipping_cost,${COST_FIELDS.map(field => `ROUND(${field},2) ${field}`).join(',')},ready_stock_deducted,bag_returned_to_stock,paper_cut_done,handle_stock_deducted,breaking_sheet_class,form_id,form_family_snapshot FROM orders ORDER BY id`);
  datasets.costSnapshots = await all(`SELECT order_id,cost_field,ROUND(amount,2) amount FROM cost_history WHERE source='snapshot' AND source_ref='current' ORDER BY order_id,cost_field`);
  datasets.sales = await all(`SELECT order_id,ROUND(gross_sale,2) gross_sale,ROUND(shipping_cost,2) shipping_cost,ROUND(insurance_fee,2) insurance_fee,ROUND(extra_cod_fee,2) extra_cod_fee,ROUND(other_shipping_fee,2) other_shipping_fee,ROUND(total_deductions,2) total_deductions,ROUND(total_sale,2) total_sale,ROUND(total_cost,2) total_cost,ROUND(net_profit,2) net_profit,ROUND(paid_amount,2) paid_amount,ROUND(remaining_amount,2) remaining_amount,status FROM sales_history ORDER BY order_id`);
  datasets.forms = await all(`SELECT id,product_type,form_family,sheet_class FROM forms ORDER BY id`);
  return crypto.createHash('sha256').update(JSON.stringify(datasets)).digest('hex');
}

async function main() {
  const purchase = await get(`SELECT * FROM purchases WHERE id=12`);
  check('reference-paper-purchase-12', purchase && purchase.unit === 'فرخ' && near(purchase.quantity, 6000, 0.000001) && near(purchase.unit_price, 4.5, 0.000001) && near(purchase.total_price, 27000) && near(purchase.paper_sheet_weight_kg, 0.084, 0.0000001) && near(purchase.paper_weight_kg, 504, 0.000001) && near(purchase.paper_sheets_equivalent, 6000, 0.000001) && near(purchase.paper_unit_price_kg, 53.571428, 0.000002) && near(purchase.paper_unit_price_sheet, 4.5, 0.000001), purchase || {});

  const purchaseHistory = await get(`SELECT COALESCE(SUM(kg),0) kg,COALESCE(SUM(sheets),0) sheets FROM paper_history WHERE source_type='purchase' AND source_ref='12' AND type='purchase'`);
  check('reference-paper-history-12', near(purchaseHistory?.kg, 504, 0.001) && near(purchaseHistory?.sheets, 6000, 0.001), purchaseHistory || {});

  const assets = await get(`SELECT COUNT(*) count,COALESCE(SUM(purchase_price),0) total FROM fixed_assets`);
  const badAssetCash = await get(`SELECT COUNT(*) count FROM (
    SELECT a.id FROM fixed_assets a LEFT JOIN admin_cash_ledger l ON l.source_type='fixed_asset' AND l.source_ref=CAST(a.id AS TEXT)
    GROUP BY a.id HAVING COUNT(l.id)<>1 OR ABS(COALESCE(SUM(l.delta),0)+COALESCE(a.purchase_price,0))>0.01
  )`);
  const assetExpenses = await get(`SELECT COUNT(*) count FROM expenses WHERE TRIM(COALESCE(source_type,''))='fixed_asset'`);
  check('fixed-assets-cash-not-profit', near(assets?.total, 20930) && num(badAssetCash?.count) === 0 && num(assetExpenses?.count) === 0, { assets, badAssetCash: badAssetCash?.count, assetExpenses: assetExpenses?.count });

  await validateReferenceGroup('GRP-1782821361663', 2250, 2250);
  await validateReferenceGroup('GRP-1782935425220', 690, 690);
  await validateReferenceGroup('GRP-1785761735225', 1270, 0);

  const overpaid = await get(`SELECT COUNT(*) count FROM orders WHERE COALESCE(item_no,1)=1 AND COALESCE(paid_amount,0)>COALESCE(total_price,0)+0.01`);
  check('no-order-overpayment', num(overpaid?.count) === 0, overpaid || {});
  const deliveredZero = await get(`SELECT COUNT(*) count FROM orders WHERE status='تم التسليم' AND COALESCE(item_no,1)=1 AND COALESCE(total_price,0)<=0`);
  check('no-delivered-zero-price-leader', num(deliveredZero?.count) === 0, deliveredZero || {});
  const returnedDebt = await get(`SELECT COUNT(*) count FROM orders WHERE status='مرتجع' AND ABS(COALESCE(remaining_amount,0))>0.01`);
  check('returned-orders-have-zero-remaining', num(returnedDebt?.count) === 0, returnedDebt || {});
  const childFinance = await get(`SELECT COUNT(*) count FROM orders WHERE TRIM(COALESCE(group_code,''))<>'' AND COALESCE(item_no,1)>1 AND (ABS(COALESCE(total_price,0))+ABS(COALESCE(paid_amount,0))+ABS(COALESCE(remaining_amount,0))+ABS(COALESCE(shipping_cost,0)))>0.01`);
  check('group-children-have-zero-financials', num(childFinance?.count) === 0, childFinance || {});

  const stock = {
    paperKg: (await get(`SELECT COUNT(*) count FROM paper WHERE COALESCE(total_kg,0)<-0.000001`)).count,
    paperSheets: (await get(`SELECT COUNT(*) count FROM paper WHERE COALESCE(total_sheets,0)<-0.000001`)).count,
    handles: (await get(`SELECT COUNT(*) count FROM handles WHERE COALESCE(qty,0)<-0.000001`)).count,
    bags: (await get(`SELECT COUNT(*) count FROM bags WHERE COALESCE(total_qty,0)<-0.000001`)).count
  };
  check('no-negative-stock', Object.values(stock).every(value => num(value) === 0), stock);

  const missingCash = {
    orders: (await get(`SELECT COUNT(*) count FROM order_payments p LEFT JOIN admin_cash_ledger l ON l.source_type='order_payment' AND l.source_ref=CAST(p.id AS TEXT) WHERE l.id IS NULL`)).count,
    purchases: (await get(`SELECT COUNT(*) count FROM purchase_payments p LEFT JOIN admin_cash_ledger l ON l.source_type='purchase_payment' AND l.source_ref=CAST(p.id AS TEXT) WHERE l.id IS NULL`)).count,
    expenses: (await get(`SELECT COUNT(*) count FROM expenses e LEFT JOIN admin_cash_ledger l ON l.source_type='expense' AND l.source_ref=CAST(e.id AS TEXT) WHERE TRIM(COALESCE(e.actor_username,''))<>'' AND l.id IS NULL`)).count
  };
  check('all-financial-records-have-cash-ledger', Object.values(missingCash).every(value => num(value) === 0), missingCash);
  const duplicateCash = await all(`SELECT source_type,source_ref,COUNT(*) count FROM admin_cash_ledger WHERE source_type IN (${AUTO_CASH_SOURCES.map(() => '?').join(',')}) AND TRIM(COALESCE(source_ref,''))<>'' GROUP BY source_type,source_ref HAVING COUNT(*)>1`, AUTO_CASH_SOURCES);
  check('automatic-cash-ledger-is-idempotent', duplicateCash.length === 0, { duplicates: duplicateCash });

  const currentCosts = await all(`SELECT id,${COST_FIELDS.join(',')} FROM orders ORDER BY id`);
  const snapshots = await all(`SELECT order_id,cost_field,amount FROM cost_history WHERE source='snapshot' AND source_ref='current'`);
  const snapshotMap = new Map(snapshots.map(row => [`${num(row.order_id)}:${row.cost_field}`, num(row.amount)]));
  const snapshotDrift = [];
  for (const order of currentCosts) for (const field of COST_FIELDS) {
    const key = `${num(order.id)}:${field}`;
    if (!snapshotMap.has(key) || !near(snapshotMap.get(key), order[field])) snapshotDrift.push(key);
  }
  check('latest-cost-history-matches-orders', snapshotDrift.length === 0, { driftCount: snapshotDrift.length, examples: snapshotDrift.slice(0, 10) });

  const salesRows = await all(`SELECT o.*,s.id sale_id,s.gross_sale s_gross,s.shipping_cost s_shipping,s.insurance_fee s_insurance,s.extra_cod_fee s_cod,s.other_shipping_fee s_other,s.total_deductions s_deductions,s.total_sale s_total_sale,s.total_cost s_total_cost,s.net_profit s_profit,s.remaining_amount s_remaining,s.status s_status FROM orders o LEFT JOIN sales_history s ON s.order_id=o.id WHERE TRIM(COALESCE(o.status,'')) IN ('تم التسليم','مرتجع')`);
  const salesDrift = [];
  for (const row of salesRows) {
    if (!row.sale_id) { salesDrift.push({ id: row.id, reason: 'missing' }); continue; }
    const expectedGross = row.status === 'تم التسليم' ? Math.max(0, num(row.total_price)) : 0;
    const expectedDeductions = money(num(row.s_shipping) + num(row.s_insurance) + num(row.s_cod) + num(row.s_other));
    const expectedSale = money(expectedGross - expectedDeductions);
    const rawCost = COST_FIELDS.reduce((sum, field) => sum + num(row[field]), 0);
    const recovered = row.status === 'مرتجع' && num(row.useReadyStock) === 1 && String(row.printType || 'سادة').trim() === 'سادة' && num(row.bag_returned_to_stock) === 1 ? num(row.cost_make) : 0;
    const expectedCost = money(Math.max(0, rawCost - recovered));
    const bad = !near(row.s_gross, expectedGross) || !near(row.s_deductions, expectedDeductions) || !near(row.s_total_sale, expectedSale) || !near(row.s_total_cost, expectedCost) || !near(row.s_profit, money(expectedSale - expectedCost)) || String(row.s_status || '').trim() !== String(row.status || '').trim() || (row.status === 'مرتجع' && !near(row.s_remaining, 0));
    if (bad) salesDrift.push({ id: row.id, expectedGross, expectedDeductions, expectedSale, expectedCost });
  }
  check('sales-history-matches-orders', salesDrift.length === 0, { driftCount: salesDrift.length, examples: salesDrift.slice(0, 10) });

  const invalidForms = await get(`SELECT COUNT(*) count FROM forms WHERE TRIM(COALESCE(form_family,'')) NOT IN ('bag','box','pouch','other') OR TRIM(COALESCE(sheet_class,'')) NOT IN ('quarter','half','full')`);
  const invalidFormSnapshots = await get(`SELECT COUNT(*) count FROM orders WHERE COALESCE(form_id,0)>0 AND TRIM(COALESCE(form_family_snapshot,'')) NOT IN ('bag','box','pouch','other')`);
  check('forms-have-explicit-classification', num(invalidForms?.count) === 0 && num(invalidFormSnapshots?.count) === 0, { invalidForms: invalidForms?.count, invalidSnapshots: invalidFormSnapshots?.count });

  const order165Leader = await get(`SELECT id,group_code FROM orders WHERE store_synced_display_no=165 AND COALESCE(item_no,1)=1 ORDER BY id LIMIT 1`);
  const order165Items = order165Leader ? await all(`SELECT *,(${COST_FIELDS.map(field => `COALESCE(${field},0)`).join('+')}) item_cost FROM orders WHERE group_code=? ORDER BY item_no,id`, [order165Leader.group_code]) : [];
  const order165TotalCost = money(order165Items.reduce((sum, row) => sum + num(row.item_cost), 0));
  check('reference-order-165-costs', order165Items.length === 3 && order165Items.every(row => near(row.cost_paper, 9000) && near(row.cost_lamination, 4000)) && near(order165TotalCost, 51128.15), {
    leaderId: order165Leader?.id || null,
    itemCount: order165Items.length,
    itemPaperCosts: order165Items.map(row => row.cost_paper),
    itemLaminationCosts: order165Items.map(row => row.cost_lamination),
    totalCost: order165TotalCost
  });

  const fingerprint = await makeFingerprint();
  const failures = checks.filter(item => !item.pass);
  const result = { ok: failures.length === 0, database: dbPath, fingerprint, passed: checks.length - failures.length, failed: failures.length, checks };
  console.log(JSON.stringify(result, null, 2));
  if (failures.length) process.exitCode = 1;
}

main().catch(error => {
  console.error(JSON.stringify({ ok: false, database: dbPath, error: error.message, stack: error.stack }, null, 2));
  process.exitCode = 1;
}).finally(close);
