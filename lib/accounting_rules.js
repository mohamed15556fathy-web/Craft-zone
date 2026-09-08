'use strict';

const EPSILON = 0.01;
const PAPER_UNITS = Object.freeze({ KG: 'كجم', SHEET: 'فرخ' });

function finiteNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function roundMoney(value) {
  return Math.round((finiteNumber(value) + Number.EPSILON) * 100) / 100;
}

function roundQuantity(value, precision = 6) {
  const factor = 10 ** precision;
  return Math.round((finiteNumber(value) + Number.EPSILON) * factor) / factor;
}

function normalizePaperPurchaseUnit(value) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (['كجم', 'كيلو', 'كيلوجرام', 'kg', 'kilogram', 'kilograms'].includes(raw)) return PAPER_UNITS.KG;
  if (['فرخ', 'فرخة', 'افرخ', 'أفرخ', 'شيت', 'sheet', 'sheets'].includes(raw)) return PAPER_UNITS.SHEET;
  throw new Error(`وحدة شراء الورق غير مدعومة: ${String(value ?? '').trim() || 'فارغة'}`);
}

function sheetWeightKg(lengthCm, widthCm, grammageGsm) {
  const length = finiteNumber(lengthCm);
  const width = finiteNumber(widthCm);
  const grammage = finiteNumber(grammageGsm);
  if (length <= 0 || width <= 0 || grammage <= 0) return 0;
  return roundQuantity((length * width / 10000) * (grammage / 1000), 9);
}

function convertPaperPurchase({ unit, quantity, unitPrice, lengthCm, widthCm, grammageGsm }) {
  const normalizedUnit = normalizePaperPurchaseUnit(unit);
  const inputQuantity = finiteNumber(quantity);
  const inputUnitPrice = finiteNumber(unitPrice);
  if (inputQuantity <= 0) throw new Error('كمية شراء الورق يجب أن تكون أكبر من صفر');
  if (inputUnitPrice < 0) throw new Error('سعر وحدة الورق لا يمكن أن يكون سالبًا');

  const weightPerSheetKg = sheetWeightKg(lengthCm, widthCm, grammageGsm);
  if (weightPerSheetKg <= 0) throw new Error('يجب تحديد طول وعرض وجرام الورق لحساب التحويل بين الفرخ والكيلو');

  const sheets = normalizedUnit === PAPER_UNITS.SHEET
    ? inputQuantity
    : inputQuantity / weightPerSheetKg;
  const kg = normalizedUnit === PAPER_UNITS.KG
    ? inputQuantity
    : inputQuantity * weightPerSheetKg;
  const pricePerSheet = normalizedUnit === PAPER_UNITS.SHEET
    ? inputUnitPrice
    : inputUnitPrice * weightPerSheetKg;
  const pricePerKg = normalizedUnit === PAPER_UNITS.KG
    ? inputUnitPrice
    : inputUnitPrice / weightPerSheetKg;

  return {
    unit: normalizedUnit,
    inputQuantity: roundQuantity(inputQuantity, 6),
    inputUnitPrice: roundQuantity(inputUnitPrice, 6),
    weightPerSheetKg,
    sheets: roundQuantity(sheets, 6),
    kg: roundQuantity(kg, 6),
    pricePerSheet: roundQuantity(pricePerSheet, 6),
    pricePerKg: roundQuantity(pricePerKg, 6),
    totalPrice: roundMoney(inputQuantity * inputUnitPrice),
  };
}

function assertNoOverpayment(total, paid, context = 'العملية') {
  const cleanTotal = roundMoney(Math.max(0, finiteNumber(total)));
  const cleanPaid = roundMoney(Math.max(0, finiteNumber(paid)));
  if (cleanPaid > cleanTotal + EPSILON) {
    const error = new Error(`المدفوع (${cleanPaid.toFixed(2)}) أكبر من إجمالي ${context} (${cleanTotal.toFixed(2)}). يجب المراجعة بدون إنشاء overpayment صامت.`);
    error.code = 'OVERPAYMENT_REVIEW_REQUIRED';
    error.statusCode = 409;
    error.details = { total: cleanTotal, paid: cleanPaid, context };
    throw error;
  }
  return { total: cleanTotal, paid: cleanPaid };
}

function paymentSummary(total, paid, status = '') {
  const validated = assertNoOverpayment(total, paid, 'الأوردر');
  const returned = String(status || '').trim() === 'مرتجع';
  const remaining = returned ? 0 : roundMoney(validated.total - validated.paid);
  const paymentType = returned
    ? (validated.paid > 0 ? 'مرتجع - مبلغ محصل يحتاج تسوية' : 'مرتجع')
    : (remaining <= EPSILON && validated.total > 0 ? 'مدفوع كامل' : (validated.paid > 0 ? 'عربون' : (validated.total > 0 ? 'آجل' : 'لم يتم الدفع')));
  return { ...validated, remaining, paymentType };
}

function salesAmounts({ grossSale, shippingCost, insuranceFee, extraCodFee, otherShippingFee, totalCost }) {
  const gross = roundMoney(finiteNumber(grossSale));
  const deductions = roundMoney(
    Math.max(0, finiteNumber(shippingCost)) +
    Math.max(0, finiteNumber(insuranceFee)) +
    Math.max(0, finiteNumber(extraCodFee)) +
    Math.max(0, finiteNumber(otherShippingFee))
  );
  const sale = roundMoney(gross - deductions);
  const cost = roundMoney(Math.max(0, finiteNumber(totalCost)));
  return {
    grossSale: gross,
    totalDeductions: deductions,
    totalSale: sale,
    totalCost: cost,
    netProfit: roundMoney(sale - cost),
  };
}

function isRetryableStoreFailure(status, errorCode = '') {
  const code = String(errorCode || '').trim().toUpperCase();
  return [408, 425, 429, 500, 502, 503, 504].includes(Number(status)) ||
    ['ABORT_ERR', 'ABORTERROR', 'ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN', 'FETCH_FAILED'].includes(code);
}

module.exports = {
  EPSILON,
  PAPER_UNITS,
  finiteNumber,
  roundMoney,
  roundQuantity,
  normalizePaperPurchaseUnit,
  sheetWeightKg,
  convertPaperPurchase,
  assertNoOverpayment,
  paymentSummary,
  salesAmounts,
  isRetryableStoreFailure,
};
