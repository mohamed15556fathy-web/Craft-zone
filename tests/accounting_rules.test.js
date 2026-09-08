'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const rules = require('../lib/accounting_rules');

test('شراء الورق بالفرخ يحتفظ بالوحدة ويحسب الوزن والسعر المكافئ', () => {
  const result = rules.convertPaperPurchase({
    unit: 'فرخ',
    quantity: 6000,
    unitPrice: 4.5,
    lengthCm: 84,
    widthCm: 100,
    grammageGsm: 100
  });
  assert.equal(result.unit, 'فرخ');
  assert.equal(result.inputQuantity, 6000);
  assert.equal(result.inputUnitPrice, 4.5);
  assert.equal(result.weightPerSheetKg, 0.084);
  assert.equal(result.sheets, 6000);
  assert.equal(result.kg, 504);
  assert.equal(result.pricePerSheet, 4.5);
  assert.equal(result.pricePerKg, 53.571429);
  assert.equal(result.totalPrice, 27000);
});

test('شراء الورق بالكيلو لا يعامل عدد الكيلوجرامات كعدد أفراخ', () => {
  const result = rules.convertPaperPurchase({
    unit: 'كجم', quantity: 504, unitPrice: 53.571428,
    lengthCm: 84, widthCm: 100, grammageGsm: 100
  });
  assert.equal(result.unit, 'كجم');
  assert.equal(result.kg, 504);
  assert.equal(result.sheets, 6000);
  assert.equal(result.pricePerSheet, 4.5);
});

test('التحصيل الزائد يرفض بدل تسجيل overpayment صامت', () => {
  assert.throws(
    () => rules.assertNoOverpayment(690, 700, 'الأوردر'),
    error => error.code === 'OVERPAYMENT_REVIEW_REQUIRED' && error.statusCode === 409
  );
});

test('المرتجع يصفر المتبقي ويحافظ على تاريخ السعر والمدفوع', () => {
  assert.deepEqual(rules.paymentSummary(1270, 500, 'مرتجع'), {
    total: 1270,
    paid: 500,
    remaining: 0,
    paymentType: 'مرتجع - مبلغ محصل يحتاج تسوية'
  });
});

test('Sales History يخصم الشحن والرسوم الفعلية ويحسب الربح', () => {
  assert.deepEqual(rules.salesAmounts({
    grossSale: 2250,
    shippingCost: 100,
    insuranceFee: 20,
    extraCodFee: 10,
    otherShippingFee: 5,
    totalCost: 1000
  }), {
    grossSale: 2250,
    totalDeductions: 135,
    totalSale: 2115,
    totalCost: 1000,
    netProfit: 1115
  });
});

test('مزامنة المتجر تعيد المحاولة فقط للأخطاء المؤقتة', () => {
  assert.equal(rules.isRetryableStoreFailure(502), true);
  assert.equal(rules.isRetryableStoreFailure(0, 'ABORT_ERR'), true);
  assert.equal(rules.isRetryableStoreFailure(400), false);
  assert.equal(rules.isRetryableStoreFailure(404, 'ORDER_NOT_FOUND'), false);
});
