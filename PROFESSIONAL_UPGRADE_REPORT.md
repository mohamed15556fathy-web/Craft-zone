# Craft Zone Professional Foundation Patch

تم تطبيق تعديلات تأسيسية آمنة على نسخة المشروع بدون تغيير شكل الصفحات للمستخدم.

## ما تم تعديله

1. إزالة الملفات الحساسة من المشروع
   - حذف `bosta_api_key.txt` من النسخة.
   - حذف `server.js.bak`.
   - إضافة `.gitignore` و `.dockerignore`.
   - إضافة `.env.example` للتشغيل المحلي/Railway بدون أسرار.

2. أمان السيرفر
   - إغلاق `x-powered-by`.
   - إضافة Security Headers أساسية.
   - تقليل حجم JSON/body إلى 3MB.
   - إضافة Rate Limit لتسجيل الدخول والنسخ الاحتياطي والاستيراد.
   - منع رفع ملفات تنفيذية أو سكربتات من upload.
   - تحديد حجم ملفات الرفع والنسخ الاحتياطي.

3. قاعدة البيانات
   - تفعيل SQLite PRAGMA: foreign_keys, WAL, synchronous NORMAL, busy_timeout.
   - إضافة جداول تأسيسية احترافية مستقبلية:
     - `accounting_journal_entries`
     - `accounting_journal_lines`
     - `inventory_ledger`
     - `system_health_checks`
   - إضافة Indexes مهمة للأوردرات والحسابات والمخازن والصنايعية.
   - إضافة Unique Indexes آمنة لمنع تكرار الخصم الآلي قدر الإمكان. لو قاعدة قديمة فيها تكرار، السيرفر لا يتوقف ويسجل تحذير في اللوج.

4. النسخ الاحتياطي
   - روابط تحميل النسخ الاحتياطية لم تعد تحتاج وضع Session Token الحقيقي في الرابط عند إنشاء النسخة من السيرفر.
   - تم إضافة Download Token قصير العمر و One-time للملفات المحمية.

5. فحص احترافي جديد
   - إضافة endpoint جديد: `/professional-health`
   - يفحص مشاكل أساسية مثل:
     - مصروفات مربوطة بأوردر محذوف.
     - تكرار تكاليف آلية.
     - تكرار خصم شنط جاهزة.
     - تكرار خصم يد.
   - يسجل نتيجة الفحص في `system_health_checks`.

## مهم قبل التشغيل

على Railway أو الجهاز المحلي، ضع مفتاح Bosta في متغير بيئة:

```bash
BOSTA_API_KEY=your_real_key
```

لا تضع المفتاح داخل ملف داخل المشروع.

## أوامر التشغيل

```bash
npm install
npm run check
npm start
```

## ملاحظات

هذه المرحلة تعتبر Professional Foundation وليست إعادة بناء كاملة.
المرحلة التالية المقترحة هي نقل الحسابات فعليًا إلى Ledger موحد، ونقل المخازن إلى Stock Ledger إجباري، وفصل `server.js` إلى Routes/Services تدريجيًا.
