# تجهيز المشروع لـ Railway و GitHub

## التخزين الثابت على Railway
- المشروع متعدل علشان يتعرف تلقائيًا على Railway ويستخدم Volume mounted على المسار:

```txt
/data
```

- قاعدة البيانات هتبقى افتراضيًا هنا:

```txt
/data/database.db
```

- الملفات المرفوعة هتبقى هنا:

```txt
/data/uploads
```

- النسخ الاحتياطية هتبقى هنا:

```txt
/data/backups
```

## قبل الرفع على GitHub
- GitHub للكود فقط، مش للداتا.
- لا ترفع ملفات `.db` أو فولدرات `data/uploads/backups` على GitHub.
- ملف `.gitignore` مجهز لمنع رفع ملفات البيانات.

## خطوات Railway
1. ارفع المشروع على GitHub.
2. اربط Railway بالـ GitHub Repo.
3. اعمل Volume واربطه على Service بتاعة النظام.
4. خلي Mount Path للـ Volume:

```txt
/data
```

5. اعمل Deploy.

## هل لازم أضيف Environment Variable؟
لا، طالما الـ Volume عندك على `/data`، السيستم هيستخدمه تلقائيًا على Railway.

اختياريًا فقط، لو حبيت تغير مكان التخزين، ممكن تضيف واحد من دول في Variables:

```txt
APP_DATA_DIR=/data
```

أو:

```txt
DATABASE_PATH=/data/database.db
```

## استرجاع الداتا من الباكب
بعد أول Deploy بالنسخة دي:
- افتح السيستم.
- من لوحة النسخ الاحتياطي/الاستيراد ارفع ملف الباكب `.db` أو `.zip`.
- بعد الاستيراد، الداتا هتتحفظ داخل Railway Volume ومش هتتمسح مع أي Deploy جديد.

## ملاحظات مهمة
- Railway Volume مربوط بسيرفيس واحدة، وده مناسب لاستخدام SQLite.
- لو شغلت أكتر من Replica لنفس السيرفيس، SQLite مع Volume مش مناسب. خليه Replica واحدة.
- صفحة `/storage-config` للـ admin بتعرض مسار التخزين الحالي للتأكد إن السيستم شغال على `/data`.
