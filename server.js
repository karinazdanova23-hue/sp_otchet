// Расписание студии — сервер
// Отдаёт статический фронтенд из /public и реализует простое хранилище "ключ → значение"
// поверх Timeweb S3 (совместимо с API Amazon S3), которое повторяет интерфейс window.storage,
// уже использующийся во всём фронтенде (get/set/delete/list, с флагом shared).

const path = require('path');
const crypto = require('crypto');
const express = require('express');
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} = require('@aws-sdk/client-s3');

const PORT = process.env.PORT || 3000;
const BUCKET = process.env.S3_BUCKET;
const REGION = process.env.S3_REGION || 'ru-1';
const ENDPOINT = process.env.S3_ENDPOINT; // например https://s3.timeweb.cloud

if (!BUCKET || !ENDPOINT) {
  console.warn('[storage] ВНИМАНИЕ: не заданы S3_BUCKET / S3_ENDPOINT — хранилище работать не будет, пока их не задать в переменных окружения.');
}

const s3 = new S3Client({
  region: REGION,
  endpoint: ENDPOINT,
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
  },
});

const app = express();
app.use(express.json({ limit: '10mb' }));

function safeKeyPart(s) {
  // защита от "../" и подобного в ключах — оставляем только безопасные символы
  return String(s).replace(/[^a-zA-Z0-9_\-.]/g, '_');
}

function objectPath(key, shared, deviceId) {
  const safeKey = safeKeyPart(key);
  if (shared) return `shared/${safeKey}.json`;
  const safeDevice = safeKeyPart(deviceId || 'unknown-device');
  return `personal/${safeDevice}/${safeKey}.json`;
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf-8');
}

// GET /api/storage/:key?shared=true|false
app.get('/api/storage/:key', async (req, res) => {
  try {
    const shared = req.query.shared === 'true';
    const deviceId = req.header('x-device-id');
    const key = objectPath(req.params.key, shared, deviceId);
    const result = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    const body = await streamToString(result.Body);
    res.json({ key: req.params.key, value: body, shared });
  } catch (err) {
    if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
      return res.status(404).json(null);
    }
    console.error('GET /api/storage error:', err);
    res.status(500).json({ error: 'storage_get_failed' });
  }
});

// POST /api/storage/:key   body: { value, shared }
app.post('/api/storage/:key', async (req, res) => {
  try {
    const { value, shared } = req.body || {};
    if (value === undefined) return res.status(400).json({ error: 'value_required' });
    const deviceId = req.header('x-device-id');
    const key = objectPath(req.params.key, !!shared, deviceId);
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: String(value),
      ContentType: 'application/json; charset=utf-8',
    }));
    res.json({ key: req.params.key, value, shared: !!shared });
  } catch (err) {
    console.error('POST /api/storage error:', err);
    res.status(500).json({ error: 'storage_set_failed' });
  }
});

// DELETE /api/storage/:key?shared=true|false
app.delete('/api/storage/:key', async (req, res) => {
  try {
    const shared = req.query.shared === 'true';
    const deviceId = req.header('x-device-id');
    const key = objectPath(req.params.key, shared, deviceId);
    await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }));
    res.json({ key: req.params.key, deleted: true, shared });
  } catch (err) {
    console.error('DELETE /api/storage error:', err);
    res.status(500).json({ error: 'storage_delete_failed' });
  }
});

// GET /api/storage?prefix=X&shared=true|false
app.get('/api/storage', async (req, res) => {
  try {
    const shared = req.query.shared === 'true';
    const deviceId = req.header('x-device-id');
    const prefixKeyPart = req.query.prefix ? safeKeyPart(req.query.prefix) : '';
    const basePrefix = shared ? 'shared/' : `personal/${safeKeyPart(deviceId || 'unknown-device')}/`;
    const fullPrefix = basePrefix + prefixKeyPart;
    const result = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: fullPrefix }));
    const keys = (result.Contents || []).map((obj) => {
      const withoutBase = obj.Key.slice(basePrefix.length);
      return withoutBase.replace(/\.json$/, '');
    });
    res.json({ keys, prefix: req.query.prefix, shared });
  } catch (err) {
    console.error('LIST /api/storage error:', err);
    res.status(500).json({ error: 'storage_list_failed' });
  }
});

// проверка живости — полезно для Timeweb App Platform
app.get('/health', (req, res) => res.json({ ok: true }));

// статический фронтенд
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Сервер запущен на порту ${PORT}`);
});
