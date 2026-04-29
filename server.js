import Fastify from 'fastify';
import crypto from 'node:crypto';

const {
  META_APP_SECRET,
  META_VERIFY_TOKEN,
  META_PAGE_TOKEN,
  DATACRAZY_WEBHOOK_URL,
  GRAPH_API_VERSION = 'v21.0',
  PORT = 3000,
  LOG_LEVEL = 'info',
} = process.env;

for (const [k, v] of Object.entries({
  META_APP_SECRET,
  META_VERIFY_TOKEN,
  META_PAGE_TOKEN,
  DATACRAZY_WEBHOOK_URL,
})) {
  if (!v) {
    console.error(`Missing required env var: ${k}`);
    process.exit(1);
  }
}

const app = Fastify({
  logger: { level: LOG_LEVEL },
  bodyLimit: 1024 * 1024,
});

app.removeContentTypeParser('application/json');
app.addContentTypeParser(
  'application/json',
  { parseAs: 'buffer' },
  (req, body, done) => {
    req.rawBody = body;
    try {
      done(null, body.length ? JSON.parse(body.toString('utf8')) : {});
    } catch (err) {
      err.statusCode = 400;
      done(err, undefined);
    }
  },
);

const seenLeadIds = new Map();
const SEEN_TTL_MS = 60 * 60 * 1000;

function alreadyProcessed(leadgenId) {
  const now = Date.now();
  for (const [id, ts] of seenLeadIds) {
    if (now - ts > SEEN_TTL_MS) seenLeadIds.delete(id);
  }
  if (seenLeadIds.has(leadgenId)) return true;
  seenLeadIds.set(leadgenId, now);
  return false;
}

function verifySignature(rawBody, signatureHeader) {
  if (!signatureHeader || !rawBody) return false;
  const expected =
    'sha256=' +
    crypto.createHmac('sha256', META_APP_SECRET).update(rawBody).digest('hex');
  const a = Buffer.from(signatureHeader);
  const b = Buffer.from(expected);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

async function fetchLead(leadgenId) {
  const url = `https://graph.facebook.com/${GRAPH_API_VERSION}/${leadgenId}?access_token=${META_PAGE_TOKEN}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph API ${res.status}: ${text}`);
  }
  return res.json();
}

function flattenFields(fieldData = []) {
  const out = {};
  for (const f of fieldData) {
    out[f.name] = Array.isArray(f.values) ? f.values.join(', ') : '';
  }
  return out;
}

async function sendToDataCrazy(payload, logger) {
  const res = await fetch(DATACRAZY_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`DataCrazy ${res.status}: ${text}`);
  }
  logger.info({ status: res.status, response: text.slice(0, 200) }, 'forwarded to datacrazy');
}

async function processLeadgen(change, logger) {
  const { leadgen_id, page_id, form_id, created_time, ad_id, adgroup_id, campaign_id } =
    change.value || {};

  if (!leadgen_id) {
    logger.warn({ change }, 'change without leadgen_id');
    return;
  }

  if (alreadyProcessed(leadgen_id)) {
    logger.info({ leadgen_id }, 'duplicate skipped');
    return;
  }

  const lead = await fetchLead(leadgen_id);
  const fields = flattenFields(lead.field_data);

  const payload = {
    leadgen_id: lead.id,
    created_time: lead.created_time || created_time,
    page_id,
    form_id,
    ad_id,
    adgroup_id,
    campaign_id,
    name: fields.full_name || fields.name || '',
    email: fields.email || '',
    phone: fields.phone_number || fields.phone || '',
    fields,
    raw_field_data: lead.field_data,
  };

  logger.info(
    { leadgen_id, form_id, name: payload.name, phone: payload.phone },
    'received lead',
  );

  await sendToDataCrazy(payload, logger);
}

app.get('/health', async () => ({ ok: true, ts: new Date().toISOString() }));

app.get('/webhook/leads', async (req, reply) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === META_VERIFY_TOKEN) {
    req.log.info('webhook verified');
    return reply.code(200).type('text/plain').send(challenge);
  }
  req.log.warn({ mode, tokenProvided: !!token }, 'verification failed');
  return reply.code(403).send('forbidden');
});

app.post('/webhook/leads', async (req, reply) => {
  const signature = req.headers['x-hub-signature-256'];
  if (!verifySignature(req.rawBody, signature)) {
    req.log.warn('invalid signature');
    return reply.code(401).send({ error: 'invalid signature' });
  }

  reply.code(200).send({ received: true });

  const body = req.body;
  const entries = body?.entry || [];

  setImmediate(async () => {
    for (const entry of entries) {
      for (const change of entry.changes || []) {
        if (change.field !== 'leadgen') continue;
        try {
          await processLeadgen(change, req.log);
        } catch (err) {
          req.log.error({ err: err.message, change }, 'lead processing failed');
        }
      }
    }
  });
});

app.listen({ port: Number(PORT), host: '0.0.0.0' }).then(() => {
  app.log.info(`meta-leads-webhook listening on :${PORT}`);
});
