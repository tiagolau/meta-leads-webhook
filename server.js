import Fastify from 'fastify';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const {
  META_APP_SECRET,
  META_VERIFY_TOKEN,
  META_PAGE_TOKEN,
  DATACRAZY_WEBHOOK_URL,
  GRAPH_API_VERSION = 'v21.0',
  PORT = 3000,
  LOG_LEVEL = 'info',
  POLLING_ENABLED = 'true',
  POLLING_INTERVAL_MS = '120000',
  POLLING_PAGE_ID = '556406854837637',
  STATE_DIR = '/app/state',
  META_DATASETS = '{}',
  META_STAGE_MAP = '{}',
  CAPI_INBOUND_SECRET,
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

let datasetsByPage;
try {
  datasetsByPage = JSON.parse(META_DATASETS);
} catch (err) {
  console.error('META_DATASETS is not valid JSON:', err.message);
  process.exit(1);
}

const DEFAULT_STAGE_MAP = {
  lead: 'Lead',
  novo: 'Lead',
  contato: 'Contact',
  contatado: 'Contact',
  agendado: 'Schedule',
  qualificado: 'Qualified Lead',
  inscrito: 'SubmitApplication',
  matriculado: 'Converted Lead',
  convertido: 'Converted Lead',
};

let customStageMap;
try {
  customStageMap = JSON.parse(META_STAGE_MAP);
} catch (err) {
  console.error('META_STAGE_MAP is not valid JSON:', err.message);
  process.exit(1);
}
const stageMap = { ...DEFAULT_STAGE_MAP, ...customStageMap };

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

const LEAD_FIELDS = [
  'id',
  'created_time',
  'field_data',
  'ad_id',
  'ad_name',
  'adset_id',
  'adset_name',
  'campaign_id',
  'campaign_name',
  'form_id',
  'is_organic',
  'partner_name',
  'platform',
  'custom_disclaimer_responses',
].join(',');

async function fetchLead(leadgenId) {
  const url = `https://graph.facebook.com/${GRAPH_API_VERSION}/${leadgenId}?fields=${LEAD_FIELDS}&access_token=${META_PAGE_TOKEN}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph API ${res.status}: ${text}`);
  }
  return res.json();
}

const formNameCache = new Map();
const FORM_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

async function fetchFormName(formId) {
  if (!formId) return null;
  const cached = formNameCache.get(formId);
  if (cached && Date.now() - cached.ts < FORM_CACHE_TTL_MS) return cached.name;
  try {
    const url = `https://graph.facebook.com/${GRAPH_API_VERSION}/${formId}?fields=name&access_token=${META_PAGE_TOKEN}`;
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json();
    formNameCache.set(formId, { name: data.name, ts: Date.now() });
    return data.name;
  } catch {
    return null;
  }
}

async function listActiveForms() {
  const url = `https://graph.facebook.com/${GRAPH_API_VERSION}/${POLLING_PAGE_ID}/leadgen_forms?fields=id,name,status&limit=200&access_token=${META_PAGE_TOKEN}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`leadgen_forms ${res.status}: ${text}`);
  }
  const data = await res.json();
  return (data.data || []).filter((f) => f.status === 'ACTIVE');
}

async function fetchLeadsSince(formId, sinceTimestamp) {
  const url = `https://graph.facebook.com/${GRAPH_API_VERSION}/${formId}/leads?fields=${LEAD_FIELDS}&limit=50&access_token=${META_PAGE_TOKEN}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`leads(${formId}) ${res.status}: ${text}`);
  }
  const data = await res.json();
  const leads = data.data || [];
  return leads.filter((l) => {
    const ts = Math.floor(new Date(l.created_time).getTime() / 1000);
    return ts > sinceTimestamp;
  });
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

async function buildPayload(lead, source, fallback = {}) {
  const fields = flattenFields(lead.field_data);
  const formId = lead.form_id || fallback.form_id;
  const formName = await fetchFormName(formId);

  return {
    leadgen_id: lead.id,
    created_time: lead.created_time || fallback.created_time,
    page_id: fallback.page_id || POLLING_PAGE_ID,
    form_id: formId,
    form_name: formName,
    ad_id: lead.ad_id || fallback.ad_id || null,
    ad_name: lead.ad_name || null,
    adset_id: lead.adset_id || fallback.adgroup_id || null,
    adset_name: lead.adset_name || null,
    campaign_id: lead.campaign_id || fallback.campaign_id || null,
    campaign_name: lead.campaign_name || null,
    platform: lead.platform || null,
    is_organic: lead.is_organic ?? null,
    partner_name: lead.partner_name || null,
    name: fields.full_name || fields.name || '',
    email: fields.email || '',
    phone: fields.phone_number || fields.phone || '',
    fields,
    raw_field_data: lead.field_data,
    _source: source,
  };
}

async function processLead(lead, source, logger, fallback = {}) {
  if (alreadyProcessed(lead.id)) {
    logger.info({ leadgen_id: lead.id, source }, 'duplicate skipped');
    return false;
  }

  const payload = await buildPayload(lead, source, fallback);
  await rememberLeadPage(lead.id, payload.page_id);

  logger.info(
    {
      leadgen_id: lead.id,
      form_id: payload.form_id,
      form_name: payload.form_name,
      campaign_name: payload.campaign_name,
      ad_name: payload.ad_name,
      name: payload.name,
      phone: payload.phone,
      source,
    },
    'received lead',
  );

  await sendToDataCrazy(payload, logger);
  return true;
}

async function processLeadgen(change, logger) {
  const { leadgen_id } = change.value || {};
  if (!leadgen_id) {
    logger.warn({ change }, 'change without leadgen_id');
    return;
  }
  if (alreadyProcessed(leadgen_id)) {
    logger.info({ leadgen_id, source: 'webhook' }, 'duplicate skipped');
    return;
  }
  seenLeadIds.delete(leadgen_id);
  const lead = await fetchLead(leadgen_id);
  await processLead(lead, 'webhook', logger, change.value);
}

const checkpointPath = path.join(STATE_DIR, 'checkpoint.json');
const leadPagesPath = path.join(STATE_DIR, 'lead-pages.json');

async function loadCheckpoint() {
  try {
    const data = await fs.readFile(checkpointPath, 'utf8');
    return JSON.parse(data);
  } catch {
    return {};
  }
}

async function saveCheckpoint(state) {
  await fs.mkdir(STATE_DIR, { recursive: true });
  await fs.writeFile(checkpointPath, JSON.stringify(state, null, 2));
}

const leadPages = new Map();
let leadPagesLoaded = false;

async function loadLeadPages() {
  try {
    const data = await fs.readFile(leadPagesPath, 'utf8');
    const obj = JSON.parse(data);
    for (const [k, v] of Object.entries(obj)) leadPages.set(k, v);
  } catch {}
  leadPagesLoaded = true;
}

async function persistLeadPages() {
  await fs.mkdir(STATE_DIR, { recursive: true });
  const obj = Object.fromEntries(leadPages);
  await fs.writeFile(leadPagesPath, JSON.stringify(obj));
}

async function rememberLeadPage(leadgenId, pageId) {
  if (!leadgenId || !pageId) return;
  if (leadPages.get(leadgenId) === pageId) return;
  leadPages.set(leadgenId, pageId);
  try {
    await persistLeadPages();
  } catch (err) {
    app.log.error({ err: err.message }, 'failed to persist lead-pages');
  }
}

let pollingInFlight = false;

async function pollOnce() {
  if (pollingInFlight) {
    app.log.info('previous poll still in flight, skipping');
    return;
  }
  pollingInFlight = true;
  try {
    const state = await loadCheckpoint();
    const forms = await listActiveForms();
    const nowSec = Math.floor(Date.now() / 1000);
    let totalNew = 0;

    for (const form of forms) {
      const since = state[form.id] || nowSec;
      let leads;
      try {
        leads = await fetchLeadsSince(form.id, since);
      } catch (err) {
        app.log.error({ form_id: form.id, err: err.message }, 'poll fetch error');
        continue;
      }
      if (!leads.length) continue;

      leads.sort(
        (a, b) => new Date(a.created_time).getTime() - new Date(b.created_time).getTime(),
      );

      for (const lead of leads) {
        try {
          const sent = await processLead(lead, 'poll', app.log);
          if (sent) totalNew++;
        } catch (err) {
          app.log.error(
            { leadgen_id: lead.id, err: err.message },
            'poll process error',
          );
          continue;
        }
        const ts = Math.floor(new Date(lead.created_time).getTime() / 1000);
        state[form.id] = Math.max(state[form.id] || 0, ts);
        await saveCheckpoint(state);
      }
    }
    if (totalNew > 0) app.log.info({ totalNew }, 'poll cycle complete');
  } finally {
    pollingInFlight = false;
  }
}

async function startPolling() {
  if (POLLING_ENABLED !== 'true') {
    app.log.info('polling disabled');
    return;
  }
  const state = await loadCheckpoint();
  if (Object.keys(state).length === 0) {
    const nowSec = Math.floor(Date.now() / 1000);
    try {
      const forms = await listActiveForms();
      const initial = {};
      for (const form of forms) initial[form.id] = nowSec;
      await saveCheckpoint(initial);
      app.log.info({ forms: forms.length, cutoff: nowSec }, 'initialized checkpoint');
    } catch (err) {
      app.log.error({ err: err.message }, 'failed to initialize checkpoint');
    }
  }
  app.log.info({ interval_ms: Number(POLLING_INTERVAL_MS) }, 'polling enabled');
  const tick = async () => {
    try {
      await pollOnce();
    } catch (err) {
      app.log.error({ err: err.message }, 'poll tick failed');
    } finally {
      setTimeout(tick, Number(POLLING_INTERVAL_MS));
    }
  };
  setTimeout(tick, 5000);
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

function normalizeStage(stage) {
  return String(stage || '').trim().toLowerCase();
}

function resolveEventName(stage) {
  const key = normalizeStage(stage);
  if (!key) return null;
  if (Object.prototype.hasOwnProperty.call(stageMap, key)) return stageMap[key];
  return null;
}

async function sendCapiLeadEvent({ leadgenId, eventName, eventTimeSec, datasetCfg, logger }) {
  const url = `https://graph.facebook.com/${GRAPH_API_VERSION}/${datasetCfg.dataset_id}/events?access_token=${datasetCfg.access_token}`;
  const body = {
    data: [
      {
        event_name: eventName,
        event_time: eventTimeSec,
        event_id: `${leadgenId}-${eventName}-${eventTimeSec}`,
        action_source: 'system_generated',
        user_data: {
          lead_id: Number(leadgenId),
        },
        custom_data: {
          event_source: 'crm',
          lead_event_source: datasetCfg.crm_name || 'DataCrazy',
        },
      },
    ],
  };
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {}
  if (!res.ok) {
    logger.error(
      { status: res.status, body: text.slice(0, 400), leadgen_id: leadgenId, event_name: eventName },
      'CAPI lead event failed',
    );
    const err = new Error(`Meta ${res.status}`);
    err.statusCode = res.status;
    err.meta = parsed;
    throw err;
  }
  logger.info(
    {
      leadgen_id: leadgenId,
      event_name: eventName,
      events_received: parsed?.events_received,
      fbtrace_id: parsed?.fbtrace_id,
    },
    'CAPI lead event sent',
  );
  return parsed;
}

app.post('/capi/lead-event', async (req, reply) => {
  if (!CAPI_INBOUND_SECRET) {
    return reply.code(503).send({ error: 'capi inbound disabled' });
  }
  const auth = req.headers.authorization || '';
  const expected = `Bearer ${CAPI_INBOUND_SECRET}`;
  if (auth.length !== expected.length || !crypto.timingSafeEqual(Buffer.from(auth), Buffer.from(expected))) {
    return reply.code(401).send({ error: 'unauthorized' });
  }

  const { leadgen_id, stage, occurred_at, page_id: pageIdHint } = req.body || {};
  if (!leadgen_id || !stage) {
    return reply.code(400).send({ error: 'leadgen_id and stage are required' });
  }

  const eventName = resolveEventName(stage);
  if (!eventName) {
    req.log.info({ leadgen_id, stage }, 'stage not mapped, skipping');
    return reply.code(202).send({ ok: true, skipped: 'stage_unmapped', stage });
  }

  if (!leadPagesLoaded) await loadLeadPages();
  const pageId = pageIdHint || leadPages.get(String(leadgen_id));
  if (!pageId) {
    return reply.code(404).send({ error: 'page mapping not found for leadgen_id' });
  }

  const datasetCfg = datasetsByPage[pageId];
  if (!datasetCfg?.dataset_id || !datasetCfg?.access_token) {
    return reply.code(412).send({ error: 'no dataset configured for page', page_id: pageId });
  }

  const eventTimeSec = occurred_at
    ? Math.floor(new Date(occurred_at).getTime() / 1000)
    : Math.floor(Date.now() / 1000);
  if (!Number.isFinite(eventTimeSec) || eventTimeSec <= 0) {
    return reply.code(400).send({ error: 'invalid occurred_at' });
  }

  try {
    const result = await sendCapiLeadEvent({
      leadgenId: leadgen_id,
      eventName,
      eventTimeSec,
      datasetCfg,
      logger: req.log,
    });
    return reply.code(200).send({
      ok: true,
      event_name: eventName,
      dataset_id: datasetCfg.dataset_id,
      page_id: pageId,
      events_received: result?.events_received ?? null,
      fbtrace_id: result?.fbtrace_id ?? null,
    });
  } catch (err) {
    return reply.code(502).send({
      error: 'meta capi failed',
      status: err.statusCode || null,
      details: err.meta || null,
    });
  }
});

app.listen({ port: Number(PORT), host: '0.0.0.0' }).then(async () => {
  app.log.info(`meta-leads-webhook listening on :${PORT}`);
  await loadLeadPages();
  app.log.info(
    {
      lead_pages: leadPages.size,
      datasets: Object.keys(datasetsByPage).length,
      capi_enabled: !!CAPI_INBOUND_SECRET && Object.keys(datasetsByPage).length > 0,
    },
    'state ready',
  );
  startPolling();
});
