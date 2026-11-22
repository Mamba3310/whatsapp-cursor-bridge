import 'dotenv/config';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import express from 'express';

const required = [
  'VERIFY_TOKEN',
  'META_APP_SECRET',
  'WHATSAPP_TOKEN',
  'PHONE_NUMBER_ID',
  'CURSOR_API_KEY',
  'CURSOR_REPO_URL',
  'ALLOWED_WA_IDS',
];
for (const name of required) {
  if (!process.env[name]?.trim()) {
    console.error(`Missing required environment variable: ${name}`);
    process.exit(1);
  }
}

const app = express();
const port = Number(process.env.PORT || 3000);
const graphVersion = process.env.META_GRAPH_VERSION || 'v23.0';
const cursorApiUrl = (process.env.CURSOR_API_URL || 'https://api.cursor.com').replace(/\/$/, '');
const repoUrl = process.env.CURSOR_REPO_URL.trim();
const startingRef = process.env.CURSOR_STARTING_REF?.trim();
const allowedSenders = new Set(process.env.ALLOWED_WA_IDS.split(',').map(v => v.trim()).filter(Boolean));
const storePath = path.resolve(process.env.AGENT_STORE_PATH || './data/agents.json');
const agentBySender = new Map();

if (allowedSenders.size === 0) {
  console.error('ALLOWED_WA_IDS must contain at least one WhatsApp sender ID.');
  process.exit(1);
}

try {
  const saved = JSON.parse(await fs.readFile(storePath, 'utf8'));
  for (const [sender, agentId] of Object.entries(saved)) {
    if (typeof agentId === 'string') agentBySender.set(sender, agentId);
  }
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}

app.use(express.json({
  limit: '1mb',
  verify: (req, _res, buf) => { req.rawBody = Buffer.from(buf); },
}));

app.get('/health', (_req, res) => res.status(200).json({ ok: true }));

app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === process.env.VERIFY_TOKEN && challenge) {
    return res.status(200).type('text/plain').send(challenge);
  }
  return res.sendStatus(403);
});

function validMetaSignature(req) {
  const signature = req.get('x-hub-signature-256') || '';
  const match = /^sha256=([a-f0-9]{64})$/i.exec(signature);
  if (!match || !req.rawBody) return false;
  const expected = crypto.createHmac('sha256', process.env.META_APP_SECRET).update(req.rawBody).digest();
  const actual = Buffer.from(match[1], 'hex');
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}

function cursorAuthHeaders() {
  const basic = Buffer.from(`${process.env.CURSOR_API_KEY}:`).toString('base64');
  return { authorization: `Basic ${basic}`, 'content-type': 'application/json' };
}

async function cursorRequest(endpoint, options = {}) {
  const response = await fetch(`${cursorApiUrl}${endpoint}`, {
    ...options,
    headers: { ...cursorAuthHeaders(), ...options.headers },
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Cursor API returned HTTP ${response.status}: ${detail.slice(0, 500)}`);
  }
  return response.status === 204 ? null : response.json();
}

async function saveAgent(sender, agentId) {
  agentBySender.set(sender, agentId);
  await fs.mkdir(path.dirname(storePath), { recursive: true });
  const tempPath = `${storePath}.tmp`;
  await fs.writeFile(tempPath, JSON.stringify(Object.fromEntries(agentBySender), null, 2), { mode: 0o600 });
  await fs.rename(tempPath, storePath);
}

async function createAgent(promptText) {
  const repo = { url: repoUrl };
  if (startingRef) repo.startingRef = startingRef;
  return cursorRequest('/v1/agents', {
    method: 'POST',
    body: JSON.stringify({ prompt: { text: promptText }, repos: [repo] }),
  });
}

async function submitFollowUp(agentId, promptText) {
  return cursorRequest(`/v1/agents/${encodeURIComponent(agentId)}/runs`, {
    method: 'POST',
    body: JSON.stringify({ prompt: { text: promptText } }),
  });
}

async function getRunResult(agentId, runId) {
  const response = await fetch(`${cursorApiUrl}/v1/agents/${encodeURIComponent(agentId)}/runs/${encodeURIComponent(runId)}/stream`, {
    headers: { ...cursorAuthHeaders(), accept: 'text/event-stream' },
    signal: AbortSignal.timeout(20 * 60_000),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Cursor stream returned HTTP ${response.status}: ${detail.slice(0, 500)}`);
  }

  let buffer = '';
  let lastAssistantText = '';
  for await (const chunk of response.body) {
    buffer += Buffer.from(chunk).toString('utf8');
    let boundary;
    while ((boundary = buffer.indexOf('\n\n')) !== -1) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const eventName = frame.split(/\r?\n/).find(line => line.startsWith('event:'))?.slice(6).trim();
      const dataText = frame.split(/\r?\n/).filter(line => line.startsWith('data:')).map(line => line.slice(5).trim()).join('\n');
      if (!dataText) continue;
      let data;
      try { data = JSON.parse(dataText); } catch { continue; }
      if (eventName === 'assistant' && typeof data.text === 'string') lastAssistantText += data.text;
      if (eventName === 'result') {
        if (data.status !== 'FINISHED') throw new Error(`Cursor agent run ended with status ${data.status}`);
        return (typeof data.text === 'string' && data.text.trim() ? data.text : lastAssistantText).trim();
      }
      if (eventName === 'error') throw new Error(`Cursor stream error: ${data.message || data.code || 'unknown error'}`);
    }
  }
  throw new Error('Cursor agent stream closed before a final result arrived.');
}

async function runCursorMessage(sender, body) {
  const text = body.trim();
  if (!text) return 'Send a task or question for your Cursor Cloud Agent.';
  if (text === '/help') return 'Send a task to your Cursor Cloud Agent. Use /new <task> to start a fresh agent, /status to check the saved agent, or /reset to forget its ID.';
  if (text === '/reset') {
    agentBySender.delete(sender);
    await fs.mkdir(path.dirname(storePath), { recursive: true });
    await fs.writeFile(storePath, JSON.stringify(Object.fromEntries(agentBySender), null, 2), { mode: 0o600 });
    return 'Saved agent cleared. Your next task will start a new Cursor Cloud Agent.';
  }
  if (text === '/status') {
    const agentId = agentBySender.get(sender);
    if (!agentId) return 'No Cursor agent is saved yet. Send a task to start one.';
    const agent = await cursorRequest(`/v1/agents/${encodeURIComponent(agentId)}`);
    return `Saved Cursor agent: ${agent.name || agent.id}\n${agent.url || ''}`.trim();
  }

  const isNew = text.startsWith('/new ');
  const promptText = isNew ? text.slice(5).trim() : text;
  if (!promptText) return 'Add a task after /new.';

  const priorAgentId = isNew ? null : agentBySender.get(sender);
  let agentId;
  let runId;
  let agentUrl;
  if (priorAgentId) {
    const result = await submitFollowUp(priorAgentId, promptText);
    agentId = priorAgentId;
    runId = result.run?.id;
  } else {
    const result = await createAgent(promptText);
    agentId = result.agent?.id;
    runId = result.run?.id;
    agentUrl = result.agent?.url;
    if (agentId) await saveAgent(sender, agentId);
  }
  if (!agentId || !runId) throw new Error('Cursor API response did not include the agent and run IDs.');
  const finalText = await getRunResult(agentId, runId);
  return [finalText || 'Cursor finished the run without a text response.', agentUrl].filter(Boolean).join('\n\n');
}

async function sendWhatsAppText(to, text) {
  const response = await fetch(`https://graph.facebook.com/${graphVersion}/${process.env.PHONE_NUMBER_ID}/messages`, {
    method: 'POST',
    headers: { authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`, 'content-type': 'application/json' },
    body: JSON.stringify({ messaging_product: 'whatsapp', recipient_type: 'individual', to, type: 'text', text: { body: text.slice(0, 4096) } }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`WhatsApp API returned HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
}

app.post('/webhook', (req, res) => {
  if (!validMetaSignature(req)) return res.sendStatus(401);
  res.sendStatus(200);
  void processWebhook(req.body).catch(error => console.error('Webhook processing failed:', error.message));
});

async function processWebhook(body) {
  if (body?.object !== 'whatsapp_business_account') return;
  for (const entry of body.entry || []) {
    for (const change of entry.changes || []) {
      if (change.field !== 'messages') continue;
      const value = change.value || {};
      for (const message of value.messages || []) {
        if (message.type !== 'text' || typeof message.text?.body !== 'string') continue;
        const sender = message.from;
        if (!sender || !allowedSenders.has(sender)) {
          console.warn('Ignoring message from unapproved sender:', sender || 'unknown');
          continue;
        }
        try {
          const reply = await runCursorMessage(sender, message.text.body);
          await sendWhatsAppText(sender, reply);
        } catch (error) {
          console.error('Cursor WhatsApp request failed:', error.message);
          await sendWhatsAppText(sender, `Cursor request failed: ${error.message.slice(0, 800)}`).catch(sendError => console.error('Could not send error reply:', sendError.message));
        }
      }
    }
  }
}

app.listen(port, '0.0.0.0', () => console.log(`Independent WhatsApp-to-Cursor bridge listening on port ${port}`));
