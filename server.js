const express = require('express');
const { exec, spawn, execSync } = require('child_process');
const fs   = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

let protobuf = null;
try { protobuf = require('protobufjs'); } catch { /* optional — install with: npm install protobufjs */ }

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const CONFIG_FILE = path.join(__dirname, 'config.json');
const KAFKA_CONFIG_FILE = path.join(__dirname, 'kafka-config.json');

const { Kafka, logLevel } = require('kafkajs');

// ─── Config ───────────────────────────────────────────────────────────────────

function loadConfig() {
  return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
}

function getEnvAndTopic(envId, topicId) {
  const config = loadConfig();
  const env   = config.environments.find(e => e.id === envId);
  if (!env) return null;
  const topic = env.topics.find(t => t.id === topicId);
  if (!topic) return null;
  return { env, topic };
}

function loadKafkaConfigRaw() {
  return JSON.parse(fs.readFileSync(KAFKA_CONFIG_FILE, 'utf8'));
}

function kafkaEnvCredentialReady(env) {
  if (!env.sasl) return true;
  const u = process.env[env.sasl.usernameEnvVar];
  const p = process.env[env.sasl.passwordEnvVar];
  return !!(String(u || '').trim() && String(p || '').trim());
}

function kafkaSaslOptions(env) {
  if (!env.sasl || !kafkaEnvCredentialReady(env)) return undefined;
  const mech = String(env.sasl.mechanism || 'plain').toLowerCase();
  const username = process.env[env.sasl.usernameEnvVar];
  const password = process.env[env.sasl.passwordEnvVar];
  if (mech === 'plain') {
    return { mechanism: 'plain', username, password };
  }
  return undefined;
}

function getKafkaEnvTopic(envId, topicId) {
  const config = loadKafkaConfigRaw();
  const env   = config.environments.find(e => e.id === envId);
  if (!env) return null;
  const topic = env.topics.find(t => t.id === topicId);
  if (!topic) return null;
  return { env, topic };
}

/** API shape: clone env without exposing SASL secret values */
function getKafkaConfigForApi() {
  const { environments } = loadKafkaConfigRaw();
  return environments.map((env) => ({
    id: env.id,
    name: env.name,
    bootstrap: env.bootstrap,
    ssl: !!env.ssl,
    requiresCredentials: !!env.sasl,
    credentialEnvHint: env.sasl
      ? `${env.sasl.usernameEnvVar} + ${env.sasl.passwordEnvVar}`
      : null,
    topics: env.topics.map((t) => ({
      id: t.id,
      label: t.label,
      topic: t.topic,
      pulsarMirror: t.pulsarMirror || null,
      schema: t.schema || null,
      valueFormat: t.valueFormat || null,
      description: t.description || null,
      credentialReady: kafkaEnvCredentialReady(env)
    }))
  }));
}

function parseFirstBootstrapHost(bootstrap) {
  const first = String(bootstrap || '').split(',')[0].trim();
  const m = first.match(/^([^:]+):(\d+)$/);
  return m ? { host: m[1], port: parseInt(m[2], 10) } : null;
}

// ─── Java / PATH setup ────────────────────────────────────────────────────────

const BREW_PATHS = [
  '/opt/homebrew/bin',
  '/opt/homebrew/sbin',
  '/opt/homebrew/opt/openjdk/bin',
  '/opt/homebrew/opt/openjdk@21/bin',
  '/opt/homebrew/opt/openjdk@17/bin',
  '/usr/local/bin'
];

function detectJavaHome() {
  try { return execSync('/usr/libexec/java_home 2>/dev/null', { encoding: 'utf8' }).trim() || null; }
  catch { return null; }
}

const JAVA_HOME = process.env.JAVA_HOME || detectJavaHome()
  || '/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home';

const childEnv = {
  ...process.env,
  PATH: [...BREW_PATHS, process.env.PATH || ''].join(':'),
  JAVA_HOME,
  // Force Java to use latin1 for stdin/stdout so proto bytes 0x80–0xFF survive the
  // pulsar-client → Node pipeline intact (avoids UTF-8 replacement-char corruption).
  JAVA_TOOL_OPTIONS: '-Dfile.encoding=ISO-8859-1 -Dstdout.encoding=ISO-8859-1'
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Single-quote shell escape
function sq(s) { return "'" + String(s).replace(/'/g, "'\\''") + "'"; }

// Strip INFO/WARN/DEBUG log lines that pulsar-client writes to stderr.
// Handles both Logback format  "14:40:53.416 [thread] INFO ..."
// and Java util.logging format "May 12, 2026 3:40:53 PM some.Class method INFO: ..."
const LOG_LINE = /^\d{2}:\d{2}:\d{2}\.\d{3}\s+\[|^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d|^\d{4}-\d{2}-\d{2}T|^INFO:\s|^WARNING:\s|\d+ messages? successfully consumed/;

function filterClientLogs(text) {
  return text.split('\n')
    .filter(line => !LOG_LINE.test(line) && line.trim())
    .join('\n')
    .trim();
}

// ─── Proto deserialization ────────────────────────────────────────────────────

const PROTO_DIR = path.join(__dirname,
  'game-state-api-master-api_outbound/api_outbound/src/main/proto');

// Registry: schema name → protobufjs MessageType
const protoTypes = {};

async function loadProtos() {
  if (!protobuf) {
    console.log('  ⚠  protobufjs not installed — run: npm install protobufjs');
    return;
  }
  try {
    const root = await protobuf.load(
      path.join(PROTO_DIR, 'game_state_api_outbound.proto')
    );
    protoTypes['gamestate_api'] = root.lookupType('com.ppb.gamestate.api.outbound.Api');
    console.log('  ✓ Proto definitions loaded (gamestate_api)');
  } catch (err) {
    console.warn('  ✗ Proto loading failed:', err.message);
  }
}

// Decode a Buffer of proto bytes → plain object.  Returns null on failure.
//
// Two sources of "extra" bytes we need to tolerate:
//   • Leading prefix  — Pulsar schema-version header before the actual proto (0–10 bytes).
//   • Trailing suffix — the \n that pulsar-client's println appends after the last message.
//
// We try all combinations of (skip leading, trim trailing) in the ranges that make sense,
// returning the first result that has at least one recognisable field.
function tryDecodeProto(schemaName, buf) {
  const MessageType = protoTypes[schemaName];
  if (!MessageType || !buf.length) return null;

  const opts = { longs: String, enums: String, bytes: 'base64', defaults: false, oneofs: true };

  for (let skip = 0; skip <= 10; skip++) {
    for (let trim = 0; trim <= 4; trim++) {
      const end = buf.length - trim;
      if (end <= skip) continue;
      try {
        const slice = buf.slice(skip, end);
        const obj   = MessageType.toObject(MessageType.decode(slice), opts);
        if (Object.keys(obj).length > 0) return obj;
      } catch { /* try next combo */ }
    }
  }
  return null;
}

// RCP StatsHandlerDeserializer: skip any non-JSON prefix, then JSON.parse (prematch stats on Confluent).
function tryDecodeStatsJson(buf) {
  if (!buf || !buf.length) return null;
  let offset = -1;
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] === 0x7b) { offset = i; break; }
  }
  if (offset < 0) return null;
  try {
    return JSON.parse(buf.slice(offset).toString('utf8'));
  } catch {
    return null;
  }
}

// Split pulsar-client --hex stdout into individual decoded messages.
// Returns: [{ raw, decoded }]  decoded is null when hex decode/proto parse fails.
function parseConsumeOutput(text, schema) {
  // Split on the "got message" delimiter line, keeping the delimiter in the result.
  // We do NOT run filterClientLogs on the message bodies because proto payloads contain
  // raw binary bytes (e.g. 0x0A = newline) that would be incorrectly stripped.
  // Log lines in the preamble / between messages are naturally discarded by the split.
  const parts = text.split(/(-{5} got message -{5}[^\n]*)/);
  // parts layout: [preamble, delim1, body1, delim2, body2, ...]

  const blocks = [];
  for (let i = 1; i < parts.length; i += 2) {
    const delim = parts[i];           // the "--- got message ---" line (includes inline metadata)
    const body  = parts[i + 1] || ''; // continuation lines (embedded newlines from binary)
    blocks.push((delim + body).trim());
  }

  if (!blocks.length) {
    // No delimiter found — filter log lines and treat whole output as one message
    const raw = filterClientLogs(text).trim();
    if (!raw) return [];
    return [{ raw, decoded: schema ? tryDecodeContent(raw, schema) : null, text: extractContentText(raw) }];
  }

  return blocks.map(block => ({
    raw: block,
    decoded: schema ? tryDecodeContent(block, schema) : null,
    text: extractContentText(block)
  }));
}

// Extract and decode content from a message block.
// Supports three cases:
//   1) --hex flag active:  "content hex: 0a1234..."  → decode from hex string
//   2) latin1 stdout:      "content: <latin1 bytes>"  → decode from latin1 Buffer
//   3) plain text topic:   "content: <json/text>"     → return as text (no proto decode)
//
// NOTE: proto payloads contain raw binary bytes including 0x0A (newline), so we must use
// [\s\S]+ (not .+) to capture the full payload across embedded newlines.
function tryDecodeContent(block, schema) {
  // Case 1: --hex output
  const hexMatch = block.match(/\bcontent\s+hex:\s*([0-9a-fA-F\s]+)/i);
  if (hexMatch) {
    const buf = Buffer.from(hexMatch[1].replace(/\s/g, ''), 'hex');
    return tryDecodeProto(schema, buf);
  }
  // Case 2: latin1 raw bytes (JAVA_TOOL_OPTIONS=ISO-8859-1 path)
  // [\s\S]+ captures across embedded newlines that binary bytes (0x0A) introduce.
  // stripTrailingLogs removes Java shutdown lines that pulsar-client appends after the last message.
  const plainMatch = block.match(/\bcontent:([\s\S]+)/);
  if (plainMatch) {
    const buf = Buffer.from(stripTrailingLogs(plainMatch[1]), 'latin1');
    return tryDecodeProto(schema, buf);
  }
  return null;
}

// Extract plain text payload from a message block (non-schema topics).
// Uses [\s\S]+ so binary content with embedded 0x0A bytes isn't truncated.
function extractContentText(block) {
  const hex = block.match(/\bcontent\s+hex:\s*([0-9a-fA-F\s]+)/i);
  if (hex) return hex[1].replace(/\s/g, '');
  const plain = block.match(/\bcontent:([\s\S]+)/);
  return plain ? stripTrailingLogs(plain[1]).trim() : block.trim();
}

// Strip Java shutdown/OTel log text that pulsar-client writes to stdout after the last message.
// Shutdown logs use comma-millisecond timestamps:  "2026-05-13T11:39:25,645+0100 [thread] INFO"
// Proto payload timestamps use period-nanosecond:  "2026-05-13T09:33:23.164569471Z"
// The two are trivially distinguished by the comma vs period after the seconds field.
function stripTrailingLogs(content) {
  const m = content.search(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2},\d{3}[+-]\d{4} \[/);
  return m > 0 ? content.slice(0, m) : content;
}

// ─── Parse  pulsar+ssl://host:port  →  { host, port }
function parseBrokerUrl(brokerUrl) {
  const m = brokerUrl.match(/^pulsar(?:\+ssl)?:\/\/([^:]+):(\d+)/);
  return m ? { host: m[1], port: parseInt(m[2]) } : null;
}

// Resolve token from environment variable
function resolveToken(tokenEnvVar) {
  return process.env[tokenEnvVar] || null;
}

// Build the pulsar-client command string
function buildClientCmd(brokerUrl, token, args) {
  const parts = ['pulsar-client', '--url', sq(brokerUrl)];
  if (token) {
    parts.push(
      '--auth-plugin', 'org.apache.pulsar.client.impl.auth.AuthenticationToken',
      '--auth-params', sq(`token:${token}`)
    );
  }
  parts.push(...args.map(sq));
  return parts.join(' ');
}

// Run pulsar-client as a one-shot command
function runPulsarClient(brokerUrl, token, args, timeout = 25000) {
  const cmd = buildClientCmd(brokerUrl, token, args);
  return new Promise((resolve) => {
    exec(cmd, { timeout, env: childEnv, encoding: 'latin1' }, (error, stdout, stderr) => {
      resolve({
        cmd,
        stdout: stdout?.trim() || '',
        stderr: filterClientLogs(stderr?.trim() || ''),
        exitCode: error?.code ?? 0,
        timedOut: error?.killed || false,
        error: error && !stderr?.trim() ? error.message : null
      });
    });
  });
}

// ─── REST API ─────────────────────────────────────────────────────────────────

// Return full config; indicate per-topic whether its token env var is set
app.get('/api/config', (_req, res) => {
  const config = loadConfig();
  const out = config.environments.map(env => ({
    ...env,
    topics: env.topics.map(topic => ({
      ...topic,
      tokenSet: !!resolveToken(topic.tokenEnvVar)
    }))
  }));
  res.json(out);
});

// Connectivity check via nc
app.post('/api/check-connectivity', (req, res) => {
  const { brokerUrl } = req.body;
  const parsed = parseBrokerUrl(brokerUrl);
  if (!parsed) return res.status(400).json({ error: 'Invalid broker URL' });

  const cmd = `nc -zv ${parsed.host} ${parsed.port}`;
  exec(cmd, { timeout: 6000, env: childEnv }, (error, stdout, stderr) => {
    const output = (stdout + '\n' + stderr).trim();
    res.json({ cmd, success: !error, output, host: parsed.host, port: parsed.port });
  });
});

// Publish a message (pulsar-client produce)
app.post('/api/produce', async (req, res) => {
  const { envId, topicId, message } = req.body;

  const found = getEnvAndTopic(envId, topicId);
  if (!found) return res.status(404).json({ error: 'Environment or topic not found' });

  const { env, topic } = found;
  const token = resolveToken(topic.tokenEnvVar);
  if (!token) return res.status(400).json({ error: `Token not set — export ${topic.tokenEnvVar} before starting the server` });
  if (!message?.trim()) return res.status(400).json({ error: 'message is required' });

  const result = await runPulsarClient(env.brokerUrl, token, [
    'produce', topic.topic,
    '--messages', message.trim(),
    '--num-produce', '1'
  ], 15000);

  res.json(result);
});

// Tool presence check
app.get('/api/check-tools', (_req, res) => {
  const which = (cmd) => new Promise((resolve) => {
    exec(`which ${cmd} 2>/dev/null`, { env: childEnv }, (err, stdout) => {
      resolve({ installed: !err && !!stdout.trim(), path: stdout.trim() });
    });
  });
  Promise.all([which('pulsar-client'), which('nc')]).then(([client, nc]) => {
    res.json({ client, nc, javaHome: JAVA_HOME, kafkajs: true });
  });
});

// ─── Kafka REST ───────────────────────────────────────────────────────────────

app.get('/api/kafka/config', (_req, res) => {
  res.json(getKafkaConfigForApi());
});

app.post('/api/kafka/check-connectivity', (req, res) => {
  const bootstrap = req.body?.bootstrap;
  const parsed = parseFirstBootstrapHost(bootstrap);
  if (!parsed) return res.status(400).json({ error: 'Invalid bootstrap (expected host:port,...)' });

  const cmd = `nc -zv ${parsed.host} ${parsed.port}`;
  exec(cmd, { timeout: 8000, env: childEnv }, (error, stdout, stderr) => {
    const output = (stdout + '\n' + stderr).trim();
    res.json({ cmd, success: !error, output, host: parsed.host, port: parsed.port });
  });
});

// ─── WebSocket — live consume (continuous polling) ────────────────────────────

const activeStreams = new Map();
const activeKafkaStreams = new Map();

async function stopKafkaStream(clientId) {
  const meta = activeKafkaStreams.get(clientId);
  if (!meta) return;
  meta.stopped = true;
  const { consumer, ws } = meta;
  activeKafkaStreams.delete(clientId);
  try {
    if (consumer) await consumer.stop();
  } catch { /* already stopping */ }
  try {
    if (consumer) await consumer.disconnect();
  } catch { /* */ }
  if (ws && ws.readyState === WebSocket.OPEN) send(ws, { type: 'consume-stopped' });
}

async function startKafkaConsumer(clientId, ws, msg) {
  const {
    envId, topicId, subscriptionName,
    subscriptionPosition = 'Latest',
    count = 10
  } = msg;

  const found = getKafkaEnvTopic(envId, topicId);
  if (!found) {
    send(ws, { type: 'error', message: 'Kafka environment or topic not found' });
    return;
  }
  const { env, topic: topicDef } = found;
  if (!kafkaEnvCredentialReady(env)) {
    send(ws, {
      type: 'error',
      message: `Kafka credentials not set — export ${env.sasl.usernameEnvVar}=… and ${env.sasl.passwordEnvVar}=… then restart the server.`
    });
    return;
  }
  if (!subscriptionName?.trim()) {
    send(ws, { type: 'error', message: 'Consumer group id is required' });
    return;
  }

  stopStream(clientId);
  await stopKafkaStream(clientId);

  send(ws, { type: 'consume-started', topicPath: topicDef.topic, label: topicDef.label });

  const brokers = env.bootstrap.split(',').map((s) => s.trim()).filter(Boolean);
  const kafkaOpts = {
    logLevel: logLevel.NOTHING,
    clientId: `webcli-${clientId.replace(/\W/g, '').slice(-14)}`,
    brokers,
    ssl: env.ssl ? true : false,
    connectionTimeout: 60000,
    requestTimeout: 90000
  };
  const saslOpt = kafkaSaslOptions(env);
  if (saslOpt) kafkaOpts.sasl = saslOpt;
  const kafka = new Kafka(kafkaOpts);

  const consumer = kafka.consumer({
    groupId: subscriptionName.trim().slice(0, 200),
    sessionTimeout: 45000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000
  });

  activeKafkaStreams.set(clientId, { consumer, stopped: false, ws });

  const schema = topicDef.schema || null;
  const valueFormat = topicDef.valueFormat || null;
  const max = count === 0 ? Infinity : (parseInt(count, 10) || 10);
  let delivered = 0;

  try {
    await consumer.connect();
    await consumer.subscribe({
      topic: topicDef.topic,
      fromBeginning: subscriptionPosition === 'Earliest'
    });

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const st = activeKafkaStreams.get(clientId);
        if (!st || st.stopped || ws.readyState !== WebSocket.OPEN) return;

        const keyStr = message.key != null ? message.key.toString('utf8') : '';
        const valueBuf = message.value != null ? Buffer.from(message.value) : Buffer.alloc(0);

        let decoded = null;
        let decodeKind = null;
        if (schema && valueBuf.length) {
          decoded = tryDecodeProto(schema, valueBuf);
          if (decoded) decodeKind = 'proto';
        }
        if (!decoded && valueBuf.length && valueFormat === 'json') {
          decoded = tryDecodeStatsJson(valueBuf);
          if (decoded) decodeKind = 'json';
        }

        let text;
        if (decoded) text = JSON.stringify(decoded);
        else {
          const binaryish = [...valueBuf].some((b) => b < 32 && b !== 9 && b !== 10 && b !== 13);
          text = binaryish ? valueBuf.toString('hex') : valueBuf.toString('utf8');
        }

        const metaLine = `kafka partition=${partition} offset=${message.offset} key=${JSON.stringify(keyStr)}`;

        send(ws, {
          type: 'messages',
          timestamp: new Date().toISOString(),
          parsedMessages: [{ raw: text, text, decoded, decodeKind, metaLine }]
        });

        delivered += 1;
        if (delivered >= max) await stopKafkaStream(clientId);
      }
    });
  } catch (e) {
    activeKafkaStreams.delete(clientId);
    try { await consumer.disconnect(); } catch { /* */ }
    if (ws.readyState === WebSocket.OPEN) send(ws, { type: 'error', message: e.message || String(e) });
    if (ws.readyState === WebSocket.OPEN) send(ws, { type: 'consume-stopped' });
  }
}

wss.on('connection', (ws) => {
  const clientId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.action === 'stop-consume') {
      const hadKafka = activeKafkaStreams.has(clientId);
      stopStream(clientId);
      if (hadKafka) void stopKafkaStream(clientId);
      else send(ws, { type: 'consume-stopped' });
      return;
    }

    if (msg.action === 'start-consume' && msg.transport === 'kafka') {
      void startKafkaConsumer(clientId, ws, msg).catch((e) => {
        if (ws.readyState === WebSocket.OPEN) {
          send(ws, { type: 'error', message: e.message || String(e) });
          send(ws, { type: 'consume-stopped' });
        }
      });
      return;
    }

    if (msg.action === 'start-consume') {
      void stopKafkaStream(clientId);
      const {
        envId, topicId, subscriptionName,
        subscriptionType = 'Shared',
        subscriptionPosition = 'Latest',
        pollInterval = 5000, count = 10
      } = msg;
      stopStream(clientId);

      const found = getEnvAndTopic(envId, topicId);
      if (!found) {
        send(ws, { type: 'error', message: 'Environment or topic not found' });
        return;
      }
      const { env, topic } = found;
      const token = resolveToken(topic.tokenEnvVar);
      if (!token) {
        send(ws, { type: 'error', message: `Token not set — export ${topic.tokenEnvVar} before starting the server` });
        return;
      }
      if (!subscriptionName?.trim()) {
        send(ws, { type: 'error', message: 'subscriptionName is required' });
        return;
      }

      send(ws, { type: 'consume-started', topicPath: topic.topic, label: topic.label });

      const startBatch = () => {
        if (ws.readyState !== WebSocket.OPEN) return;

        const schema = topic.schema || null;  // e.g. "gamestate_api"

        const args = [
          'consume', topic.topic,
          '--subscription-name', subscriptionName.trim(),
          '--subscription-type', subscriptionType,
          '--subscription-position', subscriptionPosition,
        ];
        // count === 0 means unlimited — omit --num-messages so the process runs until stopped
        if (count > 0) args.push('--num-messages', String(count));
        // Request hex-encoded content so binary proto payloads are not corrupted by Java String conversion.
        // Always add --hex when schema is configured, even if proto loading failed (avoids binary garbage).
        if (schema) args.push('--hex');

        const cmd = buildClientCmd(env.brokerUrl, token, args);

        const proc = spawn(cmd, { env: childEnv, shell: true });
        activeStreams.set(clientId, { proc, timer: null });

        let outBuf = '';
        proc.stdout.on('data', (chunk) => { outBuf += chunk.toString('latin1'); });

        proc.stderr.on('data', (chunk) => {
          const filtered = filterClientLogs(chunk.toString());
          if (filtered && ws.readyState === WebSocket.OPEN)
            send(ws, { type: 'messages', timestamp: new Date().toISOString(), stderr: filtered });
        });

        proc.on('close', () => {
          const stream = activeStreams.get(clientId);
          if (!stream) return; // was manually stopped — consume-stopped already sent

          // Parse and optionally proto-decode the stdout
          if (outBuf.trim() && ws.readyState === WebSocket.OPEN) {
            const parsedMessages = parseConsumeOutput(outBuf, schema);
            send(ws, {
              type: 'messages',
              timestamp: new Date().toISOString(),
              stdout: outBuf.trim(),         // raw text (fallback / debug)
              parsedMessages                 // [{ raw, text, decoded }]
            });
          }

          // Batch complete (process exited naturally after N messages) — stop here
          activeStreams.delete(clientId);
          if (ws.readyState === WebSocket.OPEN)
            send(ws, { type: 'consume-stopped' });
        });
      };

      startBatch();
    }
  });

  ws.on('close', () => {
    stopStream(clientId);
    void stopKafkaStream(clientId);
  });
  ws.on('error', () => {
    stopStream(clientId);
    void stopKafkaStream(clientId);
  });
});

function stopStream(clientId) {
  const stream = activeStreams.get(clientId);
  if (!stream) return;
  if (stream.proc && !stream.proc.killed) stream.proc.kill('SIGTERM');
  if (stream.timer) clearTimeout(stream.timer);
  activeStreams.delete(clientId);
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify(data));
}

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3456;

// Load proto schemas (async, non-blocking — server starts immediately)
loadProtos();

server.listen(PORT, () => {
  console.log(`\n  ⚡ FanDuel · GCD Streaming Client → http://localhost:${PORT}\n`);
  console.log(`  Pulsar JWT tokens:`);
  const config = loadConfig();
  config.environments.forEach(env => {
    console.log(`\n  [${env.name}]`);
    env.topics.forEach(t => {
      const set = !!process.env[t.tokenEnvVar];
      console.log(`    ${set ? '✓' : '✗'} ${t.tokenEnvVar} (${t.label})`);
    });
  });
  try {
    const { environments } = loadKafkaConfigRaw();
    const confluent = environments.filter((e) => e.sasl);
    if (confluent.length) {
      console.log('\n  Kafka (Confluent API keys — only if you use Confluent clusters):');
      confluent.forEach((e) => {
        const ok = kafkaEnvCredentialReady(e);
        console.log(`    ${ok ? '✓' : '✗'} ${e.name} (${e.sasl.usernameEnvVar})`);
      });
    }
  } catch (err) {
    console.warn('  ⚠  kafka-config.json:', err.message);
  }
  console.log('');
});
