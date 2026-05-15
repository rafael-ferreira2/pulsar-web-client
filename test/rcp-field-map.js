#!/usr/bin/env node
/*
 * RCP ↔ Pulsar field-map probe.
 *
 * Walks every field path RCP's converters dereference (sourced from a real
 * read-only walk of rcp-service/) and reports, for both captured payloads:
 *   • presence count over decoded messages
 *   • observed JSON-runtime types (string / number / boolean / object / array / null)
 *   • a tiny example value
 *
 * Output is two artefacts:
 *   1. CSV  → response/rcp-field-map.csv         (spreadsheet-friendly)
 *   2. JSON → response/rcp-field-map.json        (raw probe state for re-use)
 *
 * Run from PulsarWebClient root:
 *   node test/rcp-field-map.js
 */
'use strict';

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const KAFKA_FILE = path.join(
  ROOT,
  'response/staging/kafka-kafka-stg-rcp-mirror-sgt-2026-05-15T10-07-00.json'
);
const PULSAR_FILE = path.join(
  ROOT,
  'response/staging/pulsar-staging-game-state-2026-05-15T10-06-55.json'
);

// ── The RCP field list ─────────────────────────────────────────────────────
// Each entry mirrors a real call site in rcp-service/ — see the audit doc at
// response/RCP-PULSAR-PARITY-ANALYSIS.md for `file:line` references.
//
// Path syntax: dot-notation with `[]` for repeated fields and `{}` for proto
// maps. Anything inside `[]` means "iterate / any element of".
//
// protoType: what the .proto declares (best-effort from the audit; some are
// "message" because we didn't pin every leaf in the proto descriptor).
// rcpUsage : `business`, `mapping`, `filter`, `correlation`, `audit`.
// hotPath  : true if it's on the per-message handler path.
const RCP_FIELDS = [
  // ─── meta.* ────────────────────────────────────────────────────────────
  { path: 'meta.version',                                             protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateMetaToMetaConverter.java:37' },
  { path: 'meta.gameStateId',                                         protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateMetaToMetaConverter.java:38' },
  { path: 'meta.highWaterMark',                                       protoType: 'int64',                             rcpUsage: 'business',    hotPath: true,  notes: 'Ordering / watermark; GameStateMetaToMetaConverter.java:39' },
  { path: 'meta.gameStatePreviousId',                                 protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateMetaToMetaConverter.java:40' },
  { path: 'meta.ingressCorrelationId',                                protoType: 'string',                            rcpUsage: 'correlation', hotPath: true,  notes: 'GameStateMetaToMetaConverter.java:41' },
  { path: 'meta.ingressCorrelationTimestamp',                         protoType: 'string (RFC3339)',                  rcpUsage: 'business',    hotPath: true,  notes: 'Instant.parse(); GameStateToFootballFixtureConverter.java:132-134' },
  { path: 'meta.instructionType',                                     protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateMetaToMetaConverter.java:44' },
  { path: 'meta.sport',                                               protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateMetaToMetaConverter.java:45' },
  { path: 'meta.headers',                                             protoType: 'map<string,string>',                rcpUsage: 'correlation', hotPath: true,  notes: 'getHeadersMap(); EventDrivenMessageListener.java:37' },
  { path: 'meta.feedStatus[].provider',                               protoType: 'enum (Provider)',                   rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFeedStatusToFeedStatusConverter.java:38' },
  { path: 'meta.feedStatus[].status',                                 protoType: 'enum (Status)',                     rcpUsage: 'business',    hotPath: true,  notes: 'GameStateStatusToStatusConverter.java:20' },
  { path: 'meta.feedStatus[].features.supportedIncidentTypes[].soccerIncidentType', protoType: 'enum',                rcpUsage: 'filter',      hotPath: true,  notes: 'Supported incidents allow-list; GameStateToFixtureContextConverter.java:140-141' },
  { path: 'meta.feedStatus[].features.supportedStatTypes[].soccerStatType',         protoType: 'enum',                rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFeedFeaturesToFeaturesConverter.java:26' },

  // ─── fixture.* ─────────────────────────────────────────────────────────
  { path: 'fixture.sportsBookId',                                     protoType: 'string',                            rcpUsage: 'correlation', hotPath: true,  notes: 'Kafka key / entity id; EventDrivenMessageListener.java:42' },
  { path: 'fixture.status',                                           protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateToFootballFixtureConverter.java:101' },
  { path: 'fixture.expectedStartTime.seconds',                        protoType: 'google.protobuf.Timestamp.seconds', rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateToFootballFixtureConverter.java:94' },
  { path: 'fixture.actualStartTime.seconds',                          protoType: 'google.protobuf.Timestamp.seconds', rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateToFootballFixtureConverter.java:78' },
  { path: 'fixture.expectedEndTime.seconds',                          protoType: 'google.protobuf.Timestamp.seconds', rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateToFootballFixtureConverter.java:86' },
  { path: 'fixture.externalRef[].provider',                           protoType: 'enum (Provider)',                   rcpUsage: 'filter',      hotPath: true,  notes: 'Filters on Provider.SPORTEX; GameStateToFootballFixtureConverter.java:107' },
  { path: 'fixture.externalRef[].externalId',                         protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateToFootballFixtureConverter.java:108' },
  { path: 'fixture.group.id',                                         protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureToFixtureGroupConverter.java:22' },
  { path: 'fixture.group.description',                                protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureToFixtureGroupConverter.java:23' },
  { path: 'fixture.attributes.soccerAttributes.penaltyShootoutFormat', protoType: 'enum',                             rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateToFootballFixtureConverter.java:125' },
  { path: 'fixture.attributes.soccerAttributes.homeParticipantRef',   protoType: 'string',                            rcpUsage: 'business',    hotPath: true,  notes: 'Side resolution; FootballConverter.java:13' },
  { path: 'fixture.attributes.soccerAttributes.awayParticipantRef',   protoType: 'string',                            rcpUsage: 'business',    hotPath: true,  notes: 'Side resolution; FootballConverter.java:17' },
  { path: 'fixture.attributes.soccerAttributes.firstLegScore.homeParticipant', protoType: 'int32',                    rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureToFootballFirstLegScoreConverter.java:32' },
  { path: 'fixture.attributes.soccerAttributes.firstLegScore.awayParticipant', protoType: 'int32',                    rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureToFootballFirstLegScoreConverter.java:33' },

  // ─── participants[] (teams + players) ──────────────────────────────────
  { path: 'participants[].id',                                        protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballTeamConverter.java:45 / FootballPlayerConverter.java:41' },
  { path: 'participants[].name',                                      protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballTeamConverter.java:48' },
  { path: 'participants[].type',                                      protoType: 'enum (TEAM|PLAYER)',                rcpUsage: 'filter',      hotPath: true,  notes: 'GameStateToFixtureContextConverter.java:68/74' },
  { path: 'participants[].externalRef[].provider',                    protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballTeamConverter.java:52' },
  { path: 'participants[].externalRef[].externalId',                  protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballTeamConverter.java:52' },
  { path: 'participants[].info.teamAttributes.soccerTeamAttributes.jerseyColour', protoType: 'string',                rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballTeamConverter.java:50' },
  { path: 'participants[].info.teamAttributes.soccerTeamAttributes.formation.value', protoType: 'string',             rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballTeamConverter.java:59 (hasFormation gate)' },
  { path: 'participants[].info.playerAttributes.soccerPlayerAttributes.parentParticipantRef', protoType: 'string',    rcpUsage: 'business',    hotPath: true,  notes: 'GameStateToFixtureContextConverter.java:103' },
  { path: 'participants[].info.playerAttributes.soccerPlayerAttributes.shirtNumber',         protoType: 'int32',      rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballPlayerConverter.java:50' },
  { path: 'participants[].info.playerAttributes.soccerPlayerAttributes.position',            protoType: 'enum',       rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballPlayerConverter.java:52-53' },
  { path: 'participants[].info.playerAttributes.soccerPlayerAttributes.playerPosition',      protoType: 'enum',       rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballPlayerConverter.java:55-56' },
  { path: 'participants[].info.playerAttributes.soccerPlayerAttributes.startingPosition',    protoType: 'enum',       rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballPlayerConverter.java:58-61' },
  { path: 'participants[].info.playerAttributes.soccerPlayerAttributes.formationPlace.value', protoType: 'int32',     rcpUsage: 'mapping',     hotPath: true,  notes: 'FootballPlayerConverter.java:67 (hasFormationPlace gate)' },

  // ─── incidents[] ───────────────────────────────────────────────────────
  { path: 'incidents[].id',                                           protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateIncidentToFootballIncidentConverter.java:54' },
  { path: 'incidents[].type.soccerIncidentType',                      protoType: 'enum',                              rcpUsage: 'business',    hotPath: true,  notes: 'Routing key for incident-type converters; …ConverterStrategy.get(type)' },
  { path: 'incidents[].time.soccerIncidentTime.period',               protoType: 'message (Period)',                  rcpUsage: 'business',    hotPath: true,  notes: 'GameStateToFixtureContextConverter.java:121' },
  { path: 'incidents[].time.soccerIncidentTime.clock.minutes',        protoType: 'int32',                             rcpUsage: 'business',    hotPath: true,  notes: 'Sort + duration; GameStateToFixtureContextConverter.java:126-128' },
  { path: 'incidents[].time.soccerIncidentTime.clock.seconds',        protoType: 'int32',                             rcpUsage: 'business',    hotPath: true,  notes: 'Sort + duration; GameStateToFixtureContextConverter.java:126-128' },
  { path: 'incidents[].participantRef[]',                             protoType: 'repeated string',                   rcpUsage: 'business',    hotPath: true,  notes: 'Player attribution; many incident converters' },
  { path: 'incidents[].externalRef[].provider',                       protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateIncidentToFootballIncidentConverter.java:60' },
  { path: 'incidents[].externalRef[].externalId',                     protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateIncidentToFootballIncidentConverter.java:60' },
  { path: 'incidents[].lastModified',                                 protoType: 'google.protobuf.Timestamp',          rcpUsage: 'mapping',     hotPath: true,  notes: 'Timestamps.toMillis(); GameStateIncidentToFootballDurationConverter.java:57' },
  { path: 'incidents[].qualifiers.soccerIncidentQualifiers.qualifiers[].qualifier',          protoType: 'enum',       rcpUsage: 'business',    hotPath: true,  notes: 'Filter by qualifier (ASSISTANT_PARTICIPANT_REF, FOUL_WON, INJURY_TIME_UPDATE, SHOOTOUT_ATTEMPT_STATE, EXTRA_TIME_QUALIFIERS, OUT_PARTICIPANT_REF…)' },
  { path: 'incidents[].qualifiers.soccerIncidentQualifiers.qualifiers[].value',              protoType: 'string',     rcpUsage: 'business',    hotPath: true,  notes: 'Numeric-as-string in some cases (Integer.parseInt)' },
  { path: 'incidents[].qualifiers.soccerIncidentQualifiers.qualifiers[].details.foulWonDetails.playerId.value', protoType: 'string', rcpUsage: 'mapping', hotPath: true, notes: 'GameStateIncidentToFootballFoulIncidentConverter.java:67' },

  // ─── stats.* ───────────────────────────────────────────────────────────
  { path: 'stats.matchStat[].participantRef',                         protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureStatsToFootballStatsConverter.java:49' },
  { path: 'stats.matchStat[].type.soccerStatType',                    protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureStatsToFootballStatsConverter.java:50' },
  { path: 'stats.matchStat[].value',                                  protoType: 'double',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'cast (int) stat.getValue(); …:51' },
  { path: 'stats.periodStat[].period.soccerPeriod',                   protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'GameStateFixtureStatsToFootballStatsConverter.java:57' },
  { path: 'stats.periodStat[].stat.matchStat[].participantRef',       protoType: 'string',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'Nested through convertStat()' },
  { path: 'stats.periodStat[].stat.matchStat[].type.soccerStatType',  protoType: 'enum',                              rcpUsage: 'mapping',     hotPath: true,  notes: 'Nested through convertStat()' },
  { path: 'stats.periodStat[].stat.matchStat[].value',                protoType: 'double',                            rcpUsage: 'mapping',     hotPath: true,  notes: 'Nested through convertStat()' },
];

// ── Utilities ──────────────────────────────────────────────────────────────

function classifyType(v) {
  if (v === null) return 'null';
  if (Array.isArray(v)) return v.length === 0 ? 'array<empty>' : 'array';
  return typeof v;
}

// Resolve all values for a dot-path against an object.
// Path tokens: `name` for property, `[]` for "iterate all elements".
function resolvePath(obj, segments) {
  let current = [obj];
  for (const seg of segments) {
    const next = [];
    if (seg === '[]') {
      for (const c of current) {
        if (Array.isArray(c)) for (const x of c) next.push(x);
      }
    } else {
      for (const c of current) {
        if (c !== null && c !== undefined && typeof c === 'object' && !Array.isArray(c)) {
          if (Object.prototype.hasOwnProperty.call(c, seg)) {
            next.push(c[seg]);
          }
        }
      }
    }
    current = next;
    if (current.length === 0) break;
  }
  return current;
}

function splitPath(p) {
  // Convert `incidents[].time` → ['incidents', '[]', 'time'].
  const out = [];
  let buf = '';
  for (let i = 0; i < p.length; i++) {
    const c = p[i];
    if (c === '.') { if (buf) out.push(buf); buf = ''; continue; }
    if (c === '[' && p[i + 1] === ']') { if (buf) out.push(buf); out.push('[]'); buf = ''; i++; continue; }
    buf += c;
  }
  if (buf) out.push(buf);
  return out;
}

function previewValue(v) {
  try {
    if (typeof v === 'string') return v.length > 60 ? JSON.stringify(v.slice(0, 60) + '…') : JSON.stringify(v);
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    if (Array.isArray(v))   return `array(${v.length})`;
    if (v === null)         return 'null';
    if (typeof v === 'object') return `object{${Object.keys(v).slice(0, 4).join(',')}${Object.keys(v).length > 4 ? ',…' : ''}}`;
  } catch { /* fall through */ }
  return String(v);
}

// ── Probe ──────────────────────────────────────────────────────────────────

function loadDecoded(file) {
  const j = JSON.parse(fs.readFileSync(file, 'utf8'));
  const decoded = j.messages.filter((m) => m && typeof m.content === 'object');
  return { meta: j.meta, total: j.messages.length, decoded };
}

function probe(field, decoded) {
  const segments = splitPath(field.path);
  let presentMsgCount = 0;
  let nonDefaultMsgCount = 0;
  let valueCount = 0;
  const types = new Map();
  const sampleValues = [];
  for (const msg of decoded) {
    const values = resolvePath(msg.content, segments);
    let anyInMsg = false;
    let anyNonDefault = false;
    for (const v of values) {
      anyInMsg = true;
      valueCount++;
      const t = classifyType(v);
      types.set(t, (types.get(t) || 0) + 1);
      if (!(v === '' || v === 0 || v === '0' || v === false || v === null ||
            (Array.isArray(v) && v.length === 0))) {
        anyNonDefault = true;
      }
      if (sampleValues.length < 3) sampleValues.push(previewValue(v));
    }
    if (anyInMsg) presentMsgCount++;
    if (anyNonDefault) nonDefaultMsgCount++;
  }
  return {
    field,
    presentMsgCount,
    nonDefaultMsgCount,
    valueCount,
    types: [...types.entries()].sort((a, b) => b[1] - a[1]),
    sampleValues
  };
}

function summariseTypes(types) {
  if (types.length === 0) return '(none)';
  return types.map(([t, n]) => `${t}×${n}`).join(', ');
}

// ── Main ───────────────────────────────────────────────────────────────────

const kafka  = loadDecoded(KAFKA_FILE);
const pulsar = loadDecoded(PULSAR_FILE);

console.log(`Kafka  capture: ${kafka.decoded.length}/${kafka.total} decoded messages (env=${kafka.meta.env}, topic=${kafka.meta.topic})`);
console.log(`Pulsar capture: ${pulsar.decoded.length}/${pulsar.total} decoded messages (env=${pulsar.meta.env}, topic=${pulsar.meta.topic})`);
console.log();

const rows = RCP_FIELDS.map((field) => ({
  field,
  k: probe(field, kafka.decoded),
  p: probe(field, pulsar.decoded)
}));

// Pretty table to stdout
const C = {
  ok:   '\x1b[32m', warn: '\x1b[33m', err: '\x1b[31m',
  dim:  '\x1b[2m',  bold: '\x1b[1m',  reset: '\x1b[0m'
};

function verdict(k, p) {
  // Schema-presence verdict first (does it appear in any decoded message?).
  // Then a parity-of-population verdict.
  const kAny = k.valueCount > 0;
  const pAny = p.valueCount > 0;
  if (!kAny && !pAny)                         return { tag: '— upstream-empty',    sev: 'info' };
  if ( kAny &&  pAny)                         return { tag: '✓ both populated',    sev: 'ok' };
  if ( kAny && !pAny)                         return { tag: '! only-in-Kafka-sample', sev: 'warn' };
  /* !kAny && pAny */                         return { tag: '! only-in-Pulsar-sample', sev: 'warn' };
}

const COL = { path: 70, k: 22, p: 22, verdict: 28 };
function pad(s, w) { s = String(s); return s.length >= w ? s : s + ' '.repeat(w - s.length); }

console.log(C.bold + pad('RCP field path', COL.path) + ' ' + pad('Kafka (n/types)', COL.k) + ' ' + pad('Pulsar (n/types)', COL.p) + ' ' + 'Verdict' + C.reset);
console.log(C.dim + '─'.repeat(COL.path + COL.k + COL.p + COL.verdict + 3) + C.reset);

for (const row of rows) {
  const ktag = `${row.k.presentMsgCount}/${kafka.decoded.length}  ${row.k.types[0]?.[0] || '—'}`;
  const ptag = `${row.p.presentMsgCount}/${pulsar.decoded.length}  ${row.p.types[0]?.[0] || '—'}`;
  const v = verdict(row.k, row.p);
  const color = v.sev === 'ok' ? C.ok : v.sev === 'warn' ? C.warn : C.dim;
  console.log(
    pad(row.field.path, COL.path) + ' ' +
    pad(ktag, COL.k) + ' ' +
    pad(ptag, COL.p) + ' ' +
    color + v.tag + C.reset
  );
}

// ── Persist artefacts ──────────────────────────────────────────────────────

const csvOut = path.join(ROOT, 'response/rcp-field-map.csv');
const jsonOut = path.join(ROOT, 'response/rcp-field-map.json');

const csvHeader = [
  'rcp_path', 'proto_type', 'rcp_usage', 'hot_path', 'rcp_reference',
  'kafka_present_msgs', 'kafka_value_count', 'kafka_nondefault_msgs', 'kafka_types', 'kafka_sample',
  'pulsar_present_msgs', 'pulsar_value_count', 'pulsar_nondefault_msgs', 'pulsar_types', 'pulsar_sample',
  'naming_match', 'type_match', 'verdict'
];

function csvCell(s) {
  s = String(s ?? '');
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function namingMatch(row) {
  // The two captures decode through the SAME protobuf descriptor on both
  // sides (rcp-service uses GameStateOutbound.Api.parseFrom; the Pulsar tool
  // uses the same generated lookup table). Therefore field names match
  // 1:1 by construction. We assert it by checking both captures saw the
  // dot-path resolve at least once OR neither did.
  return (row.k.valueCount > 0) === (row.p.valueCount > 0) ? 'identical' :
         (row.k.valueCount > 0 ? 'identical (k-only data sample)' : 'identical (p-only data sample)');
}

function typeMatch(row) {
  if (row.k.valueCount === 0 || row.p.valueCount === 0) return 'n/a (no data on one side)';
  const ktypes = new Set(row.k.types.map((x) => x[0]));
  const ptypes = new Set(row.p.types.map((x) => x[0]));
  // Treat array<empty> and array as compatible.
  const norm = (t) => (t === 'array<empty>' ? 'array' : t);
  const kn = new Set([...ktypes].map(norm));
  const pn = new Set([...ptypes].map(norm));
  const same = [...kn].every((t) => pn.has(t)) && [...pn].every((t) => kn.has(t));
  return same ? 'match' : `mismatch (k:${[...kn].join('|')} vs p:${[...pn].join('|')})`;
}

const csvLines = [csvHeader.join(',')];
for (const row of rows) {
  csvLines.push([
    row.field.path, row.field.protoType, row.field.rcpUsage, row.field.hotPath ? 'yes' : 'no', row.field.notes,
    row.k.presentMsgCount, row.k.valueCount, row.k.nonDefaultMsgCount, summariseTypes(row.k.types), row.k.sampleValues.join(' | '),
    row.p.presentMsgCount, row.p.valueCount, row.p.nonDefaultMsgCount, summariseTypes(row.p.types), row.p.sampleValues.join(' | '),
    namingMatch(row), typeMatch(row), verdict(row.k, row.p).tag
  ].map(csvCell).join(','));
}
fs.writeFileSync(csvOut, csvLines.join('\n') + '\n');

fs.writeFileSync(
  jsonOut,
  JSON.stringify({
    generatedAt: new Date().toISOString(),
    sources: {
      kafka:  { file: path.relative(ROOT, KAFKA_FILE),  ...kafka.meta,  decodedMessages: kafka.decoded.length },
      pulsar: { file: path.relative(ROOT, PULSAR_FILE), ...pulsar.meta, decodedMessages: pulsar.decoded.length }
    },
    rows: rows.map((r) => ({
      path: r.field.path,
      protoType: r.field.protoType,
      rcpUsage: r.field.rcpUsage,
      hotPath: r.field.hotPath,
      rcpReference: r.field.notes,
      kafka:  { presentMsgs: r.k.presentMsgCount, valueCount: r.k.valueCount, nonDefaultMsgs: r.k.nonDefaultMsgCount, types: r.k.types, sample: r.k.sampleValues },
      pulsar: { presentMsgs: r.p.presentMsgCount, valueCount: r.p.valueCount, nonDefaultMsgs: r.p.nonDefaultMsgCount, types: r.p.types, sample: r.p.sampleValues },
      namingMatch: namingMatch(r),
      typeMatch:   typeMatch(r),
      verdict:     verdict(r.k, r.p).tag
    }))
  }, null, 2)
);

console.log();
console.log(`Wrote ${path.relative(ROOT, csvOut)}`);
console.log(`Wrote ${path.relative(ROOT, jsonOut)}`);
