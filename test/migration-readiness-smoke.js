#!/usr/bin/env node
/*
 * Smoke test for the migration-readiness engine.
 *
 * Loads migration-pairs.json and the two captured staging payloads, then
 * mirrors the JS engine in compare.html (same path resolution, same
 * isDefault classifier, same evaluation rules) to assert the verdict
 * we'd surface in the UI against the real data.
 *
 * Run: node test/migration-readiness-smoke.js
 */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const ROOT = path.resolve(__dirname, '..');

const pairs = JSON.parse(fs.readFileSync(path.join(ROOT, 'migration-pairs.json'), 'utf8')).pairs;
const kafka  = JSON.parse(fs.readFileSync(path.join(ROOT, 'response/staging/kafka-kafka-stg-rcp-mirror-sgt-2026-05-15T10-07-00.json'), 'utf8'));
const pulsar = JSON.parse(fs.readFileSync(path.join(ROOT, 'response/staging/pulsar-staging-game-state-2026-05-15T10-06-55.json'), 'utf8'));

// ── Mirror compare.html ────────────────────────────────────────────────────
function splitPath(p) {
  const out = []; let buf = '';
  for (let i = 0; i < p.length; i++) {
    const c = p[i];
    if (c === '.')                              { if (buf) out.push(buf); buf = ''; continue; }
    if (c === '[' && p[i + 1] === ']')          { if (buf) out.push(buf); out.push('[]'); buf = ''; i++; continue; }
    buf += c;
  }
  if (buf) out.push(buf);
  return out;
}
function resolveAll(root, segs) {
  let cur = [root];
  for (const seg of segs) {
    const next = [];
    if (seg === '[]') {
      for (const c of cur) if (Array.isArray(c)) for (const x of c) next.push(x);
    } else {
      for (const c of cur) if (c && typeof c === 'object' && !Array.isArray(c) && Object.prototype.hasOwnProperty.call(c, seg)) next.push(c[seg]);
    }
    cur = next;
    if (!cur.length) break;
  }
  return cur;
}
function isDefault(v) {
  if (v === undefined || v === null) return true;
  if (v === '' || v === 0 || v === '0' || v === false) return true;
  if (Array.isArray(v) && v.length === 0) return true;
  if (typeof v === 'object' && Object.keys(v).length === 0) return true;
  return false;
}

function evaluatePair(pair, kafkaMsgs, pulsarMsgs) {
  const tally = {
    critical:  { total: 0, match: 0, neutral: 0, gap: 0, bonus: 0, typeMismatch: 0 },
    important: { total: 0, match: 0, neutral: 0, gap: 0, bonus: 0, typeMismatch: 0 },
    info:      { total: 0, match: 0, neutral: 0, gap: 0, bonus: 0, typeMismatch: 0 }
  };
  const fields = pair.fields.map(f => {
    const segs = splitPath(f.path);
    const popOf = (msgs) => {
      let n = 0;
      for (const m of msgs) {
        const vs = resolveAll(m, segs);
        if (vs.some(v => !isDefault(v))) n++;
      }
      return n;
    };
    const kPop = popOf(kafkaMsgs);
    const pPop = popOf(pulsarMsgs);
    let verdict = 'neutral';
    if (kPop && pPop) verdict = 'match';
    else if (!kPop && pPop) verdict = 'bonus';
    else if (kPop && !pPop) verdict = 'gap';
    const sev = tally[f.severity] || tally.info;
    sev.total++; sev[verdict]++;
    return { ...f, kPop, pPop, verdict };
  });
  const minMsgs = pair.evaluation?.minMessagesPerSide ?? 3;
  const haveEnough = kafkaMsgs.length >= minMsgs && pulsarMsgs.length >= minMsgs;
  let state;
  if (!haveEnough) state = 'pending';
  else if (tally.critical.gap > 0 || tally.critical.typeMismatch > 0) state = 'red';
  else if (tally.important.gap > 0 || tally.important.typeMismatch > 0) state = 'amber';
  else state = 'green';
  return { fields, tally, state };
}

// ── Registry shape sanity (covers all pairs, not just gamestate) ──────────
const expectedIds = ['soccer-gamestate', 'live-mappings', 'competition-prematch-stats', 'fixture-prematch-stats'];
for (const id of expectedIds) {
  const p = pairs.find(x => x.id === id);
  assert(p, `migration-pairs.json missing pair "${id}"`);
  assert(p.endpoints?.kafka?.topicIdContains || p.endpoints?.kafka?.topicPathContains,
    `pair "${id}" must define at least one kafka matcher`);
  assert(p.endpoints?.pulsar?.topicIdContains || p.endpoints?.pulsar?.topicPathContains,
    `pair "${id}" must define at least one pulsar matcher`);
  assert(Array.isArray(p.fields) && p.fields.length > 0,
    `pair "${id}" must declare fields`);
  for (const f of p.fields) {
    assert(['critical', 'important', 'info'].includes(f.severity),
      `pair "${id}" field "${f.path}" has invalid severity "${f.severity}"`);
  }
  const sevs = p.fields.map(f => f.severity);
  assert(sevs.includes('critical'),
    `pair "${id}" should declare at least one critical field`);
}
console.log(`Registry: ${pairs.length} pair(s) — all shapes valid.`);

// ── Endpoint matcher resolution against the real catalogues ──────────────
const cfgPulsar = JSON.parse(fs.readFileSync(path.join(ROOT, 'config.json'), 'utf8'));
const cfgKafka  = JSON.parse(fs.readFileSync(path.join(ROOT, 'kafka-config.json'), 'utf8'));

function matchEndpoint(matcher, topicId, topicPath) {
  const id = (topicId || '').toLowerCase();
  const tp = (topicPath || '').toLowerCase();
  if (matcher.topicIdContains && id.includes(matcher.topicIdContains.toLowerCase())) return true;
  if (matcher.topicPathContains && tp.includes(matcher.topicPathContains.toLowerCase())) return true;
  return false;
}
function findMatches(catalogueEnvs, matcher) {
  const out = [];
  for (const env of catalogueEnvs) for (const t of env.topics || []) {
    if (matchEndpoint(matcher, t.id, t.topic)) out.push(`${env.id}::${t.id} (${t.topic})`);
  }
  return out;
}
console.log('\nMatcher resolution:');
for (const id of expectedIds) {
  const p = pairs.find(x => x.id === id);
  const kHits = findMatches(cfgKafka.environments,  p.endpoints.kafka);
  const pHits = findMatches(cfgPulsar.environments, p.endpoints.pulsar);
  console.log(`  ${id.padEnd(28)} kafka=${kHits.length}  pulsar=${pHits.length}`);
  if (!kHits.length || !pHits.length) {
    console.log('    Kafka  hits:', kHits);
    console.log('    Pulsar hits:', pHits);
  }
  assert(kHits.length > 0, `pair "${id}" has no Kafka catalogue match — check topicIdContains/topicPathContains`);
  assert(pHits.length > 0, `pair "${id}" has no Pulsar catalogue match — check topicIdContains/topicPathContains`);
}

// Cross-resolution sanity: each pair's matchers should ONLY pick its own
// topics (no leak into a sibling pair). E.g. live-mappings should NOT match
// the gamestate Pulsar topic and vice-versa.
const allKafkaTopics  = cfgKafka.environments.flatMap(e => (e.topics || []).map(t => ({ id: t.id, topic: t.topic })));
const allPulsarTopics = cfgPulsar.environments.flatMap(e => (e.topics || []).map(t => ({ id: t.id, topic: t.topic })));
for (const t of allKafkaTopics) {
  const hits = pairs.filter(p => matchEndpoint(p.endpoints.kafka, t.id, t.topic));
  if (hits.length > 1) {
    throw new Error(`Kafka topic "${t.id}/${t.topic}" matches multiple pairs: ${hits.map(h => h.id).join(', ')}`);
  }
}
for (const t of allPulsarTopics) {
  const hits = pairs.filter(p => matchEndpoint(p.endpoints.pulsar, t.id, t.topic));
  if (hits.length > 1) {
    throw new Error(`Pulsar topic "${t.id}/${t.topic}" matches multiple pairs: ${hits.map(h => h.id).join(', ')}`);
  }
}
console.log('Cross-resolution: every catalogue topic matches at most one pair.');

// ── Now do the deep gamestate evaluation (the original assertions) ──────
const pair = pairs.find(p => p.id === 'soccer-gamestate');
assert(pair, 'migration-pairs.json missing soccer-gamestate entry');

const kafkaDecoded  = kafka.messages.map(m => m.content).filter(m => m && typeof m === 'object');
const pulsarDecoded = pulsar.messages.map(m => m.content).filter(m => m && typeof m === 'object');

console.log(`Kafka decoded: ${kafkaDecoded.length}  Pulsar decoded: ${pulsarDecoded.length}`);

const result = evaluatePair(pair, kafkaDecoded, pulsarDecoded);

console.log('\nTally:');
for (const sev of ['critical', 'important', 'info']) {
  const t = result.tally[sev];
  console.log(`  ${sev.padEnd(10)} total=${t.total} match=${t.match} neutral=${t.neutral} gap=${t.gap} bonus=${t.bonus}`);
}
console.log('\nHeadline state:', result.state);

// ── Assertions ────────────────────────────────────────────────────────────
// We have enough decoded messages on both sides → state must not be 'pending'.
assert.notStrictEqual(result.state, 'pending', 'should have enough data to score');

// Critical fields with gaps?  Reveal them clearly so anyone reading the test
// output can see them.
const criticalGaps = result.fields.filter(f => f.severity === 'critical' && f.verdict === 'gap');
const importantGaps = result.fields.filter(f => f.severity === 'important' && f.verdict === 'gap');
if (criticalGaps.length) {
  console.log('\nCritical Pulsar gaps:');
  for (const g of criticalGaps) console.log(`  ${g.path}  (kPop=${g.kPop}, pPop=${g.pPop})`);
}
if (importantGaps.length) {
  console.log('\nImportant Pulsar gaps:');
  for (const g of importantGaps) console.log(`  ${g.path}  (kPop=${g.kPop}, pPop=${g.pPop})`);
}

// The captured staging samples have one known critical gap due to data
// sampling (`incidents[].participantRef[]` — Pulsar capture only saw
// PERIOD_TRANSITION incidents). That makes the engine surface RED for these
// inputs — which is *correct* behaviour: the engine should warn until the
// user re-captures a richer fixture. We assert that.
assert.strictEqual(result.state, 'red',
  'with the current sample-data-driven gap on incidents[].participantRef[], the engine should surface RED');
const expectedGap = criticalGaps.find(g => g.path === 'incidents[].participantRef[]');
assert(expectedGap, 'expected the known incidents[].participantRef[] gap to be flagged as critical');

// The fields where both sides see data should populate match-count properly.
const knownGood = result.fields.find(f => f.path === 'fixture.sportsBookId');
assert(knownGood && knownGood.verdict === 'match' && knownGood.kPop > 0 && knownGood.pPop > 0,
  'fixture.sportsBookId should be a clean match in both captures');

// Total critical/important fields should be present (sanity check on the registry).
assert(result.tally.critical.total >= 12, 'expect at least 12 critical fields in the registry');
assert(result.tally.important.total >= 10, 'expect at least 10 important fields in the registry');

console.log('\nOK · all assertions passed.');
console.log('Note: state=red is *expected* with the current capture (only PERIOD_TRANSITION incidents on Pulsar).');
console.log('      Re-capture with a fixture that has GOAL/FOUL/SUBSTITUTION events and re-run to flip to GREEN.');
