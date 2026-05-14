#!/usr/bin/env node
/**
 * Smoke test for the rewritten CSV + Markdown exports in `public/compare.html`.
 *
 * We load the page's <script> block, stub the small slice of DOM/network it
 * touches at load time, then drive `renderMarkdownReport` and `exportCsv`
 * against a synthetic two-message payload and assert on the output.
 */

const fs   = require('node:fs');
const path = require('node:path');
const vm   = require('node:vm');
const assert = require('node:assert/strict');

const HTML = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'compare.html'),
  'utf8'
);

// The page has exactly one inline <script> block — grab its body.
const m = HTML.match(/<script>([\s\S]*?)<\/script>/);
if (!m) { console.error('No <script> block found.'); process.exit(2); }

// Neutralise the auto-runs at load time so the script just defines functions.
let code = m[1]
  .replace(/^paintStaticIcons\(\);$/m,                        '/* paintStaticIcons */')
  .replace(/^\(async function init\(\) \{[\s\S]*?\}\)\(\);$/m, '/* init disabled */')
  .replace(/^window\.addEventListener\('beforeunload'[\s\S]*?\);$/m, '/* unload */');

// Sink for downloadBlob so we can capture the produced files instead of writing.
const downloads = [];

// Minimal DOM/network stubs — anything the file-scoped code touches at parse time.
function stubEl() {
  const el = {
    textContent: '', innerHTML: '', value: '',
    style: {}, dataset: {},
    children: [], options: [], selectedIndex: 0,
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    addEventListener() {}, removeEventListener() {},
    appendChild() {}, removeChild() {},
    querySelector: () => stubEl(), querySelectorAll: () => [],
    setAttribute() {}, getAttribute: () => null,
    click() {}, focus() {}, blur() {},
    get checked() { return false; }, set checked(_v) {}
  };
  return el;
}

const context = {
  console,
  document: {
    addEventListener() {}, removeEventListener() {},
    getElementById: () => stubEl(),
    querySelector:  () => stubEl(),
    querySelectorAll: () => [],
    createElement:  () => stubEl(),
    body: stubEl(),
    documentElement: { style: { setProperty() {} } }
  },
  window: { addEventListener() {}, location: { search: '' }, ICONS: {}, icon: () => '' },
  navigator: { userAgent: 'node' },
  location:  { search: '' },
  URL:       { createObjectURL: () => 'blob://x', revokeObjectURL() {} },
  Blob: class { constructor(parts, opts) { this.parts = parts; this.type = opts?.type; } },
  WebSocket: class { constructor() {} addEventListener() {} close() {} send() {} },
  performance: { now: () => Date.now() },
  setTimeout, clearTimeout, setInterval, clearInterval,
  fetch: async () => ({ json: async () => ({}), text: async () => '', ok: true }),
  alert: () => {}
};

vm.createContext(context);
vm.runInContext(code, context);

// ─── Build a synthetic two-message payload via the page's own diffBoth ─────
const a = {
  meta: { gameStateId: 'g-42', fixtureId: '99', region: 'EU' },
  score: { home: 1, away: 0 },
  status: 'LIVE',
  pulsarTimestamp: 1715587200000
};
const b = {
  meta: { gameStateId: 'g-42', fixtureId: 99 /* type mismatch */ },
  score: { home: 0 /* value differs */, away: 0 },
  status: 'LIVE',
  kafkaOffset: 17
};
const diffBoth      = context.diffBoth;
const diffStructure = context.diffStructure;
const diffValues    = context.diffValues;

const latestDiff = {
  structure: diffStructure(a, b),
  values:    diffValues(a, b),
  both:      diffBoth(a, b)
};

const pair = {
  key: 'g-42',
  a:   { ts: '2026-05-14T13:00:00.000Z', content: a },
  b:   { ts: '2026-05-14T13:00:01.500Z', content: b },
  diff: latestDiff
};

const payload = {
  meta: {
    exportedAt: '2026-05-14T13:00:02.000Z',
    correlationKey: 'meta.gameStateId',
    sourceA: { bus: 'pulsar', env: 'staging',   topic: 'persistent://gcd/soccer-inbound/game-state', description: 'Pulsar staging soccer gamestate (rendered output).' },
    sourceB: { bus: 'kafka',  env: 'kafka-stg', topic: 'trading.soccer.nxt.sgt',                     description: 'GCD source Kafka topic on UKI MSK.' },
    counts:  { a: 3, b: 3, matched: 1 },
    schemaMatchPct: latestDiff.structure.matchPct
  },
  latest:  { a, b },
  diff:    latestDiff,
  pairs:   [pair],
  // In production, buildExportPayload always provides these as arrays of
  // raw receive records (text/decoded/ts/etc.). The smoke test mirrors that
  // shape — three messages each side to match `counts`.
  streamA: [
    { ts: '2026-05-14T12:59:55.000Z', decoded: a, text: '' },
    { ts: '2026-05-14T13:00:00.000Z', decoded: a, text: '' },
    { ts: '2026-05-14T13:00:05.000Z', decoded: a, text: '' }
  ],
  streamB: [
    { ts: '2026-05-14T12:59:56.000Z', decoded: b, text: '' },
    { ts: '2026-05-14T13:00:01.500Z', decoded: b, text: '' },
    { ts: '2026-05-14T13:00:06.500Z', decoded: b, text: '' }
  ]
};

// ─── Drive the Markdown renderer directly ──────────────────────────────────
const md = context.renderMarkdownReport(payload);

// Sanity checks
assert.ok(md.includes('# Topic Comparison Report'),                  'Markdown title present');
assert.ok(md.match(/## (✅ Schemas align|⚠ Schemas mostly align|❌ Significant schema divergence)/), 'verdict heading present');
assert.ok(md.includes('At a glance'),                                'at-a-glance block present');
assert.ok(md.includes('## Field mapping (latest snapshot)'),         'field-mapping heading present');
assert.ok(md.includes('### ⚠ Type mismatches — '),                   'type-mismatch section header');
assert.ok(md.includes('### ≈ Value differences — '),                 'value-differences header');
assert.ok(md.includes('### ◧ Only in Source A — '),                  'only-in-A header');
assert.ok(md.includes('### ◨ Only in Source B — '),                  'only-in-B header');
assert.ok(md.includes('### ✅ Matches — '),                          'matches header');
assert.ok(md.includes('<details>'),                                  'matches collapsed in <details>');
assert.ok(md.includes('## Legend'),                                  'legend block present');
assert.ok(md.includes('`MATCH`'),                                    'legend documents MATCH status');
assert.ok(md.includes('`TYPE_MISMATCH`'),                            'legend documents TYPE_MISMATCH');
assert.ok(md.includes('## Matched pairs — 1'),                       'matched-pairs heading with count');
assert.ok(md.includes('Pair `g-42`'),                                'pair section present');

// The synthetic payload guarantees:
//   • a type mismatch on `meta.fixtureId` (string vs number)
//   • a value diff on `score.home`
//   • A-only paths: meta.region, pulsarTimestamp
//   • B-only paths: kafkaOffset
assert.ok(md.includes('meta.fixtureId'),     'type mismatch row rendered');
assert.ok(md.includes('score.home'),         'value diff row rendered');
assert.ok(md.includes('meta.region'),        'A-only row rendered');
assert.ok(md.includes('kafkaOffset'),        'B-only row rendered');

// ─── Build the CSV via the page logic, redirecting downloads to memory ─────
context.downloadBlob = (filename, content) => downloads.push({ filename, content });

// Capture-only buildExportPayload so exportCsv/exportJson use our synthetic payload.
context.buildExportPayload = () => payload;
context.exportFilenameBase = () => 'smoke';
context.closeExportMenu    = () => {};

// Run setTimeout callbacks immediately so the JSON export's three sequential
// downloads all land synchronously inside the test process.
context.setTimeout = (cb) => { cb(); return 0; };

context.exportCsv();

assert.equal(downloads.length, 1, 'exportCsv produced exactly one download');
const csv = downloads[0].content;

// Structural assertions on the CSV — header now carries the topic short-names
// so every value column is self-labelled.
const head = csv.split('\n')[0];
assert.ok(head.startsWith('Section,Status,Severity,Path,'), 'CSV header starts with the fixed 4-col prefix');
assert.ok(head.includes('Type [game-state]'),               'header Type column names the A-side topic leaf');
assert.ok(head.includes('Type [trading.soccer.nxt.sgt]'),   'header Type column names the B-side topic leaf');
assert.ok(head.includes('Value [game-state]'),              'header Value column names the A-side topic leaf');
assert.ok(head.includes('Value [trading.soccer.nxt.sgt]'),  'header Value column names the B-side topic leaf');
assert.ok(head.trim().endsWith('Pair Key'),                 'header ends with the Pair Key column');

// META section: topic-name rows are pinned at the top of the block.
const dataLines = csv.split('\n').filter((l) => l.startsWith('META,'));
assert.ok(dataLines[0].startsWith('META,SOURCE_A_TOPIC,'),  'META SOURCE_A_TOPIC pinned first');
assert.ok(dataLines[1].startsWith('META,SOURCE_B_TOPIC,'),  'META SOURCE_B_TOPIC pinned second');
assert.ok(/^META,EXPORTED_AT,/m.test(csv),                  'META.EXPORTED_AT row present');
assert.ok(/^META,SOURCE_A_BUS,/m.test(csv),                 'META.SOURCE_A_BUS row present');
assert.ok(/^META,SOURCE_B_BUS,/m.test(csv),                 'META.SOURCE_B_BUS row present');
assert.ok(/^META,MESSAGES_A,/m.test(csv),                   'META.MESSAGES_A row present');
assert.ok(/^META,MESSAGES_B,/m.test(csv),                   'META.MESSAGES_B row present');

// SUMMARY section: counts per status.
assert.ok(/^SUMMARY,TYPE_MISMATCH,3,/m.test(csv), 'SUMMARY TYPE_MISMATCH severity 3');
assert.ok(/^SUMMARY,ONLY_IN_A,2,/m.test(csv),     'SUMMARY ONLY_IN_A severity 2');
assert.ok(/^SUMMARY,ONLY_IN_B,2,/m.test(csv),     'SUMMARY ONLY_IN_B severity 2');
assert.ok(/^SUMMARY,VALUE_DIFFERS,1,/m.test(csv), 'SUMMARY VALUE_DIFFERS severity 1');
assert.ok(/^SUMMARY,MATCH,0,/m.test(csv),         'SUMMARY MATCH severity 0');
assert.ok(/^SUMMARY,TOTAL_PATHS,/m.test(csv),     'SUMMARY TOTAL_PATHS row present');

// FIELD_MAPPING — structural lens. Type columns populated, value columns blank.
assert.ok(/^FIELD_MAPPING,TYPE_MISMATCH,3,meta\.fixtureId,string,number,,,$/m.test(csv),
          'FIELD_MAPPING type-mismatch row has both types + empty values');
assert.ok(/^FIELD_MAPPING,ONLY_IN_A,2,meta\.region,string,,,,$/m.test(csv),
          'FIELD_MAPPING only-A row has typeA only');
assert.ok(/^FIELD_MAPPING,ONLY_IN_B,2,kafkaOffset,,number,,,$/m.test(csv),
          'FIELD_MAPPING only-B row has typeB only');
assert.ok(/^FIELD_MAPPING,TYPE_MATCH,0,score\.home,number,number,,,$/m.test(csv),
          'FIELD_MAPPING type-match row uses TYPE_MATCH status (severity 0)');

// VALUE_COMPARISON — value lens. Only paths present on both sides with
// matching types appear here; type mismatches and only-A/only-B do NOT.
// Synthetic data: a.score.home = 1, b.score.home = 0 → "1,0" in A/B order.
assert.ok(/^VALUE_COMPARISON,VALUE_DIFFERS,1,score\.home,number,number,1,0,$/m.test(csv),
          'VALUE_COMPARISON exposes the value-diff with both values side by side');
assert.ok(/^VALUE_COMPARISON,MATCH,0,score\.away,number,number,0,0,$/m.test(csv),
          'VALUE_COMPARISON exposes the equal value with both values populated');
const valueCmpLines = csv.split('\n').filter((l) => l.startsWith('VALUE_COMPARISON,'));
assert.ok(!valueCmpLines.some((l) => l.startsWith('VALUE_COMPARISON,TYPE_MISMATCH,')),
          'VALUE_COMPARISON excludes TYPE_MISMATCH rows (those live in FIELD_MAPPING)');
assert.ok(!valueCmpLines.some((l) => l.startsWith('VALUE_COMPARISON,ONLY_IN_')),
          'VALUE_COMPARISON excludes ONLY_IN_* rows (those live in FIELD_MAPPING)');

// PAIR rows still get the per-pair correlation key.
assert.ok(/^PAIR,TYPE_MISMATCH,3,meta\.fixtureId,[^,]*,[^,]*,[^,]*,[^,]*,g-42$/m.test(csv),
          'PAIR row tagged with pair key in the last column');

// FIELD_MAPPING rows are sorted by structural severity (loudest at top).
const fieldSev = csv.split('\n')
  .filter((l) => l.startsWith('FIELD_MAPPING,'))
  .map((l) => Number(l.split(',')[2]));
const fieldSorted = [...fieldSev].sort((x, y) => y - x);
assert.deepEqual(fieldSev, fieldSorted, 'FIELD_MAPPING rows ordered by severity desc');

// ─── Drive the JSON export — expects three self-contained files ────────────
//
// Filenames are short and human-readable:
//   • <leafA>Response.json       (Pulsar topic → leaf segment, e.g. `game-state`)
//   • <leafB>Response.json       (Kafka topic → flat name kept as-is)
//   • comparison.json            (no per-run prefix, always the same name)
downloads.length = 0;
context.exportJson();

assert.equal(downloads.length, 3, 'exportJson produced exactly three downloads');

// Pulsar topic `persistent://gcd/soccer-inbound/game-state` → leaf `game-state`.
// Kafka topic `trading.soccer.nxt.sgt` has no path segments → kept as-is.
const nameA = 'game-stateResponse.json';
const nameB = 'trading.soccer.nxt.sgtResponse.json';
const nameCompare = 'comparison.json';

const byName = Object.fromEntries(downloads.map((d) => [d.filename, d]));
assert.ok(byName[nameA],       `Source A file named after its topic: ${nameA}`);
assert.ok(byName[nameB],       `Source B file named after its topic: ${nameB}`);
assert.ok(byName[nameCompare], `Comparison file named <A>-vs-<B>-comparison.json: ${nameCompare}`);

const sourceA = JSON.parse(byName[nameA].content);
const sourceB = JSON.parse(byName[nameB].content);
const compAB  = JSON.parse(byName[nameCompare].content);

// SourceAResponse must be self-contained for side A.
assert.equal(sourceA.source.bus,        'pulsar', 'SourceA carries side-A identity');
assert.equal(sourceA.source.env,        'staging','SourceA carries env');
assert.equal(sourceA.messagesReceived,  3,        'SourceA carries the receive count');
assert.equal(sourceA.exportedAt,        payload.meta.exportedAt, 'SourceA carries timestamp');
assert.deepEqual(sourceA.latest,        a,        'SourceA.latest is the latest decoded A message');
assert.ok(Array.isArray(sourceA.stream),          'SourceA.stream is an array');

// SourceBResponse mirror.
assert.equal(sourceB.source.bus,        'kafka',  'SourceB carries side-B identity');
assert.deepEqual(sourceB.latest,        b,        'SourceB.latest is the latest decoded B message');

// ComparisonAB owns the cross-cutting analysis but NOT the side streams.
assert.ok(compAB.meta,                            'ComparisonAB has meta');
assert.ok(compAB.diff,                            'ComparisonAB has diff');
assert.ok(Array.isArray(compAB.pairs),            'ComparisonAB has pairs');
assert.equal(compAB.meta.correlationKey, 'meta.gameStateId', 'meta carries correlation key');
assert.equal(compAB.meta.schemaMatchPct, latestDiff.structure.matchPct, 'meta carries schema match %');
assert.ok(compAB.diff.both.rows.find((r) => r.path === 'meta.fixtureId' && r.kind === 'typediff'),
          'ComparisonAB.diff exposes the type mismatch row');
assert.equal(compAB.pairs[0].key, 'g-42',         'pair carries correlation value');
assert.ok(!('streamA' in compAB) && !('streamB' in compAB),
          'ComparisonAB stays focused on the comparison (no raw streams)');

// Optional sample dump: `node export-smoke.js --dump` writes md+csv next to it.
if (process.argv.includes('--dump')) {
  const outDir = path.join(__dirname, 'sample-output');
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, 'sample.md'),  md);
  fs.writeFileSync(path.join(outDir, 'sample.csv'), csv);
  for (const d of downloads) {
    fs.writeFileSync(path.join(outDir, d.filename), d.content);
  }
  console.log(`  Dumped sample to ${outDir}`);
}

console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('✓ export-smoke.js — all assertions passed');
console.log(`  Markdown report:        ${md.length} bytes, ${md.split('\n').length} lines`);
console.log(`  CSV report:             ${csv.length} bytes, ${csv.split('\n').length} rows`);
for (const d of downloads) {
  console.log(`  ${d.filename.padEnd(64)} ${d.content.length} bytes`);
}
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
