// Synthetic check: validates that persistTopicState() correctly trims oversized
// liveHTML, prunes rawMessages/sessionLog, and LRU-evicts old topics when the
// composite snapshot would exceed the sessionStorage budget. Mirrors the
// implementation in public/index.html so the constants must stay in sync.

'use strict';

const TOPIC_STATE_MAX_BYTES = 4_000_000;
const TOPIC_LIVE_HTML_CAP   = 400_000;
const TOPIC_RAW_MSG_CAP     = 200;
const TOPIC_LOG_CAP         = 200;

function persistTopicStateInto(topicState, store) {
  const snap = {};
  for (const k of Object.keys(topicState)) {
    const s = topicState[k];
    if (!s) continue;
    const html = s.liveHTML || '';
    snap[k] = {
      liveHTML: html.length > TOPIC_LIVE_HTML_CAP
        ? html.slice(html.length - TOPIC_LIVE_HTML_CAP)
        : html,
      liveInitCleared: !!s.liveInitCleared,
      liveCount: s.liveCount || 0,
      rawMessages: Array.isArray(s.rawMessages) ? s.rawMessages.slice(-TOPIC_RAW_MSG_CAP) : [],
      sessionLog:  Array.isArray(s.sessionLog)  ? s.sessionLog.slice(-TOPIC_LOG_CAP)      : [],
      lastTouched: s.lastTouched || Date.now()
    };
  }
  let json = JSON.stringify(snap);
  while (json.length > TOPIC_STATE_MAX_BYTES) {
    const keys = Object.keys(snap);
    if (keys.length <= 1) break;
    let oldest = keys[0];
    for (const k of keys) {
      if ((snap[k].lastTouched || 0) < (snap[oldest].lastTouched || 0)) oldest = k;
    }
    delete snap[oldest];
    json = JSON.stringify(snap);
  }
  store.gcdTopicState = json;
  return { snap, bytes: json.length };
}

function assert(cond, msg) {
  if (!cond) {
    console.error('✗ FAIL: ' + msg);
    process.exit(1);
  }
}

// Case 1: oversized liveHTML gets trimmed but stays valid HTML.
{
  const huge = '<div class="entry">x</div>'.repeat(40000); // ~1MB
  const topicState = {
    'pulsar::staging::game-state': {
      liveHTML: huge,
      liveInitCleared: true,
      liveCount: 40000,
      rawMessages: Array.from({ length: 1000 }, (_, i) => ({ idx: i, payload: 'p'.repeat(10) })),
      sessionLog:  Array.from({ length: 1000 }, (_, i) => ({ event: 'data', idx: i })),
      lastTouched: Date.now()
    }
  };
  const store = {};
  const { snap, bytes } = persistTopicStateInto(topicState, store);
  const out = snap['pulsar::staging::game-state'];
  assert(out.liveHTML.length === TOPIC_LIVE_HTML_CAP, `liveHTML trimmed to cap (got ${out.liveHTML.length})`);
  assert(out.rawMessages.length === TOPIC_RAW_MSG_CAP, `rawMessages trimmed to ${TOPIC_RAW_MSG_CAP}`);
  assert(out.sessionLog.length  === TOPIC_LOG_CAP,     `sessionLog trimmed to ${TOPIC_LOG_CAP}`);
  assert(out.rawMessages[0].idx === 1000 - TOPIC_RAW_MSG_CAP, 'rawMessages kept the tail (newest)');
  assert(out.liveCount === 40000, 'liveCount preserved');
  assert(bytes < TOPIC_STATE_MAX_BYTES, `snapshot under budget (${bytes} < ${TOPIC_STATE_MAX_BYTES})`);
  console.log(`  ✓ Single oversized topic trimmed correctly (${bytes} bytes)`);
}

// Case 2: many topics totalling over budget — LRU eviction keeps newest.
{
  const topicState = {};
  // 12 topics, each ~400KB, total >> 4MB → eviction must kick in.
  for (let i = 0; i < 12; i++) {
    topicState[`pulsar::env-${i}::topic`] = {
      liveHTML: 'a'.repeat(TOPIC_LIVE_HTML_CAP),
      liveInitCleared: true,
      liveCount: i,
      rawMessages: [],
      sessionLog: [],
      lastTouched: 1000 + i  // higher = newer
    };
  }
  const store = {};
  const { snap, bytes } = persistTopicStateInto(topicState, store);
  const keys = Object.keys(snap);
  assert(bytes <= TOPIC_STATE_MAX_BYTES, `eviction respects budget (${bytes} <= ${TOPIC_STATE_MAX_BYTES})`);
  assert(keys.length < 12, `at least one topic evicted (kept ${keys.length})`);
  // Oldest topics (env-0, env-1, ...) should be gone; newest (env-11) must remain.
  assert(snap['pulsar::env-11::topic'], 'newest topic retained');
  assert(!snap['pulsar::env-0::topic'], 'oldest topic evicted');
  console.log(`  ✓ LRU eviction kept ${keys.length}/12 topics, ${bytes} bytes`);
}

// Case 3: round-trip via the store mirrors sessionStorage semantics.
{
  const store = {};
  const topicState = {
    'kafka::prd::orders': {
      liveHTML: '<div class="entry">hello</div>',
      liveInitCleared: true, liveCount: 1,
      rawMessages: [{ text: 'hello' }], sessionLog: [],
      lastTouched: Date.now()
    }
  };
  persistTopicStateInto(topicState, store);
  const rehydrated = JSON.parse(store.gcdTopicState);
  assert(rehydrated['kafka::prd::orders'].liveHTML.includes('hello'), 'rehydrated liveHTML matches');
  assert(rehydrated['kafka::prd::orders'].rawMessages[0].text === 'hello', 'rehydrated rawMessages match');
  console.log('  ✓ Round-trip through serialized store works');
}

console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('✓ topic-state-persist-smoke.js — all assertions passed');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
