# GCD Pulsar message exports — reader guide

This folder is meant to be **shared as-is** with partners who do **not** have access to our internal Pulsar client repo or configuration. Everything needed to interpret the JSON files should be documented here.

## What each JSON file contains

Exports are produced by a small internal tool. Structure is consistent:

| Field | Meaning |
|--------|---------|
| `meta.env` | Environment label (e.g. `staging`). |
| `meta.topic` | Full Pulsar topic name (tenant / namespace / topic). |
| `meta.messageCount` | Number of entries in `messages` (see counting note below). |
| `meta.exportedAt` | When the file was written (ISO-8601 UTC). |
| `messages[]` | Ordered rows: `index`, `timestamp`, `content`. |
| `session_log[]` | High-level consume lifecycle (`consume_started`, `consume_stopped`, etc.). |

**Counting messages:** `meta.messageCount` equals `messages.length`. The first `messages` row is often a JVM bootstrap line (`Picked up JAVA_TOOL_OPTIONS: …`), not a Pulsar payload. For a quick **topic payload** count, subtract that row when present.

---

## GCD topic and broker reference (FanDuel / Flutter)

Topic **paths** are the same in staging and production; only the **broker host** changes.

### Staging

| Logical name | Pulsar topic (full path) | TLS broker URL |
|--------------|--------------------------|----------------|
| Soccer live gamestate | `persistent://gcd/soccer-inbound/game-state` | `pulsar+ssl://pc-24d7b75e.gcd.dts.use2.flutterpsn.com:6651` |
| Live mappings | `persistent://gcd/livedata-inbound/livedata-mappings` | *(same broker)* |
| Competition pre-match stats | `persistent://gcd/soccer-inbound/competition-prematch-stats` | *(same broker)* |
| Fixture pre-match stats | `persistent://gcd/soccer-inbound/fixture-prematch-stats` | *(same broker)* |

### Production (for future exports)

| Logical name | Pulsar topic (full path) | TLS broker URL |
|--------------|--------------------------|----------------|
| Soccer live gamestate | `persistent://gcd/soccer-inbound/game-state` | `pulsar+ssl://pc-a62b5405.gcd.prd.use2.flutterpsn.com:6651` |
| Live mappings | `persistent://gcd/livedata-inbound/livedata-mappings` | *(same broker)* |
| Competition pre-match stats | `persistent://gcd/soccer-inbound/competition-prematch-stats` | *(same broker)* |
| Fixture pre-match stats | `persistent://gcd/soccer-inbound/fixture-prematch-stats` | *(same broker)* |

**Schema note (gamestate only):** the live **game-state** topic is published with a **protobuf** schema (gamestate outbound API). Exports from our listener may already show **decoded JSON** in `messages[].content`. Other topics may appear as raw strings, binary prefixes plus JSON, or other encodings—see [STAGING-CAPTURE-SUMMARY.md](STAGING-CAPTURE-SUMMARY.md).

---

## Captures included in this bundle

| Document | Contents |
|----------|----------|
| [STAGING-CAPTURE-SUMMARY.md](STAGING-CAPTURE-SUMMARY.md) | Staging run from **2026-05-13**: per-file summary, links under `staging/`. |

When production samples are added, place them under `production/` and extend the capture summary (or add `PRODUCTION-CAPTURE-SUMMARY.md`) the same way.

---

## Directory layout

```
response/
  README.md                        ← start here
  STAGING-CAPTURE-SUMMARY.md       ← index of current staging exports
  staging/
    *.json                         ← exported message captures
  production/                      ← optional; add when prod exports exist
```
