# Pulsar export capture — staging (2026-05-13)

**Connection and topic definitions** for staging and production are in [README.md](README.md) in this same folder (self-contained for teams without access to our internal client config).

This document indexes **this** staging export run only. There is **no** `production/` folder in this bundle yet.

**Staging broker (duplicate for convenience):** `pulsar+ssl://pc-24d7b75e.gcd.dts.use2.flutterpsn.com:6651`

**How to read the numbers:** Each file’s `meta.messageCount` matches the length of the `messages` array. The first entry is often the JVM line `Picked up JAVA_TOOL_OPTIONS: …` (not a Pulsar payload). The rows after that are what was read from the topic in that session.

---

## Per-topic summary

| Topic (Pulsar path) | UI label | Session outcome | `meta.messageCount` | Approx. topic payloads | Export file (relative) |
|---------------------|----------|-------------------|---------------------|------------------------|-------------------------|
| `persistent://gcd/soccer-inbound/game-state` | Soccer live gamestate | `consume_started` → `consume_stopped` | 11 | 10 (after JVM line) | [staging/pulsar-staging-game-state-2026-05-13T14-03-06.json](staging/pulsar-staging-game-state-2026-05-13T14-03-06.json) |
| `persistent://gcd/livedata-inbound/livedata-mappings` | Live mappings | `consume_started` → `consume_stopped` | 11 | 10 | [staging/pulsar-staging-livedata-mappings-2026-05-13T14-03-10.json](staging/pulsar-staging-livedata-mappings-2026-05-13T14-03-10.json) |
| `persistent://gcd/soccer-inbound/competition-prematch-stats` | Competition pre-match stats | `consume_started` → `consume_stopped` | 11 | 10 | [staging/pulsar-staging-competition-prematch-stats-2026-05-13T14-03-13.json](staging/pulsar-staging-competition-prematch-stats-2026-05-13T14-03-13.json) |
| `persistent://gcd/soccer-inbound/fixture-prematch-stats` | Fixture pre-match stats | `consume_started` → `consume_stopped` | 11 | 10 | [staging/pulsar-staging-fixture-prematch-stats-2026-05-13T14-03-15.json](staging/pulsar-staging-fixture-prematch-stats-2026-05-13T14-03-15.json) |

---

## Payload shape (for readers of the JSON, not a deep dive)

- **Game state:** `messages[].content` is mostly **decoded structured JSON** (protobuf decoded in the export tool).
- **Live mappings:** `content` is **opaque / wire-oriented text** (embedded control characters); not the same as gamestate JSON.
- **Competition / fixture pre-match stats:** `content` is typically a **string** with a small **non-JSON binary prefix** followed by a **JSON document** (`header` / `body`, SPMS-style envelopes).

For authoritative timestamps and topic identity, use each file’s `meta` and `session_log`.
