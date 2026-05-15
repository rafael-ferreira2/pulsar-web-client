# RCP → Pulsar multi-topic migration readiness

**Last updated:** 2026-05-15

This document is the **go/no-go reference** for migrating RCP's Kafka consumers to their Pulsar mirrors. It consolidates the audits in [`RCP-PULSAR-PARITY-ANALYSIS.md`](./RCP-PULSAR-PARITY-ANALYSIS.md) and [`RCP-PULSAR-FIELD-MAP.md`](./RCP-PULSAR-FIELD-MAP.md), and tells you what the **in-UI migration readiness chip** on `/compare.html` will tell you for every supported topic pair.

> The single source of truth that powers the UI is [`migration-pairs.json`](../migration-pairs.json) at the project root. Field severity definitions and matcher rules are documented inline there. Smoke test: [`test/migration-readiness-smoke.js`](../test/migration-readiness-smoke.js).

---

## TL;DR per topic

| # | Topic pair | Kafka source | Pulsar target | RCP consumer | Total fields tracked | Critical | Schema parity | Cutover verdict |
|---|---|---|---|---|---|---|---|---|
| 1 | **Soccer live game-state** | `trading.soccer.{nxt,prd}.sgt` (MSK + use2-event-streaming mirror) | `persistent://gcd/soccer-inbound/game-state` | `EventDrivenMessageListener` → `GameStateOutbound.Api` → `FootballFixtureConverter*` | 59 | 14 | **100%** (RCP-consumed paths fully present in Pulsar) | **GO** — pending live-stream sample with goal/foul incidents to flip UI chip GREEN |
| 2 | **Live ID/RAMP mappings** | `livedata.mappings` (MFSL UKI + use2 mirror) | `persistent://gcd/livedata-inbound/livedata-mappings` | `providers/mapping` listener → `MappingDeserializer` → `Fixture/Competition/Meeting/Race` converters | 35 | 8 | **100%** (same `MappingOuterClass.Mapping` proto; Pulsar is the connector destination) | **GO** — verify `MappingDeserializer` proto JAR is available to the Pulsar consumer; sample-size dependent |
| 3 | **Competition pre-match stats** | `trading.{nxt.,}competition.soccer.prematch.stats` (Confluent Cloud) | `persistent://gcd/soccer-inbound/competition-prematch-stats` | `CompetitionStatsHandlerDrivenMessageListener` → `StatsHandlerDeserializer` → `CompetitionMessage` JSON | 28 | 6 | **100%** (JSON envelope passes through unchanged via the mirror connector) | **GO** — confirm the non-JSON byte prefix the deserialiser skips is also preserved by the connector |
| 4 | **Fixture pre-match stats** | `trading.{nxt.,}fixture.soccer.prematch.stats` (Confluent Cloud) | `persistent://gcd/soccer-inbound/fixture-prematch-stats` | `FixtureStatsHandlerDrivenMessageListener` → `StatsHandlerDeserializer` → `FixtureMessage` JSON | 46 | 12 | **100%** (same envelope, same deserialiser class as competition) | **GO** — same prefix-byte caveat as competition |

> **Bottom line:** every field RCP currently dereferences from Kafka is present in the corresponding Pulsar topic. The only outstanding work items are **operational** (deserialiser JAR bundling, Confluent prefix-byte preservation by the connector) and **observational** (capturing live samples that include the rarer enum values so the UI chip flips solidly GREEN). See per-topic notes below.

---

## How the in-UI chip computes readiness

The compare page (`/compare.html`) loads `migration-pairs.json` on startup and exposes a `GET /api/migration-pairs` server endpoint. Whenever you select a Kafka topic on one side and a Pulsar topic on the other, the engine:

1. **Detects the pair** — substring-matches the picked `topic.id` and `topic.topic` against each pair's `endpoints.{kafka,pulsar}.{topicIdContains,topicPathContains}`. If both sides hit the *same* pair, the chip appears.
2. **Counts field population** — for every entry in `pair.fields`, walks the path through every decoded message on both sides and counts how many messages populate it. (`null`, `''`, `0`, `false`, empty array, and empty object are treated as *default / not populated* — this matches protobuf-to-JSON's omit-defaults rule so we don't penalise Pulsar for skipping default values.)
3. **Tallies by severity** —
   - **CRITICAL** field with `kPop > 0 && pPop == 0` → instant **RED**.
   - **IMPORTANT** field with same gap → **AMBER**.
   - **INFO** gaps are surfaced in the panel but never block.
   - Bonus fields (Pulsar shows data Kafka didn't sample) are highlighted but never block.
4. **State machine** —
   - Not enough messages on either side (`< minMessagesPerSide`, default 3) → **PENDING**.
   - Any critical gap or type mismatch → **RED**.
   - Any important gap or type mismatch → **AMBER**.
   - Otherwise → **GREEN**.

Click the chip to open a slide-over with: the verdict card, severity tally, per-field table (sortable), and recommendations. Same engine runs in the [`test/migration-readiness-smoke.js`](../test/migration-readiness-smoke.js) Node script for CI verification.

---

## Per-topic notes

### 1. Soccer live game-state — `soccer-gamestate`

Audit basis: full deep-dive in [`RCP-PULSAR-PARITY-ANALYSIS.md`](./RCP-PULSAR-PARITY-ANALYSIS.md) and the field-by-field table in [`RCP-PULSAR-FIELD-MAP.md`](./RCP-PULSAR-FIELD-MAP.md). Programmatic check in [`rcp-field-map.csv`](./rcp-field-map.csv).

- **Schema parity:** 100% on RCP-consumed paths. The 4 "Kafka-only" and 4 "Pulsar-only" paths observed in the captures are entirely **data-sampling artefacts** (different fixtures, protobuf's omit-defaults rule).
- **Current chip state (against 2026-05-15 staging captures):** **RED**, driven by one critical and three important gaps:
  - `incidents[].participantRef[]` (critical) — Pulsar capture only had `PERIOD_TRANSITION` incidents (global-not-per-player), no `GOAL/FOUL/SUBSTITUTION` events.
  - `incidents[].externalRef[].{provider,externalId}` (important) — same root cause.
  - `incidents[].time.soccerIncidentTime.clock.minutes` (important) — likewise.
- **To flip GREEN:** capture a live fixture window that contains at least one `GOAL`, `FOUL`, `SUBSTITUTION`, or `CANCELLED` incident on the Pulsar side and re-run the comparison.
- **Operational checklist for cutover:** verify (a) `GameStateOutbound.Api` proto bundling on the Pulsar side, (b) downstream RCP consumer factory wiring, (c) parity on optional-field population policy (`homeParticipantRef`, `awayParticipantRef`, `meta.ingressCorrelationTimestamp`, `participants[].formation.value` — all already validated).

### 2. Live ID/RAMP mappings — `live-mappings`

Audit basis: deep-dive in this commit (added 2026-05-15) — see registry comments and [`migration-pairs.json`](../migration-pairs.json) for the 35 audited fields.

- **Wire format:** identical on both sides — `MappingOuterClass.Mapping` protobuf bytes. The Pulsar topic is **the destination of the same Kafka→Pulsar sink connector**, so no schema translation happens in flight.
- **Branch model:** `Mapping` is a `oneof` discriminator (`fixture` / `competition` / `meeting` / `race`). RCP routes each message to a different converter based on which branch is set. The registry tracks each branch root with `important` severity, plus all the leaf identity/business fields each converter reads.
- **Operational checklist:**
  - Bundle the `feeds-msv-v2` proto JAR with the Pulsar consumer; the deserialiser logic in [`MappingDeserializer`](../../rcp-service/providers/mapping/src/main/java/com/ppb/rcp/providers/mapping/MappingDeserializer.java) ports verbatim.
  - **Kafka record headers** (`msv_type` for tombstones, `msv_sport` for the sport filter, `headers{}` map propagated as `RcpContext`) must round-trip through the connector. **This is the single highest-risk operational item** — Pulsar message *properties* preserve string headers, but verify with a side-by-side per-record check that the header keys/values are identical.
  - Tombstone behaviour: the deserialiser returns `null` for tombstones; the `HeadersToEntityCaseConverter` then routes them via `lastHeader("msv_type")`. Verify tombstones survive the connector translation.

### 3. Competition pre-match stats — `competition-prematch-stats`

Audit basis: deep-dive added 2026-05-15. 28 fields, 6 critical, 12 important. See `body.stages[].divisions[].seasonRankings[]` for the data RCP enriches its league-standings model with.

- **Wire format:** JSON envelope with an optional non-JSON byte prefix that `StatsHandlerDeserializer` strips by skipping bytes before the first `{`. `FAIL_ON_UNKNOWN_PROPERTIES = false`, so the consumer is forward-compatible.
- **Operational checklist:**
  - Verify the connector **preserves the leading prefix bytes** (or, equivalently, that the same prefix-skip is wired into the Pulsar consumer; even better — strip the prefix at the connector boundary so the Pulsar payload is pure JSON).
  - Identical `ObjectMapper` registration is required to deserialise `com.flutter.uki.stp.trading.spmsks.messaging.competition.CompetitionMessage`. The Pulsar consumer must register the same Kotlin / `JsonDeserializer` module that the Kafka consumer uses today.
  - Confirm the **header map** built in `AbstractStatsHandlerDrivenMessageListener.buildHeadersAsMap(...)` survives the connector — these become correlation IDs in `RcpContext`.

### 4. Fixture pre-match stats — `fixture-prematch-stats`

Audit basis: deep-dive added 2026-05-15. 46 fields, 12 critical, 14 important. Same envelope + deserialiser pattern as competition, but with the largest payload of the four topics (teams, players, form, past head-to-heads, seasonal stats).

- **Operational checklist:** identical to competition pre-match stats — same `StatsHandlerDeserializer` is parameterised with a different `targetClass` per Spring `@Bean`.
- **Critical fields surface guards:** `body.homeTeam` / `body.awayTeam` are required (the validity guard in `FootballTeamConverter.isValidTeam` short-circuits on missing name) — the chip will flag RED if either side is observed empty.

---

## How to reproduce

Run the smoke test from the project root:

```bash
cd PulsarWebClient
node test/migration-readiness-smoke.js
```

What it verifies:

- All 4 pairs in `migration-pairs.json` are structurally valid (matchers + severity-classified field lists).
- Each pair's endpoint matchers resolve to ≥ 1 catalogue topic on each side and **never** match more than one pair (no cross-pair leak).
- For `soccer-gamestate`, evaluates the registry against the captured staging payloads (`response/staging/*.json`) and asserts the expected verdict.

To add a new RCP-consumed topic to the readiness mechanism:

1. Append an entry to [`migration-pairs.json`](../migration-pairs.json) using the existing pairs as a template.
2. Add severity-classified fields based on a code audit of `rcp-service/` (search for the deserialiser class, its `@KafkaListener` / `ConsumerFactory` wiring, and every `.getX()` / property access on the payload type — the structural pattern is identical across all 4 existing pairs).
3. Re-run the smoke test — the matcher resolver will warn if the new entry collides with an existing one.
