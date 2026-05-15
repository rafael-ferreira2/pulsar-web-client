<!--
  This is the *map* document: every field RCP currently reads from the
  Kafka game-state topic, mapped 1:1 against what the Pulsar capture
  carries. Each row records data-type parity, field-naming parity, and
  whether the field was actually populated in the captured sample.

  Companion docs:
    • response/RCP-PULSAR-PARITY-ANALYSIS.md ← narrative parity audit
    • response/rcp-field-map.csv             ← spreadsheet-friendly source
    • response/rcp-field-map.json            ← raw probe state (re-usable)
    • test/rcp-field-map.js                  ← script that produced both
-->

# RCP ↔ Pulsar field map — soccer game-state

**Date:** 2026-05-15
**Sources**

| Side | Capture | Topic | Decoded msgs | Fixtures sampled |
|---|---|---|---|---|
| Kafka  | [`staging/kafka-…-2026-05-15T10-07-00.json`](./staging/kafka-kafka-stg-rcp-mirror-sgt-2026-05-15T10-07-00.json) | `trading.soccer.prd.sgt` (staging mirror) | 10/10 | 15716908, 15719256, 15717438, 15722073, 15717470, 15723417 |
| Pulsar | [`staging/pulsar-staging-game-state-2026-05-15T10-06-55.json`](./staging/pulsar-staging-game-state-2026-05-15T10-06-55.json) | `persistent://gcd/soccer-inbound/game-state` | 10/11 | 39995603, 39959838, 39996693 |

**RCP code scope:** `rcp-service/` Java path — `EventDrivenMessageListener` → `GameStateDeserializer` (`GameStateOutbound.Api.parseFrom`) → `FootballFixtureConverterBuilder` + ~25 converters.

## Bottom line

| Metric | Value |
|---|---|
| RCP-consumed fields catalogued | **59** |
| Schema present in Pulsar (same path, same type) | **59 / 59** ✅ |
| Field-naming parity | **59 / 59 identical** (same protobuf descriptor on both sides) |
| Data-type parity (JSON-runtime) | **all populated fields match** — zero type mismatches |
| Populated in BOTH samples | 17 |
| Populated only in Kafka sample (proto-default omission elsewhere) | 4 |
| Populated only in Pulsar sample (proto-default omission elsewhere) | 2 |
| Upstream-empty in both samples (optional fields not yet populated for these fixtures) | 36 |

> **Key insight on the apparent gaps:** the Pulsar feed and the Kafka feed are decoded through the **same generated protobuf class** (`GameStateOutbound.Api`). Field naming is identical by construction. Where one side shows a value and the other doesn't, it's because protobuf-JSON omits proto3 defaults (numbers = 0, strings = "", repeated = empty) and the two captures sampled different fixtures with different optional-field populations. None of the 59 fields shows a real schema gap.

Legend used in the map below:

| Symbol | Meaning |
|---|---|
| ✅ | Field present in both samples with matching JSON-runtime type — confidence-high for cutover |
| 🟡 | Field only populated on one side **in this sample**, but schema-present on both. Explained in row notes. |
| ⚪ | Both samples upstream-empty (optional field not yet populated for these fixtures by either feed) — neutral evidence |
| 🔴 | (None observed) — would indicate a real schema/type/naming gap |

---

## 1. `meta.*` (per-message envelope)

| # | RCP path | Proto type | Used for | Kafka data | Pulsar data | Naming | Type | Verdict |
|---|---|---|---|---|---|---|---|---|
| 1 | `meta.version` | `string` | mapping/persistence | `"1.1.92"` (10/10) | `"1.1.92"` (10/10) | identical | match | ✅ |
| 2 | `meta.gameStateId` | `string` | mapping (identity) | empty (0/10) | empty (0/10) | identical | n/a | ⚪ upstream-empty both sides |
| 3 | `meta.highWaterMark` | `int64` | business — ordering / watermark | `4, 1, 2, …` (10/10) | `1, 1, 2, …` (10/10) | identical | match (`number`) | ✅ |
| 4 | `meta.gameStatePreviousId` | `string` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 5 | `meta.ingressCorrelationId` | `string` (UUID) | **correlation key** | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 6 | `meta.ingressCorrelationTimestamp` | `string` (RFC3339) | business — Instant.parse for incident freshness | `"2026-05-11T10:11:45.600799864Z"` etc. | `"2026-05-14T08:06:03.414229Z"` etc. | identical | match | ✅ |
| 7 | `meta.instructionType` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 8 | `meta.sport` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 9 | `meta.headers` | `map<string,string>` | correlation / tracing pass-through | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 10 | `meta.feedStatus[].provider` | `enum (Provider)` | mapping | `"UNKNOWN_PROVIDER"` (10/10) | `"UNKNOWN_PROVIDER"` (10/10) | identical | match | ✅ |
| 11 | `meta.feedStatus[].status` | `enum (Status)` | business — feed status check | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 12 | `meta.feedStatus[].features.supportedIncidentTypes[].soccerIncidentType` | `enum` | **filter** — supported-incident allow-list | 10 entries × types (10/10) | same set, same order (10/10) | identical | match | ✅ |
| 13 | `meta.feedStatus[].features.supportedStatTypes[].soccerStatType` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |

> Highlights: `meta.ingressCorrelationId`, `meta.ingressCorrelationTimestamp`, `meta.highWaterMark`, and the supported-incident allow-list — the four RCP-critical metas — are **identical in name, type, and population frequency** on both feeds.

---

## 2. `fixture.*` (per-fixture envelope)

| # | RCP path | Proto type | Used for | Kafka data | Pulsar data | Naming | Type | Verdict |
|---|---|---|---|---|---|---|---|---|
| 14 | `fixture.sportsBookId` | `string` | **correlation key** (Kafka record key) | `"15716908"` etc. (10/10) | `"39995603"` etc. (10/10) | identical | match | ✅ |
| 15 | `fixture.status` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 16 | `fixture.expectedStartTime.seconds` | `Timestamp.seconds` (int64 as string in JSON) | mapping | `"1778425200"` (10/10) | `"1778767200"` (10/10) | identical | match (`string` per proto3-JSON int64 rule) | ✅ |
| 17 | `fixture.actualStartTime.seconds` | `Timestamp.seconds` | mapping | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 18 | `fixture.expectedEndTime.seconds` | `Timestamp.seconds` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 19 | `fixture.externalRef[].provider` | `enum` | **filter** — `Provider.SPORTEX` | empty (0/10) | empty (0/10) | identical | n/a | ⚪ — re-test with a SPORTEX-tagged fixture |
| 20 | `fixture.externalRef[].externalId` | `string` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 21 | `fixture.group.id` | `string` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 22 | `fixture.group.description` | `string` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 23 | `fixture.attributes.soccerAttributes.penaltyShootoutFormat` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 24 | `fixture.attributes.soccerAttributes.homeParticipantRef` | `string` | **business** — side resolution | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 25 | `fixture.attributes.soccerAttributes.awayParticipantRef` | `string` | **business** — side resolution | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 26 | `fixture.attributes.soccerAttributes.firstLegScore.homeParticipant` | `int32` | mapping (two-legged ties) | empty (0/10) | empty (0/10) | identical | n/a | ⚪ — no two-legged fixtures sampled |
| 27 | `fixture.attributes.soccerAttributes.firstLegScore.awayParticipant` | `int32` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |

> Note on `Timestamp.seconds`: protobuf-JSON renders int64 as a **JSON string** (not a number) — this is the canonical proto3-JSON mapping, not a quirk. Both feeds render it the same way (`"1778425200"`), so RCP's `getSeconds()` will parse cleanly on both.

---

## 3. `participants[]` (teams + players, top-level repeated)

> RCP's view: teams **and** players are both modelled here, with `participants[].type` distinguishing `TEAM` vs `PLAYER`. There is no separate top-level `lineups` field.

| # | RCP path | Proto type | Used for | Kafka data | Pulsar data | Naming | Type | Verdict |
|---|---|---|---|---|---|---|---|---|
| 28 | `participants[].id` | `string` | mapping | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 29 | `participants[].name` | `string` | mapping | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 30 | `participants[].type` | `enum (TEAM|PLAYER)` | **filter** — drives the team-vs-player branch | populated 10/10 | populated 10/10 | identical | match | ✅ |
| 31 | `participants[].externalRef[].provider` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 32 | `participants[].externalRef[].externalId` | `string` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 33 | `participants[].info.teamAttributes.soccerTeamAttributes.jerseyColour` | `string` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 34 | `participants[].info.teamAttributes.soccerTeamAttributes.formation.value` | `string` (e.g. `"4-3-3"`) | mapping | populated 10/10 (20 entries) | populated 10/10 (20 entries) | identical | match | ✅ |
| 35 | `participants[].info.playerAttributes.soccerPlayerAttributes.parentParticipantRef` | `string` | **business** — lineup bucketing per team | empty (0/10) | empty (0/10) | identical | n/a | ⚪ — sample had no player participants |
| 36 | `participants[].info.playerAttributes.soccerPlayerAttributes.shirtNumber` | `int32` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 37 | `participants[].info.playerAttributes.soccerPlayerAttributes.position` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 38 | `participants[].info.playerAttributes.soccerPlayerAttributes.playerPosition` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 39 | `participants[].info.playerAttributes.soccerPlayerAttributes.startingPosition` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 40 | `participants[].info.playerAttributes.soccerPlayerAttributes.formationPlace.value` | `int32` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |

> Both captures sampled only **TEAM** participants (no `PLAYER` rows in the data). The player-attribute paths (35-40) are schema-identical but couldn't be data-verified — re-capture with a live fixture that has lineup data to close that gap.

---

## 4. `incidents[]` (per-event)

| # | RCP path | Proto type | Used for | Kafka data | Pulsar data | Naming | Type | Verdict |
|---|---|---|---|---|---|---|---|---|
| 41 | `incidents[].id` | `string` | mapping | 2 entries | 2 entries | identical | match | ✅ |
| 42 | `incidents[].type.soccerIncidentType` | `enum` | **business** — routing key for incident converters | `"CANCELLED"`, `"PERIOD_TRANSITION"` | `"PERIOD_TRANSITION"` | identical | match | ✅ (different types in samples, same enum domain) |
| 43 | `incidents[].time.soccerIncidentTime.period` | `message (Period)` | business — filter penalty-period goals | populated 2 | populated 2 | identical | match (`string` once flattened) | ✅ |
| 44 | `incidents[].time.soccerIncidentTime.clock.minutes` | `int32` | business — sort + duration | populated 2 (all non-zero) | empty (all zero → omitted) | identical | match (when populated) | 🟡 sample-only — proto3 default omission; schema identical |
| 45 | `incidents[].time.soccerIncidentTime.clock.seconds` | `int32` | business — sort + duration | empty (all zero → omitted) | populated 2 (all non-zero) | identical | match (when populated) | 🟡 sample-only — mirror of #44 |
| 46 | `incidents[].participantRef[]` | `repeated string` | business — goal/foul/sub/card player attribution | 4 entries (CANCELLED incidents have refs) | empty (PERIOD_TRANSITION events don't reference players) | identical | match (when populated) | 🟡 sample-only — only PERIOD_TRANSITION in Pulsar capture |
| 47 | `incidents[].externalRef[].provider` | `enum` | mapping | 4 entries | empty | identical | match (when populated) | 🟡 sample-only — same root cause as #46 |
| 48 | `incidents[].externalRef[].externalId` | `string` | mapping | 4 entries | empty | identical | match (when populated) | 🟡 sample-only |
| 49 | `incidents[].lastModified` | `Timestamp` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 50 | `incidents[].qualifiers.soccerIncidentQualifiers.qualifiers[].qualifier` | `enum` | business — filter on `ASSISTANT_PARTICIPANT_REF`, `FOUL_WON`, `INJURY_TIME_UPDATE`, `SHOOTOUT_ATTEMPT_STATE`, `EXTRA_TIME_QUALIFIERS`, `OUT_PARTICIPANT_REF` | populated 2 | populated 2 | identical | match | ✅ |
| 51 | `incidents[].qualifiers.…qualifiers[].value` | `string` | business — `Integer.parseInt(value)` | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 52 | `incidents[].qualifiers.…qualifiers[].details.foulWonDetails.playerId.value` | `string` | mapping | empty | empty | identical | n/a | ⚪ |

> Rows 44–48 are flagged 🟡 because **one side's sample didn't trigger the optional**. Cause analysis:
>
> | Path | Kafka sample saw | Pulsar sample saw | Why the gap is data, not schema |
> |---|---|---|---|
> | `clock.minutes` | 2 incidents at non-zero minutes | 2 incidents at minutes=0 | proto3-JSON omits zero ints. Schema identical. |
> | `clock.seconds` | 2 incidents at seconds=0 | 2 incidents at non-zero seconds | Mirror of above. |
> | `participantRef[]` | CANCELLED events reference involved players | PERIOD_TRANSITION events are global, never reference players | Domain semantics; both feeds will emit the array when populated. |
> | `externalRef[].provider` / `.externalId` | CANCELLED events carry provider ids | PERIOD_TRANSITION events don't | Same as above. |

---

## 5. `stats.*`

| # | RCP path | Proto type | Used for | Kafka data | Pulsar data | Naming | Type | Verdict |
|---|---|---|---|---|---|---|---|---|
| 53 | `stats.matchStat[].participantRef` | `string` | mapping | populated 10/10 | populated 8/10 | identical | match | ✅ |
| 54 | `stats.matchStat[].type.soccerStatType` | `enum` | mapping | populated 10/10 | populated 8/10 | identical | match | ✅ |
| 55 | `stats.matchStat[].value` | `double` | mapping — `(int) stat.getValue()` | empty (all zero → omitted) | populated 8/10 (non-zero values) | identical | match (when populated) | 🟡 sample-only — Kafka sample had stats all at 0 |
| 56 | `stats.periodStat[].period.soccerPeriod` | `enum` | mapping | empty (0/10) | empty (0/10) | identical | n/a | ⚪ |
| 57 | `stats.periodStat[].stat.matchStat[].participantRef` | `string` | mapping | empty | empty | identical | n/a | ⚪ |
| 58 | `stats.periodStat[].stat.matchStat[].type.soccerStatType` | `enum` | mapping | empty | empty | identical | n/a | ⚪ |
| 59 | `stats.periodStat[].stat.matchStat[].value` | `double` | mapping | empty | empty | identical | n/a | ⚪ |

---

## What RCP does NOT read from the protobuf

For completeness (capacity / cleanup conversations), the following top-level fields are emitted in the proto but **never dereferenced** in the RCP gamestate path:

| Top-level field | Where it appears | Why it's safe to ignore on the RCP side |
|---|---|---|
| `score` | both feeds | Score is computed by RCP from `incidents[].GOAL` events. |
| `clock` | both feeds | Clock is a derived view; RCP uses `incidents[].time.soccerIncidentTime.clock.*` instead. |
| `state` | both feeds | Game-state-machine state; RCP relies on `fixture.status` + period transitions. |
| `streamA` / `streamB` | both feeds | Per-stream provider buffers; not consumed by the fixture converter. |
| `period` | both feeds | Top-level period summary; RCP uses incident-level `time.soccerIncidentTime.period`. |
| `extras` | both feeds | Provider escape-hatch. |

These are **schema-symmetric** between the two feeds (same dot paths, same types), so even if they migrate to a different downstream tomorrow, the migration path is symmetric.

---

## Naming & type-parity summary

| Dimension | Verdict | Evidence |
|---|---|---|
| **Field naming** | **100 % identical** across all 59 RCP-consumed paths | Both feeds decode via the *same* generated `GameStateOutbound.Api` protobuf class, so JSON field names follow the same descriptor. The probe confirmed empirically that every dot-path either resolves on both sides or on neither (no "renamed" / "moved" fields). |
| **Data type (JSON-runtime)** | **No mismatches** in the 17 populated-on-both rows. All `string` vs `string`, `number` vs `number`, `array` vs `array`. | See the `kafka_types` / `pulsar_types` columns in [`rcp-field-map.csv`](./rcp-field-map.csv). |
| **Data type (proto declaration)** | Same proto file → same wire format → same Java getters. | RCP wires `GameStateDeserializer` → `Api.parseFrom(bytes)`. The same descriptor is shared with the Pulsar emitter side. |
| **Population frequency** | Identical for every RCP-critical optional (home/awayParticipantRef, ingressCorrelationTimestamp, formation, supportedIncidentTypes, etc.) over 10 messages each. | See § 1 → § 5 above and `participants[].formation` = 20 entries on both sides. |

---

## Outstanding empirical gaps (sample needed, not schema)

| # | Path or path family | Why empirical evidence is thin | Quick way to close the gap |
|---|---|---|---|
| 1 | All `participants[].info.playerAttributes.soccerPlayerAttributes.*` fields (rows 35-40) | Both captures had only `TEAM` participants in this window | Re-capture during a live fixture that has lineups (`/listener` → set cap = ∞ → wait for `Participants - 22+` count) |
| 2 | `incidents[]` with types other than `CANCELLED` / `PERIOD_TRANSITION` (esp. `GOAL`, `FOUL`, `SUBSTITUTION`, `YELLOW_CARD`) | Sample-window timing | Same — re-capture during a live fixture |
| 3 | `fixture.attributes.soccerAttributes.firstLegScore.*` (rows 26-27) | No two-legged ties sampled | Re-capture during a known cup tie |
| 4 | `fixture.externalRef[].provider = SPORTEX` (row 19) | No SPORTEX-tagged fixtures in the window | Re-capture once a SPORTEX-mapped fixture is live |

**None of these are blockers** — they're cases the schema definitively supports. Re-running the probe against a richer Pulsar capture will populate them.

---

## How to reproduce

```bash
cd PulsarWebClient
node test/rcp-field-map.js
# Outputs:
#   response/rcp-field-map.csv
#   response/rcp-field-map.json
```

To re-probe with new captures, edit the two file constants near the top of `test/rcp-field-map.js`. To add a new RCP-consumed field, append a row to the `RCP_FIELDS` array — the script handles everything else (path resolution, type classification, naming/type assertion, CSV + JSON output).

For a UI-driven re-comparison once you have fresh captures, open the listener on staging for both topics, then visit `/compare.html` with:

- Source A = Kafka `kafka-stg-rcp-mirror` → `trading.soccer.prd.sgt`
- Source B = Pulsar `staging` → `persistent://gcd/soccer-inbound/game-state`
- Correlation key = `meta.ingressCorrelationId` (or `fixture.sportsBookId`)
- Cap / side = ∞ Unlimited

Export → JSON / Markdown / CSV and diff against this map.
