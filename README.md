# FanDuel · GCD Streaming Client (Pulsar + Kafka)

> **Internal FanDuel developer tool.** Brand assets (FanDuel shield + wordmark, FanDuel-blue palette `#1493FF`, Inter typeface) live in `public/fanduel-*.png` and `public/shared.css`. Not for distribution outside the company.

A local web tool for **consuming**, **inspecting**, **comparing**, and **exporting** messages from the FanDuel GCD topics — across **Apache Pulsar** (StreamNative, JWT) and **Apache Kafka** (AWS MSK, MFSL, Confluent Cloud, and the RCP-mirrored US clusters). Staging and production clusters are pre-configured to match the GCD city map and the deployed `sb-rcpfd-infra` Ansible vars.

The UI runs at <http://localhost:3456> and has two pages, both themed with the FanDuel brand (electric-blue accents on a dark dev-friendly canvas, Inter font, FanDuel shield favicon):

- **`/`** — single-topic listener. Pick a bus → environment → topic, subscribe, see decoded messages live, export them.
- **`/compare.html`** — open **two consumers in parallel** (any combination of Pulsar/Kafka, same or different envs) and explore the differences across four views: structure-and-values diff, matched pairs, **VSCode-style side-by-side line diff**, and raw stream.

> Designed for diagnostics, not production traffic. No data is persisted server-side.

---

## At a glance

| Capability | Where |
|---|---|
| Live consume from Pulsar (TLS + JWT) | listener page, `Pulsar` mode |
| Live consume from Kafka (MSK / MFSL plaintext, Confluent SASL_SSL) | listener page, `Kafka` mode |
| Auto-decode soccer gamestate **protobuf** (`GameStateOutbound.Api`) | both pages |
| Auto-decode prematch stats **JSON** (matches RCP's `StatsHandlerDeserializer`) | both pages |
| Connectivity probe (`nc -zv`) | "Connectivity" button on the listener page |
| Publish a message to a Pulsar topic | listener page, `Pulsar` mode only |
| One-click JSON export (listener); **JSON (3 files) / CSV / Markdown-report** export menu (compare) | both pages |
| Structure vs Values diff (`📐 Structure` / `⚖ Values` / `🔍 Both`) with schema match score | compare page, **Latest snapshot diff** tab |
| Matched-pair diff by correlation key (`meta.gameStateId`, etc.) | compare page, **Matched pairs** tab |
| **VSCode-style side-by-side line diff** with unified-diff clipboard export | compare page, **Side-by-side line diff** tab |

---

## Prerequisites (macOS)

- **Homebrew** — <https://brew.sh>
- **Node.js v18+** — `brew install node`
- **Apache Pulsar CLI** (for Pulsar mode only) — `brew install apache-pulsar`. Verify with `pulsar-client --version`.
- **Java** (transitively used by `pulsar-client`) — `brew install openjdk` if you see Java errors.
- **`nc`** (netcat) — preinstalled on macOS; used for connectivity probes.

The Kafka side runs entirely in Node via `kafkajs` — no Kafka CLI required.

---

## Setup

### 1) Install Node deps

```sh
npm install
```

Installs `express`, `ws`, `protobufjs`, and `kafkajs`.

### 2) Configure credentials

```sh
cp tokens.sh.example tokens.sh
# then edit tokens.sh and fill in what you have
```

**Pulsar (StreamNative JWTs)** — required for any Pulsar topic you want to read:

```sh
export PULSAR_STG_GAMESTATE="…"
export PULSAR_STG_MAPPINGS="…"
export PULSAR_STG_COMP_STATS="…"
export PULSAR_STG_FIXTURE_STATS="…"

export PULSAR_PRD_GAMESTATE="…"
export PULSAR_PRD_MAPPINGS="…"
export PULSAR_PRD_COMP_STATS="…"
export PULSAR_PRD_FIXTURE_STATS="…"
```

**Kafka — Confluent Cloud only**. MSK, MFSL, and RCP-mirrored clusters are PLAINTEXT and need **no env vars**. Confluent prematch needs an **API key + secret**:

```sh
export KAFKA_STG_CONFLUENT_API_KEY="…"
export KAFKA_STG_CONFLUENT_API_SECRET="…"
export KAFKA_PRD_CONFLUENT_API_KEY="…"
export KAFKA_PRD_CONFLUENT_API_SECRET="…"
```

`tokens.sh` is gitignored — **never commit secrets**.

### 3) Run

```sh
source tokens.sh && npm start
```

Open <http://localhost:3456>. You should see `✓ Proto definitions loaded (gamestate_api)` in the server log — that confirms protobuf decoding is wired for soccer gamestate on both buses.

> **Network reminder**: MSK and MFSL clusters typically require the corporate / UKI VPN. RCP-mirrored US clusters (`*.fndlsb.net`) may require a different network reach. Confluent Cloud is public and just needs the API keys.

---

## Listener page (`/`)

Pick **Pulsar** or **Kafka** in the sidebar bus switch, then an environment and a topic.

### Sidebar — filter, sort, collapse, identify

- The bus switch at the top is a Pulsar / Kafka segmented control. Topic items below are tagged with the matching bus icon (lightning bolt for Pulsar, stylized "K" for Kafka), so you can tell at a glance which bus a topic belongs to.
- **Environment filter chips** — two pill checkboxes (`STG` amber, `PRD` green) with live counts let you focus the list. Toggle either off to hide all envs of that type. Especially useful on Kafka, where you have ~8 environments (MSK STG/PRD, MFSL STG/PRD, RCP-mirror STG/PRD, Confluent STG/PRD): flip PRD off while you investigate staging, and the list halves. Toggling both off shows a friendly hint instead of nothing.
- **Sort topics** dropdown — `default | A→Z | Z→A`. Sorts topics *within* each visible environment by display label. Environment order itself always follows `config.json` / `kafka-config.json` order, so related clusters stay grouped (e.g. STG MSK, STG MFSL, STG RCP-mirror, STG Confluent appear in the canonical pipeline order).
- **Collapse** — click any environment header (or the `↕ All` button on the right) to collapse / expand. Per-bus collapse state is persisted, so closing all PRD envs while you investigate STG sticks across page reloads.
- All preferences — filter chips, topic sort, collapse state — persist in `localStorage` (`gcdEnvFilters`, `gcdSidebarSort`, `gcdCollapsedEnvs`).
- Each topic row shows: bus icon · credential dot (green = creds loaded, red = missing) · label · live indicator while consuming.

### Main panel — Details tab vs. Live feed tab

Once you pick a topic, the main area shows two tabs:

- **Details** *(default)* — everything you need to understand the topic before listening:
  - **Header** with the topic label, bus tag (Pulsar / Kafka), STG/PRD badge, and the full topic path.
  - **About** — the topic description from `config.json` / `kafka-config.json`, with inline `code` formatting preserved.
  - **Connection** — bus, environment, broker URL, topic path, key field, decoder (e.g. `Protobuf · gamestate_api.proto · GameStateOutbound.Api` or `JSON (Stats deserializer …)`), credential status pill, and a `Pulsar mirror` row for Kafka topics that mirror from Pulsar (or vice versa).
  - **Quick actions** — `Listen` (switches to Live feed and starts consuming), `Publish` (Pulsar only), `Test connectivity` (TCP probe), `Export captured` (JSON dump of what you've captured so far), and `Compare with another topic` (deep-links to `/compare.html` with the current topic pre-loaded as **Source A**).
  - **Response data structure** — a live, type-only tree derived from the most recent decoded message: keys keep their names, values are replaced with their runtime type (`"string"`, `"number"`, `[…]`, etc.). It updates as new messages arrive while you're on this tab. If nothing has been received yet, it prompts you to press Listen.
- **Live feed** — the streaming console, exactly as before. Use the `Listen` button in the controls bar (or in Details → Quick actions) to start; messages decode as protobuf or JSON depending on the topic's configured decoder, with a `proto` / `json` badge on each bubble.

The standalone `⬇ Export` button (top-right of the tab bar) and the controls bar (`▶ Listen`, `✉ Publish`, `🗑 Clear`) remain available regardless of which tab you're on.

### Pulsar

1. Bus → **Pulsar**, then env (`Staging` / `Production`) and topic (`Soccer live gamestate`, `Live mappings`, `Competition pre-match stats`, `Fixture pre-match stats`).
2. Enter a **Subscription name** (e.g. `local-yourname-test`), pick a **Subscription type** (Shared / Exclusive / Failover / Key_Shared), **Start position** (Latest / Earliest), and **message count** (or ∞).
3. **▶ Listen**. The feed shows decoded messages — gamestate is rendered as pretty-printed JSON courtesy of the bundled `.proto` files.
4. **🔌 Connectivity** runs `nc -zv broker port` for the current cluster.
5. **✉ Publish** opens a panel to send a one-off message to the current topic (Pulsar only; the server uses `pulsar-client produce`).
6. **⬇ Export** dumps the current feed to a JSON file (`pulsar-<env>-<topic>-<timestamp>.json`).

### Kafka

1. Bus → **Kafka**, then cluster (MSK / MFSL / Confluent / RCP-mirrored) and topic.
2. Enter a **Consumer group** id (think of it like a Pulsar subscription name — unique per user is fine, e.g. `local-yourname-compare`).
3. **Start position** Latest / Earliest, message count (or ∞).
4. **▶ Listen** / **⏹ Stop**, **🔌 Connectivity** (TCP check on the first `host:port` from the bootstrap list), **⬇ Export** JSON. Publish is intentionally disabled.

Sidebar dots indicate credential state per topic:

- **Green** — credentials loaded for this env/topic (Pulsar JWT exported, or Kafka cluster requires none / Confluent key is set).
- **Red** — credentials missing. For Pulsar, the missing env var is shown in the tooltip; for Confluent, both `KAFKA_<env>_CONFLUENT_API_KEY` / `…_SECRET` must be set.

---

## Compare page (`/compare.html`)

The compare page opens **two independent WebSocket consumers** — one per side — and renders four views over the streams. The single-topic listener and the compare page can run side-by-side in different browser tabs without interfering.

Open it from the sidebar's **⇄ Compare topics** link or go directly to <http://localhost:3456/compare.html>.

### Picking sources

Each side (A and B) has its own pickers: **Bus**, **Env**, **Topic**, **Sub / group**, **Start position**. The defaults auto-pick Pulsar STG soccer gamestate for A and Kafka STG MSK soccer gamestate for B — the canonical "is the Pulsar mirror equal to the Kafka source?" question.

Above the views:

- **Correlation key (dot path)** — empty for auto (tries `meta.gameStateId`, `meta.gameStateMatchId`, `fixture.fixtureId`, `fixtureId`, `gameStateId`, `matchId`, `id` in order). Override with anything like `fixture.fixtureId`.
- **Cap / side** — how many messages each consumer takes before stopping (default 50, `∞` available).
- **▶ Start comparison** / **⏹ Stop** / **↻ Reset** / **⬇ Export**.

A live strip below shows: counts per side (with a blinking dot while consuming), matched-pair count, active correlation key, and last-update timestamp.

### Tab 1 — Latest snapshot diff (clean structured comparison)

A two-step model for "are these two messages the same?":

1. **📐 Structure** *(default)* — compares **field names and types only**. Array indices are normalized to `[*]` so two arrays of different lengths don't pollute the diff. Row types:
   - `OK` — path on both sides with the same type.
   - `Type ≠` — same path, different runtime types (e.g. `string` vs `number`).
   - `A only` / `B only` — path missing on the other side.
   - Cell values render as **type pills** (`string`, `number`, `boolean`, `object`, `array`, `null`).

2. **⚖ Values** — only paths present on both sides; compares actual values. Row types: `Equal` / `Differs`. Array indices preserved so you can see element-level drift.

3. **🔍 Both** — combined table for power use. All five row kinds: equal · differs · type ≠ · A only · B only.

**Schema match score** card stays visible above the snapshots — a percentage with a colored bar (green ≥ 95%, amber ≥ 75%, red below), plus a count line like *"152 matching paths · 4 type ≠ · 19 only in A · 0 only in B."*. Computed from the Structure diff so it doesn't fluctuate with timestamps.

Filter chips at the bottom (per mode):
- **Structure**: `Type mismatches`, `A only`, `B only`, `Matching`.
- **Values**: `Different values`, `Identical values`.
- **Both**: all five.

### Tab 2 — Matched pairs

Messages are grouped by the correlation key value. A "pair" appears when both sides have received a message sharing the same key. Each card shows the **schema match %** for the pair and the same mode-aware summary pills, then expands to the field table on demand. Most recent on top.

Useful for "for fixture X across N updates, what consistently differs?".

### Tab 3 — Side-by-side line diff (VSCode-style)

Pick any specific message from A and any from B, and see them line-by-line. Implementation is a proper LCS (longest common subsequence) line diff with adjacent del+add pairing — the same algorithm VSCode uses for its compare view.

- **White / no highlight** — line identical on both sides.
- **Red on left + green on right** — modified line (aligned at the same row).
- **Red only / green only** — line exists on only one side; the empty side shows a striped placeholder so the row alignment is obvious.
- **Numbered gutters** for both sides, scroll-synced because they share one grid.

Three ways to pick which two messages to compare:

1. **Manual** — the `Message A` and `Message B` dropdowns list every message received on that side, labelled like `#3 · 10:30:15 · meta.gameStateId=abc-123`.
2. **By matched key** — the third dropdown lists every correlation-key value where both sides have data. Pick a key and the two side pickers snap to that pair automatically.
3. **Live** — leave both on `▶ Latest message (live)` and the diff updates in real time as messages stream in.

**📋 Copy unified** writes a git-style unified diff to the clipboard, ready to paste into Slack / Jira / a PR. Messages over 2500 lines fall back to a naive zip diff with a "truncated" warning to keep the browser responsive.

### Tab 4 — Stream

Raw per-side decoded message stream — proto for gamestate, JSON for prematch, latin1/hex fallback otherwise. Useful for sanity-checking what's actually arriving before reading too much into the diffs.

### Export

The **⬇ Export ▾** button opens a dropdown with three formats. All three reflect the same underlying comparison state at the moment you click — they just package it differently for different audiences.

#### 1. JSON — three self-contained, topic-named files

A single click on **JSON** triggers **three** sequential downloads. The side files carry the actual topic name so the Downloads folder stays self-documenting; the comparison artefact uses a stable, short name so you can always find it. The browser shows a one-time "this site wants to download multiple files" consent banner on first use; accept it and future exports go through silently.

Filenames:

```
<topicLeafA>Response.json
<topicLeafB>Response.json
comparison.json
```

Where `<topicLeaf>` is the last meaningful segment of the topic name (Pulsar's `persistent://gcd/soccer-inbound/game-state` → `game-state`; Kafka's dotted `trading.soccer.nxt.sgt` stays as-is since it has no `/` segments). On the rare collision where both topics share the same leaf, the duplicate side gets a `<bus>-` prefix so the two downloads never overwrite each other.

Concrete example — comparing the Pulsar gamestate mirror against the upstream Kafka topic produces:

```
game-stateResponse.json
trading.soccer.nxt.sgtResponse.json
comparison.json
```

What each file contains:

| File | What's inside | Use it when… |
|---|---|---|
| **`<topicLeafA>Response.json`** | Everything about Source A: identity (`source`), `messagesReceived`, `exportedAt`, the latest decoded message (`latest`), and the full session receive buffer (`stream`). | You only care about one side — replaying, schema-extracting, or feeding A's payloads into a downstream tool. |
| **`<topicLeafB>Response.json`** | Same shape, for Source B. | Same as above, but for the other side. |
| **`comparison.json`** | The cross-cutting analysis: `meta` (timestamp, correlation key, schema match %, both source identities, message counts), `diff` (structure / values / both for the latest snapshot), and `pairs` (each matched pair with its per-pair diff). Deliberately omits the raw streams — they already live in the two response files. | Sharing the *comparison result* with a reviewer or attaching to a ticket without dragging along the raw messages. |

If a side has no topic selected when you export, its filename falls back to `source-aResponse.json` / `source-bResponse.json` so the download still succeeds.

Per-file shapes:

```jsonc
// <topicLeafA>Response.json  (and analogous <topicLeafB>Response.json)
{
  "source":           { "bus", "env", "topic", "description" },
  "messagesReceived": 12,
  "exportedAt":       "2026-05-14T13:00:00Z",
  "latest":           { /* decoded latest message */ },
  "stream":           [ /* every message seen this session */ ]
}

// comparison.json
{
  "meta": { "exportedAt", "correlationKey", "schemaMatchPct", "sourceA", "sourceB", "counts" },
  "diff": { "structure": {…}, "values": {…}, "both": {…} },
  "pairs": [ { "key", "a": {ts, content}, "b": {ts, content}, "diff": {…} }, … ]
}
```

Best for: re-processing in scripts, attaching to JIRA, replaying messages, or feeding into another diff tool without dragging along everything else.

#### 2. CSV (diff table)

Flat, sectioned tabular export — one row per field path, ready to open in Excel / Google Sheets / Numbers. Filename: `…-<ts>.csv`.

The four data columns (`Type [A]`, `Type [B]`, `Value [A]`, `Value [B]`) **carry the actual topic short-name in the header** — so you never have to guess "is column E the Pulsar or the Kafka side?". E.g. `Type [game-state]`, `Value [trading.soccer.nxt.sgt]`. Long names are capped at 40 chars; the full topic name still sits at the top of `META`.

**9 columns:**

| Section | Status | Severity | Path | Type [\<topicA\>] | Type [\<topicB\>] | Value [\<topicA\>] | Value [\<topicB\>] | Pair Key |
|---|---|---:|---|---|---|---|---|---|
| `META` | `SOURCE_A_TOPIC` |  |  |  |  | `persistent://gcd/.../game-state` |  |  |
| `META` | `SOURCE_B_TOPIC` |  |  |  |  |  | `trading.soccer.nxt.sgt` |  |
| `META` | `EXPORTED_AT` |  |  |  |  | `2026-05-14T13:00:00Z` |  |  |
| `SUMMARY` | `TYPE_MISMATCH` | `3` |  |  |  | `1` |  |  |
| `SUMMARY` | `MATCH` | `0` |  |  |  | `128` |  |  |
| `FIELD_MAPPING` | `TYPE_MISMATCH` | `3` | `meta.fixtureId` | `string` | `number` |  |  |  |
| `FIELD_MAPPING` | `ONLY_IN_B` | `2` | `kafkaOffset` |  | `number` |  |  |  |
| `FIELD_MAPPING` | `TYPE_MATCH` | `0` | `score.home` | `number` | `number` |  |  |  |
| `VALUE_COMPARISON` | `VALUE_DIFFERS` | `1` | `score.home` | `number` | `number` | `1` | `0` |  |
| `VALUE_COMPARISON` | `MATCH` | `0` | `meta.gameStateId` | `string` | `string` | `g-42` | `g-42` |  |
| `PAIR` | `TYPE_MISMATCH` | `3` | `meta.fixtureId` | `string` | `number` | `"42"` | `42` | `g-42` |

**Sections** (always in this order):

- `META` — identification block. **Topic names pinned on the top two rows** so the file is self-documenting even after the column headers scroll off. Also carries timestamp, correlation key, schema match %, both bus/env entries, descriptions, and per-side message counts.
- `SUMMARY` — one row per status with its total count, plus `TOTAL_PATHS`. Severity column makes filter-and-sort in Excel trivial.
- **`FIELD_MAPPING`** — **structural lens**: one row per field path showing whether the field exists on both sides and carries the same runtime type. Type columns are populated, value columns are blank. Status: `TYPE_MATCH` · `TYPE_MISMATCH` · `ONLY_IN_A` · `ONLY_IN_B`. Use this to answer *"do both topics describe the same data?"*.
- **`VALUE_COMPARISON`** — **value lens**: restricted to paths present on both sides with matching types — i.e. the paths where a side-by-side value compare is meaningful. Type AND value columns are populated. Status: `MATCH` · `VALUE_DIFFERS`. Type-mismatches and only-in-A/B paths are deliberately excluded here (they're already covered by `FIELD_MAPPING`). Use this to answer *"for the fields they share, are the values the same?"*.
- `PAIR` — combined per-pair detail rows (structure + values together) for matched correlation keys. Pair key sits in the rightmost column for easy filtering.

**Status vocabulary:**

| Status | Severity | Section | Meaning |
|---|---:|---|---|
| `TYPE_MISMATCH` | `3` | `FIELD_MAPPING`, `PAIR`, `SUMMARY` | Same path on both sides, different runtime type — almost always a bug. |
| `ONLY_IN_A` / `ONLY_IN_B` | `2` | `FIELD_MAPPING`, `PAIR`, `SUMMARY` | Structural drift — one side ships the field, the other doesn't. |
| `VALUE_DIFFERS` | `1` | `VALUE_COMPARISON`, `PAIR`, `SUMMARY` | Same path & type, different runtime value (often expected). |
| `TYPE_MATCH` | `0` | `FIELD_MAPPING` | Field exists on both sides with the same runtime type (says nothing about values — see `VALUE_COMPARISON`). |
| `MATCH` | `0` | `VALUE_COMPARISON`, `PAIR`, `SUMMARY` | Path is fully equivalent (name + type + value). |

Within `FIELD_MAPPING`, `VALUE_COMPARISON`, and `PAIR`, rows are pre-sorted by **severity descending**, then by path A→Z. Open the file and the loudest fields are at the top — no spreadsheet wizardry required. Sort by `Severity` desc, or filter `Status != MATCH` / `Status != TYPE_MATCH` to hide noise. Long values are truncated to 500 chars; quotes/commas/newlines properly escaped.

Best for: spreadsheet analysis, pivot tables, sharing with stakeholders who prefer Excel, or `awk`/`csvkit` post-processing in a pipeline.

#### 3. Markdown report

Detailed human-readable report. Filename: `…-<ts>.md`. Renders cleanly in Slack, GitHub, Bitbucket, Notion, Confluence — anywhere markdown is supported. Sections, top to bottom:

1. **Header** — generation timestamp, correlation key, matched-pair count.
2. **Verdict** — one of *✅ Schemas align* (≥95%), *⚠ Schemas mostly align* (≥75%), or *❌ Significant divergence*, plus a Unicode progress bar (`████████░░░░  50%`) showing the schema match %.
3. **At a glance** — a 6-row counter table (`✅ Match` / `≈ Value differs` / `⚠ Type mismatch` / `◧ Only in A` / `◨ Only in B` / `Total paths`) with a one-line definition for each outcome.
4. **Sources** — A and B side-by-side, comparing bus, environment, topic, message count, and the topic description.
5. **Field mapping (latest snapshot)** — the full mapping, categorised by outcome with one sub-section per status. Each table shows the field path, types, and values so the *what* and the *why* sit on the same row.
   - `⚠ Type mismatches` — path, type A, type B, value A, value B (highest signal — printed first).
   - `≈ Value differences` — path, value A, value B.
   - `◧ Only in Source A` and `◨ Only in Source B` — path, type, sample value.
   - `✅ Matches` — collapsed inside `<details>` so the report stays scannable for big payloads; expand to audit perfect matches.
6. **Matched pairs** — one card per pair. Each card carries a mini summary table (`Match / Differs / Type ≠ / A only / B only / Total`) plus a sorted list of *only* the non-matching rows for that pair. Pairs with no differences print `_Fully equivalent — no differences for this pair._` so they don't add noise.
7. **Snapshots** — pretty-printed JSON of the latest decoded message on each side.
8. **Legend** — a glossary that explains every icon and status used in the report, so a reader opening the document cold understands it without context.
9. **Appendix** — the full JSON export embedded in a fenced code block (equivalent to option 1).

Best for: weekly write-ups, PR descriptions, posting in a Slack thread for review, "what differs between Pulsar and Kafka right now?" reports, attaching to a JIRA ticket.

---

## Cluster topology and what to compare

There are **three** valid Kafka entry-points for the same logical data, and `kafka-config.json` exposes all of them so you can pick the right comparison:

| Group of envs in the UI | Cluster | Role |
|---|---|---|
| `STG · MSK (…)` / `PRD · MSK (…)` | AWS MSK, `eu-west-1` (`mskgamestatenxt2…` STG / `mskgamestateprd1…` PRD), PLAINTEXT | **GCD upstream** — what the Kafka→Pulsar sink connector reads from. Topic names: `trading.soccer.nxt.sgt` (STG), `trading.soccer.prd.sgt` (PRD). |
| `STG · MFSL` / `PRD · MFSL` | Internal MFSL, UKI (`ieX-mfsld00X-{nxt,prd}.betfair`), PLAINTEXT | Source for `livedata.mappings` (Kafka→Pulsar) and sink for `mappings.discovery` (Pulsar→Kafka, `euw1` connector). |
| `STG · Confluent` / `PRD · Confluent` | Confluent Cloud, `eu-west-1` (`lkc-zg6w6d…` / `lkc-195pyv…`), SASL_SSL + API key | Prematch stats source — STG topics are `trading.nxt.*`, PRD topics drop the `nxt.`. |
| `STG · RCP-mirrored (use2-event-streaming)` / `PRD · RCP-mirrored (use1-mfsfd)` | US-region internal Kafka used by the deployed **RCP** service (`sb-rcpfd-infra`) | **What RCP actually consumes** — both envs use the topic name `trading.soccer.prd.sgt` (STG override sets it back from the default `nxt.sgt`). Useful for a three-way comparison: RCP-view ↔ GCD-source ↔ GCD-Pulsar. |

Pulsar side (use2 region for both STG and PRD; same topic paths in both):

| Logical | Topic | STG broker | PRD broker |
|---|---|---|---|
| Soccer live gamestate | `persistent://gcd/soccer-inbound/game-state` | `pulsar+ssl://pc-24d7b75e.gcd.dts.use2.flutterpsn.com:6651` | `pulsar+ssl://pc-a62b5405.gcd.prd.use2.flutterpsn.com:6651` |
| Live mappings | `persistent://gcd/livedata-inbound/livedata-mappings` | *(same broker)* | *(same broker)* |
| Competition pre-match stats | `persistent://gcd/soccer-inbound/competition-prematch-stats` | *(same broker)* | *(same broker)* |
| Fixture pre-match stats | `persistent://gcd/soccer-inbound/fixture-prematch-stats` | *(same broker)* | *(same broker)* |

### RCP deserializer alignment

This client decodes each payload the same way RCP does, so the **Compare** view is apples-to-apples:

- **Gamestate** (`trading.soccer.*.sgt` and the Pulsar `game-state` mirror) → **protobuf** (`GameStateOutbound.Api`). Decoded when the topic has `schema: "gamestate_api"`.
- **Prematch stats** (`trading.*.prematch.stats` on Confluent and the Pulsar `*-prematch-stats` mirrors) → **JSON** with optional non-JSON prefix (RCP's `StatsHandlerDeserializer` skips bytes before the first `{`, then parses). Decoded when the topic has `valueFormat: "json"`.
- **`livedata.mappings`** → **protobuf** (`MappingDeserializer` in RCP). The mapping `.proto` isn't bundled here yet, so those values surface as hex / lossy UTF-8 until the schema is added.

> The `rcp-service` codebase contains `application.properties` defaults pointing at `localhost:9092` and dev names like `SoccerTopic`; those are local/test only. The **`sb-rcpfd-infra` Ansible vars** (`vars/stg.yml`, `vars/prd.yml`) are the deployed truth and align with the table above.

---

## Config files

| File | Purpose | Hot-reload? |
|---|---|---|
| `config.json` | Pulsar environments, broker URLs, topics, JWT env var names, optional `schema` per topic. | Server picks it up on next consume; restart for sidebar to refresh. |
| `kafka-config.json` | Kafka bootstrap brokers, SSL/SASL, topics, optional `pulsarMirror` hint, optional `schema` (proto) / `valueFormat` (`"json"`). | Same as above. |
| `tokens.sh` | Local-only env-var exports (Pulsar JWTs + Confluent keys). | Re-export and restart. |
| `tokens.sh.example` | Template; copy to `tokens.sh`. | — |

Per-topic optional fields you can set in `kafka-config.json`:

```jsonc
{
  "id":           "soccer_gamestate",
  "label":        "Soccer gamestate (GCD source)",
  "topic":        "trading.soccer.nxt.sgt",
  "pulsarMirror": "persistent://gcd/soccer-inbound/game-state",  // UI hint only
  "schema":       "gamestate_api",                               // triggers proto decode
  "valueFormat":  "json",                                        // triggers JSON-with-prefix decode
  "description":  "One- or two-sentence purpose of this topic. Backticks render as inline code."
}
```

`description` is rendered in the listener page (info strip + sidebar tooltip) and in the compare page (under each source's topic picker) so it's always clear *which* "soccer gamestate" you've selected when several similarly-named topics exist across clusters (MSK source, RCP-mirrored, etc.). The Pulsar `config.json` accepts the same field.

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Red dot on a Pulsar topic, "Token not set: PULSAR_…" | The corresponding env var isn't exported. Edit `tokens.sh`, re-source it, restart the server. |
| Red dot on a Confluent topic | `KAFKA_<env>_CONFLUENT_API_KEY` / `…_SECRET` missing. Set them and restart. |
| Connectivity probe fails on MSK or MFSL | VPN is down or not the right one. MSK/MFSL hosts (`*.amazonaws.com`, `*.betfair`) need corporate / UKI VPN; RCP-mirrored hosts (`*.fndlsb.net`) may need a different reach. |
| Kafka consumer hangs at "▶ Listening" | First TLS / DNS handshake over VPN can be slow; the client uses 60s connection timeout. Try **⏹ Stop** then **▶ Listen** again, or check the server console for `kafkajs` errors. |
| Gamestate messages show as hex | Proto schema didn't load. Server log should say `✓ Proto definitions loaded (gamestate_api)` on startup; if not, run `npm install` (proto files are bundled under `game-state-api-master-api_outbound/`). |
| Compare page: schema match very low | Open the **Structure** tab and look at the `A only` / `B only` rows — usually a Pulsar message has extra envelope fields the Kafka decoded form doesn't, or one side hasn't received its first message yet. |
| `pulsar-client` not found at startup | `brew install apache-pulsar`, then either `eval "$(/opt/homebrew/bin/brew shellenv)"` or open a new shell. |

---

## Requesting Confluent API access (copy-paste)

> Hi team — I'm running the local **GCD Streaming Client** to consume the Confluent Cloud prematch topics for diagnostics. MSK/MFSL/RCP-mirrored clusters work without extra creds, but Confluent needs **SASL_SSL + API key**. Could you provision or point me to **read-only API key + secret** for both clusters?
>
> - **STG**: cluster `lkc-zg6w6d-69eomp.eu-west-1.aws.glb.confluent.cloud:9092` — topics `trading.nxt.competition.soccer.prematch.stats`, `trading.nxt.fixture.soccer.prematch.stats`.
> - **PRD**: cluster `lkc-195pyv-69m5l5.eu-west-1.aws.glb.confluent.cloud:9092` — topics `trading.competition.soccer.prematch.stats`, `trading.fixture.soccer.prematch.stats`.
>
> I'll export them as `KAFKA_{STG,PRD}_CONFLUENT_API_KEY` / `…_SECRET` and restart the app. Happy to use a dedicated service account if that's easier. Thanks!

---

## Project layout

```
PulsarWebClient/
├── server.js                                # Express + WS; runs pulsar-client (Pulsar) and kafkajs (Kafka)
├── config.json                              # Pulsar envs/topics
├── kafka-config.json                        # Kafka envs/topics (MSK / MFSL / Confluent / RCP-mirrored)
├── tokens.sh.example                        # Template for Pulsar JWTs and Confluent keys
├── package.json                             # Node deps (express, ws, kafkajs, protobufjs)
├── game-state-api-master-api_outbound/      # Bundled gamestate outbound .proto files
├── public/
│   ├── index.html                           # Listener page (single topic)
│   ├── compare.html                         # Compare page (two consumers, four views)
│   ├── shared.css                           # FanDuel design system (palette, navbar, buttons)
│   ├── icons.js                             # Inline SVG icon library
│   ├── fanduel-shield.png                   # Favicon + navbar brand icon
│   └── fanduel-wordmark.png                 # Full FanDuel lockup (unused at runtime, kept for future)
└── response/                                # Past JSON exports + reader guide (shared as-is)
    ├── README.md
    ├── STAGING-CAPTURE-SUMMARY.md
    └── staging/*.json
```

---

## Quick start (TL;DR)

**First time on this machine — let the helper do everything:**

```sh
./setup.sh   # one-time: installs prerequisites, scaffolds tokens.sh, runs npm install
# (open tokens.sh in your editor and paste your Pulsar JWTs when prompted)
./run.sh     # every other time: pre-flights, sources tokens.sh, starts the server
# open http://localhost:3456 (or /compare.html)
```

`./setup.sh` checks (and offers to fix, on macOS) every prerequisite — Node ≥ 18, npm, `pulsar-client` (Apache Pulsar CLI, installed via Homebrew), Java, `nc` (netcat), the local `node_modules/`, and a populated `tokens.sh` with JWT-shaped values. `./run.sh` pre-flights the same things every launch and bails out **before** Node crashes if something is missing, pointing you back at `./setup.sh`.

If you'd rather drive things manually:

```sh
brew install node apache-pulsar openjdk
npm install
cp -n tokens.sh.example tokens.sh
# edit tokens.sh and paste your Pulsar JWTs (+ optional Confluent keys)
source tokens.sh && npm start
```

### Available scripts

| Command | What it does |
|---|---|
| `./setup.sh`        | Interactive first-run wizard — installs missing deps (Homebrew on macOS), scaffolds `tokens.sh` from the template, runs `npm install`. Exits 0 when fully green, 1 if you still need to edit `tokens.sh`, 2 on a hard failure (e.g. no Homebrew on macOS). |
| `./setup.sh --check`| Read-only diagnostic. Never installs anything, just reports the state of every prerequisite. Useful for CI or "is this clone ready?" sanity checks. |
| `./setup.sh --yes`  | Non-interactive — assumes "yes" to every install prompt. CI-friendly. |
| `./run.sh`          | Daily-driver. Pre-flights the prerequisites, sources `tokens.sh`, then `exec`s `npm start`. Ctrl-C cleanly stops the server. |
| `./run.sh --skip-check` | Skip the pre-flight (advanced — you'll see raw Node errors if something is wrong). |
| `npm run setup`     | Alias for `./setup.sh`. |
| `npm run check`     | Alias for `./setup.sh --check`. |
| `npm run dev` / `npm run run` | Alias for `./run.sh`. |
| `npm start`         | Raw start — assumes prerequisites are already in place; no pre-flight. |

### What `setup.sh` will check / fix

| Prerequisite | How it's detected | Auto-installable on macOS? |
|---|---|---|
| Node.js ≥ v18 | `node -v` | yes — `brew install node` |
| npm | `npm -v` | bundled with Node |
| `pulsar-client` CLI | `command -v pulsar-client` | yes — `brew install apache-pulsar` |
| Java | `java -version` | yes — `brew install openjdk` |
| `nc` (netcat) | `command -v nc` | yes — usually pre-installed |
| `node_modules/` | directory present and `node_modules/.package-lock.json` not older than `package-lock.json` | yes — `npm install` |
| `tokens.sh` exists | file present | yes — copied from `tokens.sh.example` |
| Pulsar JWTs populated | each `PULSAR_*` env var looks like a JWT (`*.*.*`, ≥ 80 chars) once `tokens.sh` is sourced | **no — only the human filling in the file can do this**; the script tells you which vars are empty or malformed so you know exactly what to paste |

The script reports each check with a colored `✓ / ! / ✗`, ends with a one-line summary per check, and tells you precisely what (if anything) is left to do. JWT *values* are never printed — only their names — so you can safely paste the output into a Slack thread when asking for help.
