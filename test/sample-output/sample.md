# Topic Comparison Report

> **Generated** `2026-05-14T13:00:02.000Z`  
> **Correlation key** `meta.gameStateId`  
> **Matched pairs** 1


---

## ❌ Significant schema divergence

Source A and Source B share **50%** of their schemas (4 of 8 unique paths match by name **and** type).

```
████████████░░░░░░░░░░░░  50%
```

### At a glance

| Outcome | Count | What it means |
|---|---:|---|
| ✅ Match           | **3**   | Same path · same type · same value |
| ≈ Value differs   | **1**    | Same path & type — different runtime value |
| ⚠ Type mismatch   | **1**| Same path — different runtime type |
| ◧ Only in A       | **2**   | Path present on A, absent on B |
| ◨ Only in B       | **1**   | Path present on B, absent on A |
| **Total paths**    | **8** | Union of every path on either side |


---

## Sources

|                       | Source A | Source B |
|-----------------------|----------|----------|
| **Bus**               | `pulsar` | `kafka` |
| **Environment**       | `staging` | `kafka-stg` |
| **Topic**             | `persistent://gcd/soccer-inbound/game-state` | `trading.soccer.nxt.sgt` |
| **Messages received** | 3 | 3 |
| **Description**       | Pulsar staging soccer gamestate (rendered output). | GCD source Kafka topic on UKI MSK. |


---

## Field mapping (latest snapshot)

Every field path from either side, grouped by outcome. Loudest categories come first; matches are collapsed at the bottom.

### ⚠ Type mismatches — 1

Same path on both sides, but the runtime types disagree.

| Path | Type A | Type B | Value A | Value B |
|---|---|---|---|---|
| `meta.fixtureId` | `string` | `number` | `99` | `99` |

### ≈ Value differences — 1

Same path **and** type on both sides — but the runtime values disagree.

| Path | Value A | Value B |
|---|---|---|
| `score.home` | `1` | `0` |

### ◧ Only in Source A — 2

These fields ship on A but never appear on B (might be A-specific enrichment or B-specific drops).

| Path | Type | Sample value |
|---|---|---|
| `meta.region` | `string` | `EU` |
| `pulsarTimestamp` | `number` | `1715587200000` |

### ◨ Only in Source B — 1

These fields ship on B but never appear on A.

| Path | Type | Sample value |
|---|---|---|
| `kafkaOffset` | `number` | `17` |

### ✅ Matches — 3

Paths with identical name, type, **and** value on both sides.

<details>
<summary>Click to expand all 3 matching fields</summary>

| Path | Type | Value |
|---|---|---|
| `meta.gameStateId` | `string` | `g-42` |
| `score.away` | `number` | `0` |
| `status` | `string` | `LIVE` |

</details>


---

## Matched pairs — 1

Each pair correlates one message from A with one from B (same value of the correlation key).

### ❌ Pair `g-42` — 50% schema match

- **A timestamp** `2026-05-14T13:00:00.000Z`
- **B timestamp** `2026-05-14T13:00:01.500Z`

| ✅ Match | ≈ Differs | ⚠ Type ≠ | ◧ A only | ◨ B only | Total |
|---:|---:|---:|---:|---:|---:|
| 3 | 1 | 1 | 2 | 1 | 8 |

| Path | Status | Value A | Value B |
|---|---|---|---|
| `meta.fixtureId` | ⚠ Type mismatch | `99` | `99` |
| `kafkaOffset` | ◨ Only in B | `—` | `17` |
| `meta.region` | ◧ Only in A | `EU` | `—` |
| `pulsarTimestamp` | ◧ Only in A | `1715587200000` | `—` |
| `score.home` | ≈ Value differs | `1` | `0` |


---

## Snapshots

### Source A (latest decoded message)

```json
{
  "meta": {
    "gameStateId": "g-42",
    "fixtureId": "99",
    "region": "EU"
  },
  "score": {
    "home": 1,
    "away": 0
  },
  "status": "LIVE",
  "pulsarTimestamp": 1715587200000
}
```

### Source B (latest decoded message)

```json
{
  "meta": {
    "gameStateId": "g-42",
    "fixtureId": 99
  },
  "score": {
    "home": 0,
    "away": 0
  },
  "status": "LIVE",
  "kafkaOffset": 17
}
```


---

## Legend

| Symbol | Status        | Meaning |
|--------|---------------|---------|
| ✅ | `MATCH`         | Same path, same type, same value on both sides |
| ≈ | `VALUE_DIFFERS` | Same path & type — different value at runtime |
| ⚠ | `TYPE_MISMATCH` | Same path — different JS runtime type |
| ◧ | `ONLY_IN_A`     | Path only present in Source A |
| ◨ | `ONLY_IN_B`     | Path only present in Source B |

**Schema match %** counts only how many paths share both name and runtime type (value differences do not reduce it). Use the Matches column to see deeper agreement.


---

## Appendix — full machine-readable export

Equivalent to the `Export → JSON (full)` option. Use this for downstream tooling.

```json
{
  "meta": {
    "exportedAt": "2026-05-14T13:00:02.000Z",
    "correlationKey": "meta.gameStateId",
    "sourceA": {
      "bus": "pulsar",
      "env": "staging",
      "topic": "persistent://gcd/soccer-inbound/game-state",
      "description": "Pulsar staging soccer gamestate (rendered output)."
    },
    "sourceB": {
      "bus": "kafka",
      "env": "kafka-stg",
      "topic": "trading.soccer.nxt.sgt",
      "description": "GCD source Kafka topic on UKI MSK."
    },
    "counts": {
      "a": 3,
      "b": 3,
      "matched": 1
    },
    "schemaMatchPct": 50
  },
  "latest": {
    "a": {
      "meta": {
        "gameStateId": "g-42",
        "fixtureId": "99",
        "region": "EU"
      },
      "score": {
        "home": 1,
        "away": 0
      },
      "status": "LIVE",
      "pulsarTimestamp": 1715587200000
    },
    "b": {
      "meta": {
        "gameStateId": "g-42",
        "fixtureId": 99
      },
      "score": {
        "home": 0,
        "away": 0
      },
      "status": "LIVE",
      "kafkaOffset": 17
    }
  },
  "diff": {
    "structure": {
      "rows": [
        {
          "path": "kafkaOffset",
          "a": "—",
          "b": "number",
          "kind": "onlyB",
          "label": "B only",
          "isType": true
        },
        {
          "path": "meta.fixtureId",
          "a": "string",
          "b": "number",
          "kind": "typediff",
          "label": "Type ≠",
          "isType": true
        },
        {
          "path": "meta.gameStateId",
          "a": "string",
          "b": "string",
          "kind": "match",
          "label": "OK",
          "isType": true
        },
        {
          "path": "meta.region",
          "a": "string",
          "b": "—",
          "kind": "onlyA",
          "label": "A only",
          "isType": true
        },
        {
          "path": "pulsarTimestamp",
          "a": "number",
          "b": "—",
          "kind": "onlyA",
          "label": "A only",
          "isType": true
        },
        {
          "path": "score.away",
          "a": "number",
          "b": "number",
          "kind": "match",
          "label": "OK",
          "isType": true
        },
        {
          "path": "score.home",
          "a": "number",
          "b": "number",
          "kind": "match",
          "label": "OK",
          "isType": true
        },
        {
          "path": "status",
          "a": "string",
          "b": "string",
          "kind": "match",
          "label": "OK",
          "isType": true
        }
      ],
      "nMatch": 4,
      "nType": 1,
      "nA": 2,
      "nB": 1,
      "total": 8,
      "matchPct": 50,
      "sharedDenom": 8
    },
    "values": {
      "rows": [
        {
          "path": "meta.fixtureId",
          "a": "99",
          "b": 99,
          "kind": "equal",
          "label": "Equal"
        },
        {
          "path": "meta.gameStateId",
          "a": "g-42",
          "b": "g-42",
          "kind": "equal",
          "label": "Equal"
        },
        {
          "path": "score.away",
          "a": 0,
          "b": 0,
          "kind": "equal",
          "label": "Equal"
        },
        {
          "path": "score.home",
          "a": 1,
          "b": 0,
          "kind": "diff",
          "label": "Differs"
        },
        {
          "path": "status",
          "a": "LIVE",
          "b": "LIVE",
          "kind": "equal",
          "label": "Equal"
        }
      ],
      "nEq": 4,
      "nDiff": 1,
      "totalShared": 5
    },
    "both": {
      "rows": [
        {
          "path": "kafkaOffset",
          "a": "—",
          "b": 17,
          "kind": "onlyB",
          "label": "B only"
        },
        {
          "path": "meta.fixtureId",
          "a": "99",
          "b": 99,
          "kind": "typediff",
          "label": "Type ≠ (string / number)"
        },
        {
          "path": "meta.gameStateId",
          "a": "g-42",
          "b": "g-42",
          "kind": "equal",
          "label": "Equal"
        },
        {
          "path": "meta.region",
          "a": "EU",
          "b": "—",
          "kind": "onlyA",
          "label": "A only"
        },
        {
          "path": "pulsarTimestamp",
          "a": 1715587200000,
          "b": "—",
          "kind": "onlyA",
          "label": "A only"
        },
        {
          "path": "score.away",
          "a": 0,
          "b": 0,
          "kind": "equal",
          "label": "Equal"
        },
        {
          "path": "score.home",
          "a": 1,
          "b": 0,
          "kind": "diff",
          "label": "Differs"
        },
        {
          "path": "status",
          "a": "LIVE",
          "b": "LIVE",
          "kind": "equal",
          "label": "Equal"
        }
      ],
      "nEq": 3,
      "nDiff": 1,
      "nType": 1,
      "nA": 2,
      "nB": 1
    }
  },
  "pairs": [
    {
      "key": "g-42",
      "a": {
        "ts": "2026-05-14T13:00:00.000Z",
        "content": {
          "meta": {
            "gameStateId": "g-42",
            "fixtureId": "99",
            "region": "EU"
          },
          "score": {
            "home": 1,
            "away": 0
          },
          "status": "LIVE",
          "pulsarTimestamp": 1715587200000
        }
      },
      "b": {
        "ts": "2026-05-14T13:00:01.500Z",
        "content": {
          "meta": {
            "gameStateId": "g-42",
            "fixtureId": 99
          },
          "score": {
            "home": 0,
            "away": 0
          },
          "status": "LIVE",
          "kafkaOffset": 17
        }
      },
      "diff": {
        "structure": {
          "rows": [
            {
              "path": "kafkaOffset",
              "a": "—",
              "b": "number",
              "kind": "onlyB",
              "label": "B only",
              "isType": true
            },
            {
              "path": "meta.fixtureId",
              "a": "string",
              "b": "number",
              "kind": "typediff",
              "label": "Type ≠",
              "isType": true
            },
            {
              "path": "meta.gameStateId",
              "a": "string",
              "b": "string",
              "kind": "match",
              "label": "OK",
              "isType": true
            },
            {
              "path": "meta.region",
              "a": "string",
              "b": "—",
              "kind": "onlyA",
              "label": "A only",
              "isType": true
            },
            {
              "path": "pulsarTimestamp",
              "a": "number",
              "b": "—",
              "kind": "onlyA",
              "label": "A only",
              "isType": true
            },
            {
              "path": "score.away",
              "a": "number",
              "b": "number",
              "kind": "match",
              "label": "OK",
              "isType": true
            },
            {
              "path": "score.home",
              "a": "number",
              "b": "number",
              "kind": "match",
              "label": "OK",
              "isType": true
            },
            {
              "path": "status",
              "a": "string",
              "b": "string",
              "kind": "match",
              "label": "OK",
              "isType": true
            }
          ],
          "nMatch": 4,
          "nType": 1,
          "nA": 2,
          "nB": 1,
          "total": 8,
          "matchPct": 50,
          "sharedDenom": 8
        },
        "values": {
          "rows": [
            {
              "path": "meta.fixtureId",
              "a": "99",
              "b": 99,
              "kind": "equal",
              "label": "Equal"
            },
            {
              "path": "meta.gameStateId",
              "a": "g-42",
              "b": "g-42",
              "kind": "equal",
              "label": "Equal"
            },
            {
              "path": "score.away",
              "a": 0,
              "b": 0,
              "kind": "equal",
              "label": "Equal"
            },
            {
              "path": "score.home",
              "a": 1,
              "b": 0,
              "kind": "diff",
              "label": "Differs"
            },
            {
              "path": "status",
              "a": "LIVE",
              "b": "LIVE",
              "kind": "equal",
              "label": "Equal"
            }
          ],
          "nEq": 4,
          "nDiff": 1,
          "totalShared": 5
        },
        "both": {
          "rows": [
            {
              "path": "kafkaOffset",
              "a": "—",
              "b": 17,
              "kind": "onlyB",
              "label": "B only"
            },
            {
              "path": "meta.fixtureId",
              "a": "99",
              "b": 99,
              "kind": "typediff",
              "label": "Type ≠ (string / number)"
            },
            {
              "path": "meta.gameStateId",
              "a": "g-42",
              "b": "g-42",
              "kind": "equal",
              "label": "Equal"
            },
            {
              "path": "meta.region",
              "a": "EU",
              "b": "—",
              "kind": "onlyA",
              "label": "A only"
            },
            {
              "path": "pulsarTimestamp",
              "a": 1715587200000,
              "b": "—",
              "kind": "onlyA",
              "label": "A only"
            },
            {
              "path": "score.away",
              "a": 0,
              "b": 0,
              "kind": "equal",
              "label": "Equal"
            },
            {
              "path": "score.home",
              "a": 1,
              "b": 0,
              "kind": "diff",
              "label": "Differs"
            },
            {
              "path": "status",
              "a": "LIVE",
              "b": "LIVE",
              "kind": "equal",
              "label": "Equal"
            }
          ],
          "nEq": 3,
          "nDiff": 1,
          "nType": 1,
          "nA": 2,
          "nB": 1
        }
      }
    }
  ],
  "streamA": [
    {
      "ts": "2026-05-14T12:59:55.000Z",
      "decoded": {
        "meta": {
          "gameStateId": "g-42",
          "fixtureId": "99",
          "region": "EU"
        },
        "score": {
          "home": 1,
          "away": 0
        },
        "status": "LIVE",
        "pulsarTimestamp": 1715587200000
      },
      "text": ""
    },
    {
      "ts": "2026-05-14T13:00:00.000Z",
      "decoded": {
        "meta": {
          "gameStateId": "g-42",
          "fixtureId": "99",
          "region": "EU"
        },
        "score": {
          "home": 1,
          "away": 0
        },
        "status": "LIVE",
        "pulsarTimestamp": 1715587200000
      },
      "text": ""
    },
    {
      "ts": "2026-05-14T13:00:05.000Z",
      "decoded": {
        "meta": {
          "gameStateId": "g-42",
          "fixtureId": "99",
          "region": "EU"
        },
        "score": {
          "home": 1,
          "away": 0
        },
        "status": "LIVE",
        "pulsarTimestamp": 1715587200000
      },
      "text": ""
    }
  ],
  "streamB": [
    {
      "ts": "2026-05-14T12:59:56.000Z",
      "decoded": {
        "meta": {
          "gameStateId": "g-42",
          "fixtureId": 99
        },
        "score": {
          "home": 0,
          "away": 0
        },
        "status": "LIVE",
        "kafkaOffset": 17
      },
      "text": ""
    },
    {
      "ts": "2026-05-14T13:00:01.500Z",
      "decoded": {
        "meta": {
          "gameStateId": "g-42",
          "fixtureId": 99
        },
        "score": {
          "home": 0,
          "away": 0
        },
        "status": "LIVE",
        "kafkaOffset": 17
      },
      "text": ""
    },
    {
      "ts": "2026-05-14T13:00:06.500Z",
      "decoded": {
        "meta": {
          "gameStateId": "g-42",
          "fixtureId": 99
        },
        "score": {
          "home": 0,
          "away": 0
        },
        "status": "LIVE",
        "kafkaOffset": 17
      },
      "text": ""
    }
  ]
}
```
