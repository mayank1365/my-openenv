"""
tasks.py — Deterministic task scenarios for data-pipeline-repair.
Each task defines: pipeline_name, pipeline_config (the BROKEN version),
sample_input_rows, expected_output_rows, error_log, input_schema,
expected_output_schema, gold_diagnosis_keywords, gold_patch_step_indices.
"""

TASKS = {

# ─────────────────────────────────────────────────────────────────────
# EASY: single type-cast bug, explicit error log, one-step fix
# Agent must change null_handling from "error" to "coerce" in step 0
# ─────────────────────────────────────────────────────────────────────
"easy": {
    "pipeline_name": "user_signups_daily",
    "pipeline_config": {
        "steps": [
            {
                "type": "cast",
                "params": {
                    "column": "age",
                    "dtype": "int",
                    "null_handling": "error"     # BUG: should be "coerce"
                }
            },
            {
                "type": "filter",
                "params": {"column": "age", "op": "notnull", "value": None}
            },
            {
                "type": "select",
                "params": {"columns": ["user_id", "email", "age", "country"]}
            }
        ]
    },
    "sample_input_rows": [
        {"user_id": "u001", "email": "alice@example.com", "age": "28",  "country": "IN", "signup_src": "web"},
        {"user_id": "u002", "email": "bob@example.com",   "age": "N/A", "country": "US", "signup_src": "app"},
        {"user_id": "u003", "email": "carol@example.com", "age": "34",  "country": "UK", "signup_src": "web"},
        {"user_id": "u004", "email": "dan@example.com",   "age": "null","country": "IN", "signup_src": "api"},
        {"user_id": "u005", "email": "eve@example.com",   "age": "22",  "country": "US", "signup_src": "web"},
    ],
    "expected_output_rows": [
        {"user_id": "u001", "email": "alice@example.com", "age": 28,  "country": "IN"},
        {"user_id": "u003", "email": "carol@example.com", "age": 34,  "country": "UK"},
        {"user_id": "u005", "email": "eve@example.com",   "age": 22,  "country": "US"},
    ],
    "error_log": "Step 0 (cast): ValueError: Cannot cast 'N/A' to int (null_handling=error)",
    "input_schema":  {"columns": ["user_id","email","age","country","signup_src"],
                      "types":   ["str","str","str","str","str"]},
    "expected_output_schema": {"columns": ["user_id","email","age","country"],
                               "types":   ["str","str","int","str"]},
    "gold_diagnosis_keywords": ["type", "cast", "null", "coerce", "N/A", "invalid"],
    "gold_patch_step_indices": [0],
    "gold_patches": [
        {"step_index": 0, "field": "params.null_handling",
         "old_value": "error", "new_value": "coerce"}
    ],
    "max_steps": 4,
    "success_threshold": 0.8,
},

# ─────────────────────────────────────────────────────────────────────
# MEDIUM: upstream column dropped, causing downstream KeyError
# Agent must trace the error from step 2 back to the select in step 1
# Fix: add "region" back to step 1's columns list
# ─────────────────────────────────────────────────────────────────────
"medium": {
    "pipeline_name": "sales_report_weekly",
    "pipeline_config": {
        "steps": [
            {
                "type": "rename",
                "params": {"mapping": {"amt": "amount", "ts": "event_date"}}
            },
            {
                "type": "select",
                "params": {
                    "columns": ["order_id", "amount", "event_date"]
                    # BUG: "region" is missing — step 2 needs it
                }
            },
            {
                "type": "agg",
                "params": {
                    "group_by": ["region", "event_date"],
                    "agg_col": "amount",
                    "agg_fn": "sum"
                }
            }
        ]
    },
    "sample_input_rows": [
        {"order_id": "o1", "amt": 120.0, "ts": "2024-01-15", "region": "North", "channel": "online"},
        {"order_id": "o2", "amt":  80.0, "ts": "2024-01-15", "region": "South", "channel": "store"},
        {"order_id": "o3", "amt": 200.0, "ts": "2024-01-16", "region": "North", "channel": "online"},
        {"order_id": "o4", "amt":  60.0, "ts": "2024-01-16", "region": "East",  "channel": "store"},
        {"order_id": "o5", "amt": 150.0, "ts": "2024-01-16", "region": "South", "channel": "online"},
    ],
    "expected_output_rows": [
        {"region": "North", "event_date": "2024-01-15", "amount": 120.0},
        {"region": "South", "event_date": "2024-01-15", "amount":  80.0},
        {"region": "North", "event_date": "2024-01-16", "amount": 200.0},
        {"region": "East",  "event_date": "2024-01-16", "amount":  60.0},
        {"region": "South", "event_date": "2024-01-16", "amount": 150.0},
    ],
    "error_log": "Step 2 (agg): KeyError: \"Column(s) not found: ['region']\"",
    "input_schema": {
        "columns": ["order_id","amt","ts","region","channel"],
        "types":   ["str","float","str","str","str"]
    },
    "expected_output_schema": {
        "columns": ["region","event_date","amount"],
        "types":   ["str","str","float"]
    },
    "gold_diagnosis_keywords": ["region", "select", "dropped", "missing", "column", "upstream"],
    "gold_patch_step_indices": [1],
    "gold_patches": [
        {
            "step_index": 1,
            "field": "params.columns",
            "old_value": ["order_id", "amount", "event_date"],
            "new_value":  ["order_id", "amount", "event_date", "region"]
        }
    ],
    "max_steps": 6,
    "success_threshold": 0.75,
},

# ─────────────────────────────────────────────────────────────────────
# HARD: two silent bugs — no error log. Agent must diff the data.
# Bug 1: date format is "%Y/%m/%d" but data uses "%Y-%m-%d" → all dates NULL
# Bug 2: right_rows in join has duplicate user_ids → row duplication
# Agent must apply two patches in sequence.
# ─────────────────────────────────────────────────────────────────────
"hard": {
    "pipeline_name": "revenue_attribution_v2",
    "pipeline_config": {
        "steps": [
            {
                "type": "date_parse",
                "params": {
                    "column": "event_date",
                    "format": "%Y/%m/%d",         # BUG 1: data uses "%Y-%m-%d"
                    "null_handling": "coerce"
                }
            },
            {
                "type": "join",
                "params": {
                    "on": "user_id",
                    "dedup_right": False,          # BUG 2: right_rows has dupes
                    "right_rows": [
                        {"user_id": "u001", "plan": "pro",  "mrr": 49.0},
                        {"user_id": "u001", "plan": "pro",  "mrr": 49.0},  # DUPLICATE
                        {"user_id": "u002", "plan": "free", "mrr":  0.0},
                        {"user_id": "u003", "plan": "pro",  "mrr": 49.0},
                        {"user_id": "u003", "plan": "pro",  "mrr": 49.0},  # DUPLICATE
                    ]
                }
            },
            {
                "type": "agg",
                "params": {
                    "group_by": ["event_date", "plan"],
                    "agg_col": "mrr",
                    "agg_fn": "sum"
                }
            }
        ]
    },
    "sample_input_rows": [
        {"user_id": "u001", "event_date": "2024-01-15", "event": "upgrade"},
        {"user_id": "u002", "event_date": "2024-01-15", "event": "signup"},
        {"user_id": "u003", "event_date": "2024-01-16", "event": "upgrade"},
    ],
    # Expected: correct dates, no duplication → correct MRR sums
    "expected_output_rows": [
        {"event_date": "2024-01-15", "plan": "pro",  "mrr": 49.0},
        {"event_date": "2024-01-15", "plan": "free", "mrr":  0.0},
        {"event_date": "2024-01-16", "plan": "pro",  "mrr": 49.0},
    ],
    "error_log": "",   # NO ERROR — silent bugs only. Agent must inspect data.
    "input_schema": {
        "columns": ["user_id","event_date","event"],
        "types":   ["str","str","str"]
    },
    "expected_output_schema": {
        "columns": ["event_date","plan","mrr"],
        "types":   ["str","str","float"]
    },
    "gold_diagnosis_keywords": [
        "date", "format", "silent", "null", "duplicate", "join", "dedup", "inflated"
    ],
    "gold_patch_step_indices": [0, 1],
    "gold_patches": [
        {
            "step_index": 0,
            "field": "params.format",
            "old_value": "%Y/%m/%d",
            "new_value": "%Y-%m-%d"
        },
        {
            "step_index": 1,
            "field": "params.dedup_right",
            "old_value": False,
            "new_value": True
        }
    ],
    "max_steps": 8,
    "success_threshold": 0.70,
},

}
