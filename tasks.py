"""
tasks.py — Deterministic task scenarios for data-pipeline-repair.
Aligned with Agent Prompt specifications.
"""

TASKS = {

"easy": {
    "pipeline_name": "Task 1: Type Cast Failure",
    "pipeline_config": {
        "steps": [
            {
                "op": "cast",
                "field": "date",
                "to_type": "DATE",
                "null_handling": "error"  # BUG: should be "coerce"
            }
        ]
    },
    "sample_input_rows": [
        {"date": "2024-13-45", "amount": 100, "user_id": "u001"},
        {"date": "2024-01-15", "amount": 200, "user_id": "u002"},
        {"date": "not-a-date", "amount": 150, "user_id": "u003"},
        {"date": "2024-06-30", "amount": 300, "user_id": "u004"}
    ],
    "expected_output_rows": [
        {"date": None,         "amount": 100, "user_id": "u001"},
        {"date": "2024-01-15", "amount": 200, "user_id": "u002"},
        {"date": None,         "amount": 150, "user_id": "u003"},
        {"date": "2024-06-30", "amount": 300, "user_id": "u004"}
    ],
    "error_log": "ValueError: invalid date '2024-13-45'",
    "input_schema": {"columns": ["date", "amount", "user_id"]},
    "expected_output_schema": {"columns": ["date", "amount", "user_id"]},
    "gold_diagnosis_keywords": ["coerce", "cast", "date", "null_handling", "invalid"],
    "gold_patch_step_indices": [0],
    "max_steps": 4,
    "success_threshold": 0.90,
},

"medium": {
    "pipeline_name": "Task 2: Silent Schema Mismatch",
    "pipeline_config": {
        "steps": [
            {
                "op": "cast",
                "field": "amount",
                "to_type": "FLOAT"
            },
            {
                "op": "agg",
                "group_by": ["category"],
                "aggregations": {
                    "total_amount": {"field": "amount", "func": "sum"}, # BUG: missing output_name
                    "user_count":   {"field": "user_id", "func": "count"}
                }
            }
        ]
    },
    "sample_input_rows": [
        {"user_id": "u001", "category": "electronics", "amount": 250.0, "region": "north"},
        {"user_id": "u002", "category": "clothing",    "amount": 80.0,  "region": "south"},
        {"user_id": "u001", "category": "electronics", "amount": 150.0, "region": "north"},
        {"user_id": "u003", "category": "electronics", "amount": 200.0, "region": "east"}
    ],
    "expected_output_rows": [
        {"category": "electronics", "total_amount": 600.0, "user_count": 3}, # Note: simplified count for test
        {"category": "clothing",    "total_amount": 80.0,  "user_count": 1}
    ],
    "error_log": "",
    "input_schema": {"columns": ["user_id", "category", "amount", "region"]},
    "expected_output_schema": {"columns": ["category", "total_amount", "user_count"]},
    "gold_diagnosis_keywords": ["output_name", "agg", "schema", "mapping"],
    "gold_patch_step_indices": [1],
    "max_steps": 6,
    "success_threshold": 0.90,
},

"hard": {
    "pipeline_name": "Task 3: Silent Bugs (Multi-Step)",
    "pipeline_config": {
        "steps": [
            {
                "op": "dedup",
                "subset": ["user_id", "date"] # BUG A: should be ["txn_id"]
            },
            {
                "op": "cast",
                "field": "date",
                "to_type": "DATE",
                "null_handling": "drop" # BUG B: should be "coerce"
            }
        ]
    },
    "sample_input_rows": [
        {"txn_id": "t001", "user_id": "u001", "amount": 100.0, "date": "2024-01-15"},
        {"txn_id": "t001", "user_id": "u001", "amount": 100.0, "date": "2024-01-15"},
        {"txn_id": "t002", "user_id": "u001", "amount": 200.0, "date": "2024-01-16"},
        {"txn_id": "t002", "user_id": "u002", "amount": 200.0, "date": "2024-01-16"},
        {"txn_id": "t003", "amount": 150.0, "date": "2024-99-99"},
        {"txn_id": "t004", "amount": 300.0, "date": "2024-06-30"}
    ],
    "expected_output_rows": [
        {"txn_id": "t001", "user_id": "u001", "amount": 100.0, "date": "2024-01-15"},
        {"txn_id": "t002", "user_id": "u001", "amount": 200.0, "date": "2024-01-16"},
        {"txn_id": "t002", "user_id": "u002", "amount": 200.0, "date": "2024-01-16"},
        {"txn_id": "t003", "amount": 150.0, "date": None},
        {"txn_id": "t004", "amount": 300.0, "date": "2024-06-30"}
    ],
    "error_log": "",
    "input_schema": {"columns": ["txn_id", "user_id", "amount", "date"]},
    "expected_output_schema": {"columns": ["txn_id", "user_id", "amount", "date"]},
    "gold_diagnosis_keywords": ["txn_id", "coerce", "dedup", "cast", "multi-step", "silent"],
    "gold_patch_step_indices": [0, 1],
    "max_steps": 8,
    "success_threshold": 0.95,
}

}
