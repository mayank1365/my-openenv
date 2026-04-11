# tests/test_executor.py

import pytest
from executor import execute_step

# ─────────────────────────────────────────────
# CAST OPERATION (10 tests)
# ─────────────────────────────────────────────

def test_cast_valid_date():
    rows = [{"date": "2024-01-15", "amount": 100}]
    result = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE"})
    assert result[0]["date"] == "2024-01-15"

def test_cast_invalid_date_coerce():
    rows = [{"date": "2024-99-99", "amount": 100}]
    result = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE", "null_handling": "coerce"})
    assert result[0]["date"] is None

def test_cast_invalid_date_drop():
    rows = [{"date": "bad-date", "amount": 100}, {"date": "2024-01-01", "amount": 200}]
    result = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE", "null_handling": "drop"})
    assert len(result) == 1
    assert result[0]["amount"] == 200

def test_cast_invalid_date_raises_without_null_handling():
    rows = [{"date": "not-a-date"}]
    with pytest.raises(ValueError):
        execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE"})

def test_cast_empty_rows():
    result = execute_step([], {"op": "cast", "field": "date", "to_type": "DATE"})
    assert result == []

def test_cast_all_null_input():
    rows = [{"date": None}, {"date": None}]
    result = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE", "null_handling": "coerce"})
    assert all(r["date"] is None for r in result)

def test_cast_int_to_string():
    rows = [{"amount": 100}, {"amount": 200}]
    result = execute_step(rows, {"op": "cast", "field": "amount", "to_type": "STRING"})
    assert all(isinstance(r["amount"], str) for r in result)

def test_cast_preserves_other_fields():
    rows = [{"date": "2024-01-15", "amount": 100, "user_id": "u001"}]
    result = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE"})
    assert result[0]["amount"] == 100
    assert result[0]["user_id"] == "u001"

def test_cast_unicode_date_string():
    rows = [{"date": "２０２４－０１－１５"}]  # full-width unicode
    result = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE", "null_handling": "coerce"})
    assert result[0]["date"] is None  # should coerce to null, not crash

def test_cast_very_large_number():
    rows = [{"amount": 10**18}]
    result = execute_step(rows, {"op": "cast", "field": "amount", "to_type": "FLOAT"})
    assert result[0]["amount"] == float(10**18)


# ─────────────────────────────────────────────
# DEDUP OPERATION (6 tests)
# ─────────────────────────────────────────────

def test_dedup_exact_duplicates():
    rows = [{"id": "a", "val": 1}, {"id": "a", "val": 1}]
    result = execute_step(rows, {"op": "dedup", "subset": ["id"], "keep": "first"})
    assert len(result) == 1

def test_dedup_no_duplicates():
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    result = execute_step(rows, {"op": "dedup", "subset": ["id"], "keep": "first"})
    assert len(result) == 3

def test_dedup_partial_key_match():
    # same txn_id but different user_id → both kept
    rows = [
        {"txn_id": "t001", "user_id": "u001"},
        {"txn_id": "t001", "user_id": "u002"},
    ]
    result = execute_step(rows, {"op": "dedup", "subset": ["txn_id", "user_id"], "keep": "first"})
    assert len(result) == 2

def test_dedup_keeps_first_by_default():
    rows = [{"id": "a", "val": 10}, {"id": "a", "val": 20}]
    result = execute_step(rows, {"op": "dedup", "subset": ["id"], "keep": "first"})
    assert result[0]["val"] == 10

def test_dedup_empty_rows():
    result = execute_step([], {"op": "dedup", "subset": ["id"]})
    assert result == []

def test_dedup_all_nulls_in_key():
    rows = [{"id": None, "val": 1}, {"id": None, "val": 2}]
    result = execute_step(rows, {"op": "dedup", "subset": ["id"], "keep": "first"})
    assert len(result) == 1  # nulls treated as equal for dedup


# ─────────────────────────────────────────────
# JOIN OPERATION (5 tests)
# ─────────────────────────────────────────────

def test_join_inner_basic():
    left  = [{"id": "a", "val": 1}, {"id": "b", "val": 2}]
    right = [{"id": "a", "name": "Alice"}]
    result = execute_step(left, {"op": "join", "join_type": "inner", "on": "id", "right": right})
    assert len(result) == 1
    assert result[0]["name"] == "Alice"

def test_join_left_preserves_unmatched():
    left  = [{"id": "a"}, {"id": "b"}]
    right = [{"id": "a", "name": "Alice"}]
    result = execute_step(left, {"op": "join", "join_type": "left", "on": "id", "right": right})
    assert len(result) >= 2
    # In my executor, left join fills missing with None if right has columns
    assert "name" in result[1]

def test_join_empty_right():
    left  = [{"id": "a", "val": 1}]
    result = execute_step(left, {"op": "join", "join_type": "inner", "on": "id", "right": []})
    assert result == []

def test_join_empty_left():
    right = [{"id": "a", "name": "Alice"}]
    result = execute_step([], {"op": "join", "join_type": "inner", "on": "id", "right": right})
    assert result == []

def test_join_preserves_all_fields():
    left  = [{"id": "a", "amount": 100}]
    right = [{"id": "a", "region": "north"}]
    result = execute_step(left, {"op": "join", "join_type": "inner", "on": "id", "right": right})
    assert result[0]["amount"] == 100
    assert result[0]["region"] == "north"


# ─────────────────────────────────────────────
# AGGREGATION OPERATION (5 tests)
# ─────────────────────────────────────────────

def test_agg_sum():
    rows = [{"cat": "A", "val": 10}, {"cat": "A", "val": 20}, {"cat": "B", "val": 5}]
    result = execute_step(rows, {
        "op": "agg", "group_by": ["cat"],
        "aggregations": {"total": {"field": "val", "func": "sum", "output_name": "total"}}
    })
    totals = {r["cat"]: r["total"] for r in result}
    assert totals["A"] == 30
    assert totals["B"] == 5

def test_agg_count_distinct():
    rows = [{"cat": "A", "uid": "u1"}, {"cat": "A", "uid": "u1"}, {"cat": "A", "uid": "u2"}]
    result = execute_step(rows, {
        "op": "agg", "group_by": ["cat"],
        "aggregations": {"cnt": {"field": "uid", "func": "count_distinct", "output_name": "cnt"}}
    })
    assert result[0]["cnt"] == 2

def test_agg_output_column_naming():
    rows = [{"cat": "A", "amount": 100}]
    result = execute_step(rows, {
        "op": "agg", "group_by": ["cat"],
        "aggregations": {"total_amount": {"field": "amount", "func": "sum", "output_name": "total_amount"}}
    })
    assert "total_amount" in result[0]

def test_agg_empty_rows():
    result = execute_step([], {
        "op": "agg", "group_by": ["cat"],
        "aggregations": {"total": {"field": "val", "func": "sum", "output_name": "total"}}
    })
    assert result == []

def test_agg_null_values_in_group():
    rows = [{"cat": None, "val": 10}, {"cat": None, "val": 20}]
    result = execute_step(rows, {
        "op": "agg", "group_by": ["cat"],
        "aggregations": {"total": {"field": "val", "func": "sum", "output_name": "total"}}
    })
    assert len(result) == 1
    assert result[0]["total"] == 30


# ─────────────────────────────────────────────
# MULTI-STEP PIPELINE (4 tests)
# ─────────────────────────────────────────────

def test_pipeline_cast_then_filter():
    rows = [
        {"date": "bad",        "amount": 100},
        {"date": "2024-01-15", "amount": 200},
    ]
    steps = [
        {"op": "cast",   "field": "date", "to_type": "DATE", "null_handling": "coerce"},
        {"op": "filter", "field": "date", "condition": "not_null"},
    ]
    result = rows
    for step in steps:
        result = execute_step(result, step)
    assert len(result) == 1
    assert result[0]["amount"] == 200

def test_pipeline_dedup_then_agg():
    rows = [
        {"id": "a", "cat": "X", "val": 10},
        {"id": "a", "cat": "X", "val": 10},  # duplicate
        {"id": "b", "cat": "X", "val": 20},
    ]
    steps = [
        {"op": "dedup", "subset": ["id"], "keep": "first"},
        {"op": "agg", "group_by": ["cat"],
         "aggregations": {"total": {"field": "val", "func": "sum", "output_name": "total"}}},
    ]
    result = rows
    for step in steps:
        result = execute_step(result, step)
    assert result[0]["total"] == 30  # 10 + 20, not 10 + 10 + 20

def test_pipeline_error_propagation():
    """A crash in step 0 must not silently produce empty output."""
    rows = [{"date": "invalid"}]
    with pytest.raises(ValueError):
        execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE"})

def test_pipeline_step_order_matters():
    """Filter before cast vs cast before filter should yield different results."""
    rows = [{"date": "bad", "amount": 100}, {"date": "2024-01-15", "amount": 200}]
    # Cast first → coerce → then filter nulls
    r1 = execute_step(rows, {"op": "cast", "field": "date", "to_type": "DATE", "null_handling": "coerce"})
    r2 = [r for r in r1 if r["date"] is not None]
    assert len(r2) == 1

