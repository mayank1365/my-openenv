# Hard Task Walkthrough: revenue_attribution_v2

This document providing a concrete example and walkthrough for Task 3 in the `data-pipeline-repair` benchmark.

## The Problem

Task 3 is difficult because it contains **two independent bugs** that happen silently. The pipeline finishes without crashing, but the resulting data is missing records or contains incorrect information.

### Incident Report
- **Symptoms**: 
    1. Output row count is incorrect (e.g., 400 instead of 498).
    2. Data totals for Monthly Recurring Revenue (MRR) are wrong across specific dates.
- **Error Log**: `""` (Empty - simulation runs but logic is flawed).

---

## BUG A: Deduplication on Non-Unique Key (Step 1)

### The Issue
The pipeline attempts to remove duplicate transactions using the fields `["user_id", "date"]`. However, a user can have multiple legitimate transactions on the same day (`t002` and `t003` for the same user). Using these fields as the deduplication key deletes valid data.

**Broken Config:**
```json
{
  "op": "dedup",
  "subset": ["user_id", "date"],
  "keep": "first"
}
```

**Result**: Legitimate transactions are dropped, leading to a significant drop in `row_match`.

**The Fix**: Deduplicate using the natural primary key of the transaction: `["txn_id"]`.

---

## BUG B: Silent Row Drop in Date Casting (Step 2)

### The Issue
The pipeline converts the `date` string to a `DATE` type. One of the records contains an invalid date (`"2024-99-99"`). The step is configured with `null_handling: "drop"`, which silently deletes the entire row instead of keeping it for downstream analysis.

**Broken Config:**
```json
{
  "op": "cast",
  "field": "date",
  "to_type": "DATE",
  "null_handling": "drop"
}
```

**Result**: Instead of preserving all rows and marking invalid dates as `null`, the pipeline loses the record entirely.

**The Fix**: Set `null_handling` to `"coerce"`. This ensures the row is kept with a `null` date value.

---

## Success Criteria

A successful repair requires fixing **both** bugs.
1. Fix Dedup: Improves `row_match` by preserving unique transaction IDs.
2. Fix Cast: Ensures that records with invalid data are not silently deleted, reaching `exact_match = true`.

**Final Expected Score**: `normalized_reward ≥ 0.95`.
