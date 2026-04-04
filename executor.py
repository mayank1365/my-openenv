"""
executor.py — Pure-Python ETL pipeline interpreter.
Supports step types: cast, select, rename, filter, date_parse, join, agg
"""
import copy
import datetime
from typing import Any

SUPPORTED_STEP_TYPES = ["cast", "select", "rename", "filter", "date_parse", "join", "agg"]

CAST_TYPES = {
    "int":   lambda v: int(float(str(v).strip())),
    "float": lambda v: float(str(v).strip()),
    "str":   lambda v: str(v),
    "bool":  lambda v: str(v).lower() in ("1", "true", "yes"),
}

def run_pipeline(config: dict, rows: list[dict]) -> tuple[list[dict], str | None]:
    """
    Execute each step in config["steps"] sequentially.
    Returns (output_rows, error_string).
    error_string is None on success.
    """
    data = copy.deepcopy(rows)
    for i, step in enumerate(config["steps"]):
        try:
            data = _apply_step(step, data)
        except Exception as e:
            return [], f"Step {i} ({step['type']}): {type(e).__name__}: {e}"
    return data, None


def apply_patch(config: dict, patch: dict) -> dict:
    """
    Apply a patch to config["steps"][step_index].
    field is dot-notation: "params.null_handling" → config["steps"][i]["params"]["null_handling"]
    Returns a new config dict (does not mutate original).
    """
    config = copy.deepcopy(config)
    step = config["steps"][patch["step_index"]]
    parts = patch["field"].split(".")
    target = step
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = patch["new_value"]
    return config


def compare_output(got: list[dict], expected: list[dict]) -> dict:
    """
    Compare actual output rows against expected rows.
    Returns a dict with row_match (float 0–1), schema_match (bool),
    count_match (bool), matching_rows (int), total_expected (int).
    """
    if not expected:
        return {
            "row_match": 1.0, "schema_match": True,
            "count_match": True, "matching_rows": 0, "total_expected": 0,
        }
    schema_match = bool(got) and set(got[0].keys()) == set(expected[0].keys())
    count_match = len(got) == len(expected)
    matching_rows = sum(
        1 for g, e in zip(
            [_normalize_row(r) for r in got],
            [_normalize_row(r) for r in expected]
        ) if g == e
    )
    row_match = matching_rows / len(expected)
    return {
        "row_match": round(row_match, 4),
        "schema_match": schema_match,
        "count_match": count_match,
        "matching_rows": matching_rows,
        "total_expected": len(expected),
    }


def _normalize_row(row: dict) -> dict:
    """Normalize values for comparison: strip strings, None stays None."""
    result = {}
    for k, v in row.items():
        if isinstance(v, str):
            result[k] = v.strip()
        elif isinstance(v, float) and v == int(v):
            result[k] = int(v)
        else:
            result[k] = v
    return result


def _apply_step(step: dict, rows: list[dict]) -> list[dict]:
    t = step["type"]
    p = step.get("params", {})

    if t not in SUPPORTED_STEP_TYPES:
        raise ValueError(f"Unknown step type: {t!r}")

    if t == "cast":
        col = p["column"]
        dtype = p["dtype"]
        null_handling = p.get("null_handling", "error")  # error | coerce | drop
        cast_fn = CAST_TYPES.get(dtype)
        if not cast_fn:
            raise ValueError(f"Unsupported dtype: {dtype!r}")
        out = []
        for row in rows:
            val = row.get(col)
            if val is None or str(val).strip() in ("", "N/A", "null", "NULL", "None"):
                if null_handling == "coerce":
                    out.append({**row, col: None})
                elif null_handling == "drop":
                    pass
                else:
                    raise ValueError(f"Cannot cast {val!r} to {dtype} (null_handling=error)")
                continue
            try:
                out.append({**row, col: cast_fn(val)})
            except (ValueError, TypeError) as e:
                if null_handling == "coerce":
                    out.append({**row, col: None})
                elif null_handling == "drop":
                    pass
                else:
                    raise ValueError(f"Cannot cast {val!r} to {dtype}: {e}")
        return out

    elif t == "select":
        cols = p["columns"]
        out = []
        for row in rows:
            missing = [c for c in cols if c not in row]
            if missing:
                raise KeyError(f"Column(s) not found: {missing}")
            out.append({c: row[c] for c in cols})
        return out

    elif t == "rename":
        mapping = p["mapping"]
        return [{mapping.get(k, k): v for k, v in row.items()} for row in rows]

    elif t == "filter":
        col = p["column"]
        op  = p["op"]
        val = p["value"]
        ops = {
            "eq":       lambda a, b: a == b,
            "ne":       lambda a, b: a != b,
            "gt":       lambda a, b: a is not None and a > b,
            "lt":       lambda a, b: a is not None and a < b,
            "gte":      lambda a, b: a is not None and a >= b,
            "lte":      lambda a, b: a is not None and a <= b,
            "contains": lambda a, b: b in str(a) if a is not None else False,
            "notnull":  lambda a, b: a is not None,
            "isnull":   lambda a, b: a is None,
        }
        if op not in ops:
            raise ValueError(f"Unknown filter op: {op!r}")
        return [row for row in rows if ops[op](row.get(col), val)]

    elif t == "date_parse":
        col = p["column"]
        fmt = p["format"]
        out_fmt = p.get("output_format", "%Y-%m-%d")
        null_handling = p.get("null_handling", "error")
        out = []
        for row in rows:
            raw = row.get(col)
            if raw is None or str(raw).strip() == "":
                if null_handling == "coerce":
                    out.append({**row, col: None})
                elif null_handling == "drop":
                    pass
                else:
                    raise ValueError(f"Cannot parse null date in column {col!r}")
                continue
            try:
                parsed = datetime.datetime.strptime(str(raw).strip(), fmt)
                out.append({**row, col: parsed.strftime(out_fmt)})
            except ValueError as e:
                if null_handling == "coerce":
                    out.append({**row, col: None})
                elif null_handling == "drop":
                    pass
                else:
                    raise ValueError(f"Cannot parse date {raw!r} with format {fmt!r}: {e}")
        return out

    elif t == "join":
        on_col    = p["on"]
        right_rows = p["right_rows"]
        join_type  = p.get("join_type", "left")
        dedup      = p.get("dedup_right", False)
        # Build lookup; if dedup=False and there are duplicate keys, rows will be duplicated
        if dedup:
            lookup = {}
            for r in right_rows:
                k = r[on_col]
                if k not in lookup:
                    lookup[k] = r
        else:
            from collections import defaultdict
            lookup_multi = defaultdict(list)
            for r in right_rows:
                lookup_multi[r[on_col]].append(r)
        out = []
        for row in rows:
            key = row.get(on_col)
            if dedup:
                match = lookup.get(key, {})
                merged = {**row}
                for k, v in match.items():
                    if k != on_col:
                        merged[k] = v
                out.append(merged)
            else:
                matches = lookup_multi.get(key, [{}])
                for match in matches:
                    merged = {**row}
                    for k, v in match.items():
                        if k != on_col:
                            merged[k] = v
                    out.append(merged)
        return out

    elif t == "agg":
        group_by = p.get("group_by", [])
        agg_col  = p["agg_col"]
        agg_fn   = p["agg_fn"]   # sum | count | mean | min | max
        from collections import defaultdict
        groups: dict[tuple, list] = defaultdict(list)
        for row in rows:
            key = tuple(row.get(g) for g in group_by)
            val = row.get(agg_col)
            if val is not None:
                groups[key].append(val)
        result = []
        for key, vals in groups.items():
            row = {g: k for g, k in zip(group_by, key)}
            if agg_fn == "sum":
                row[agg_col] = sum(vals)
            elif agg_fn == "count":
                row[agg_col] = len(vals)
            elif agg_fn == "mean":
                row[agg_col] = round(sum(vals) / len(vals), 4) if vals else None
            elif agg_fn == "min":
                row[agg_col] = min(vals)
            elif agg_fn == "max":
                row[agg_col] = max(vals)
            else:
                raise ValueError(f"Unknown agg_fn: {agg_fn!r}")
            result.append(row)
        return result

    raise ValueError(f"Unhandled step type: {t!r}")
