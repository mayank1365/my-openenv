"""
inference.py — Baseline LLM agent for data-pipeline-repair.

MANDATORY environment variables (injected by hackathon validator):
  API_BASE_URL   — LiteLLM proxy endpoint
  API_KEY        — LiteLLM proxy key
  MODEL_NAME     — model identifier (default: llama-3.3-70b-versatile)

For local development only:
  HF_TOKEN       — fallback when API_KEY is not set
  ENV_URL        — environment server URL

STDOUT FORMAT (strictly required by validator):
  [START] task=<task_name> env=<benchmark> model=<model_name>
  [STEP]  step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
  [END]   success=<true|false> steps=<n> score=<0.00> rewards=<r1,r2,...>
"""
import os
import json
import time
import requests
from openai import OpenAI

# ── Environment variables ─────────────────────────────────────────────────────
# Validator injects API_BASE_URL and API_KEY. HF_TOKEN is local-dev fallback.
API_BASE_URL = os.getenv("API_BASE_URL") or "https://api.groq.com/openai/v1"
MODEL_NAME   = os.getenv("MODEL_NAME")   or "llama-3.3-70b-versatile"
API_KEY      = os.getenv("API_KEY")      or os.getenv("HF_TOKEN")
ENV_URL      = os.getenv("ENV_URL")      or "https://hollow-abyss-my-env.hf.space"
BENCHMARK    = "data-pipeline-repair"

# ── OpenAI client ─────────────────────────────────────────────────────────────
# Validator injects API_BASE_URL and API_KEY; HF_TOKEN is local-dev fallback.
# Use os.getenv() (never os.environ[]) to avoid KeyError on missing vars.
if not API_KEY:
    raise EnvironmentError(
        "No API key found. Set API_KEY (injected by validator) "
        "or HF_TOKEN for local development."
    )

try:
    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)
except Exception as _client_err:
    raise RuntimeError(
        f"Failed to initialise OpenAI client "
        f"(base_url={API_BASE_URL!r}): {_client_err}"
    ) from _client_err


# ── Stdout logging helpers ────────────────────────────────────────────────────
def log_start(task: str) -> None:
    print(f"[START] task={task} env={BENCHMARK} model={MODEL_NAME}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error) -> None:
    error_val = error if error else "null"
    done_val  = str(done).lower()
    print(
        f"[STEP] step={step} action={action} reward={reward:.2f} "
        f"done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: list) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} "
        f"score={score:.2f} rewards={rewards_str}",
        flush=True,
    )


# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a data engineering agent. You debug broken ETL pipeline configs.

At each step you receive a JSON observation and must respond with exactly ONE JSON object.

=== OUTPUT FORMAT ===
{
  "diagnosis": "<root cause hypothesis — be specific>",
  "patch": {
    "step_index": <int: 0-indexed step to fix>,
    "field": "<dot.notation path e.g. params.null_handling>",
    "old_value": <current value, optional>,
    "new_value": <replacement value>
  },
  "validate_only": false
}
Omit "patch" when using validate_only:true.

=== VALID FIELD VALUES ===
- params.null_handling: ONLY "coerce", "drop", or "error". NEVER use "skip", "ignore", or any other value.
- params.dedup_right: true or false (boolean, not string)
- params.format (date_parse): Python strptime format strings e.g. "%Y-%m-%d", "%Y/%m/%d"

=== DECISION RULES ===

1. READ error_log FIRST. If it contains a step number and error type, fix that exact step.
   - "Cannot cast X to int (null_handling='error'...)" → set params.null_handling to "coerce"
   - "Column(s) not found: ['region']" at step N → fix the upstream select step to include that column

2. If error_log is EMPTY, inspect current_output_rows for silent bugs:
   - Are any values null that should not be? → check date_parse format or cast null_handling
   - Is row count wrong (more rows than expected)? → check join for dedup_right:false with duplicate right_rows
   - Are numeric values inflated (e.g. 2x expected)? → join duplication; set params.dedup_right=true

3. Fix ROOT CAUSE, not symptoms:
   - Missing column error in agg/filter → fix the upstream select step, NOT the agg step
   - Do NOT patch aggregation params when the real issue is missing input columns

4. NEVER repeat a patch you already applied. Check fix_attempts carefully before proposing a patch.
   If a patch did not improve row_match, it either didn't fix the right thing or introduced the wrong value.
   Try a DIFFERENT field or a DIFFERENT new_value.

5. NEVER toggle between two values (e.g. "error" → "skip" → "error" → ...). If a value didn't work, move on.

6. When unsure, validate first WITHOUT patching to inspect output:
   {"diagnosis": "checking current state", "validate_only": true}

7. Stop immediately when solved:
   If comparison.row_match == 1.0:
   {"diagnosis": "pipeline is fixed", "validate_only": true}

=== MULTI-BUG STRATEGY (for hard tasks) ===
When error_log is empty and row_match < 1.0 after a patch:
- Check EVERY field in current_output_rows for anomalies, not just the one you fixed.
- Common silent bug pairs:
  a. Wrong date format (all dates become null) + join duplication (MRR inflated)
  b. Fix them ONE at a time. After each patch, re-examine current_output_rows.

=== ANTI-PATTERNS — NEVER DO THESE ===
- Do NOT use null_handling="skip" — it is not a valid value
- Do NOT modify params.right_rows directly — use params.dedup_right=true instead
- Do NOT patch steps unrelated to the bug
- Do NOT repeat the same (step_index, field, new_value) combination from fix_attempts
- Do NOT make multiple changes in one patch

Respond ONLY with valid JSON. No markdown, no explanations outside the JSON."""


# ── Conversation history builder ───────────────────────────────────────────────
def _build_history(fix_attempts: list) -> list:
    messages = []
    for attempt in fix_attempts:
        action = {
            "diagnosis": attempt.get("diagnosis", ""),
            "patch":     attempt.get("patch"),
        }
        messages.append({"role": "assistant", "content": json.dumps(action)})
        feedback = {
            "result":         "patch_applied" if attempt.get("patch") else "validate_only",
            "row_match_after": attempt.get("row_match", 0.0),
            "reward":          attempt.get("reward", 0.0),
            "note": (
                "✅ Improvement!" if attempt.get("reward", 0) > 0
                else "❌ No improvement — try a different approach."
            ),
        }
        messages.append({"role": "user", "content": json.dumps(feedback)})
    return messages


# ── LLM call ──────────────────────────────────────────────────────────────────
def call_llm(obs: dict, fix_attempts: list) -> str:
    initial_context = {
        "pipeline_config":        obs["pipeline_config"],
        "error_log":              obs["error_log"],
        "sample_input_rows":      obs["sample_input_rows"],
        "current_output_rows":    obs["current_output_rows"],
        "expected_output_schema": obs["expected_output_schema"],
        "comparison":             obs["comparison"],
        "step_number":            obs["step_number"],
    }

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    if fix_attempts:
        messages.append({
            "role":    "user",
            "content": json.dumps(initial_context) + "\n\n[No patches applied yet. Diagnose the bug.]"
        })
        messages.extend(_build_history(fix_attempts))
        messages.append({
            "role":    "user",
            "content": (
                f"Current state after {len(fix_attempts)} attempt(s):\n"
                + json.dumps({
                    "pipeline_config":     obs["pipeline_config"],
                    "error_log":           obs["error_log"],
                    "current_output_rows": obs["current_output_rows"],
                    "comparison":          obs["comparison"],
                }, indent=2)
                + "\n\nWhat is your next action?"
            )
        })
    else:
        messages.append({
            "role":    "user",
            "content": json.dumps(initial_context, indent=2)
        })

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    return resp.choices[0].message.content


# ── Task runner ────────────────────────────────────────────────────────────────
def run_task(task_id: str) -> float:
    # Reset environment
    try:
        resp = requests.post(f"{ENV_URL}/reset", json={"task": task_id}, timeout=30)
        resp.raise_for_status()
        obs = resp.json()
    except Exception as e:
        print(f"[START_ERROR] Failed to reset task={task_id}: {e}", flush=True)
        log_start(task_id)
        log_end(success=False, steps=0, score=0.0, rewards=[])
        return 0.0

    log_start(task_id)

    rewards   = []
    step      = 0
    done      = False
    max_steps = {"easy": 4, "medium": 6, "hard": 8}.get(task_id, 8)

    while not done and step < max_steps:
        # Call LLM
        try:
            raw_action = call_llm(obs, obs.get("fix_attempts", []))
            action     = json.loads(raw_action)
        except Exception as e:
            action_str = "LLM_ERROR"
            log_step(step + 1, action_str, 0.0, False, str(e))
            rewards.append(0.0)
            step += 1
            continue

        # Step environment
        try:
            result = requests.post(f"{ENV_URL}/step", json=action, timeout=30).json()
        except Exception as e:
            log_step(step + 1, "REQUEST_ERROR", 0.0, False, str(e))
            rewards.append(0.0)
            step += 1
            continue

        obs    = result["observation"]
        reward = result["reward"]
        done   = result["done"]
        error  = (
            result.get("info", {}).get("pipeline_error")
            or result.get("info", {}).get("patch_error")
        )

        action_str = json.dumps({
            "diagnosis": action.get("diagnosis", "")[:70],
            "patch":     action.get("patch"),
        })

        log_step(step + 1, action_str, reward, done, error)
        rewards.append(reward)
        step += 1
        time.sleep(0.5)

    # Normalise score to [0, 1]
    min_possible  = max_steps * -0.20
    best_possible = 0.70
    raw_score = sum(rewards)
    score = round(min(1.0, max(0.0, (raw_score - min_possible) / (best_possible - min_possible))), 4)
    success = score >= {"easy": 0.80, "medium": 0.75, "hard": 0.70}.get(task_id, 0.70)

    log_end(success=success, steps=step, score=score, rewards=rewards)
    return score


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    all_scores = {}
    for task_id in ["easy", "medium", "hard"]:
        try:
            score = run_task(task_id)
        except Exception as e:
            print(f"[FATAL_TASK_ERROR] task={task_id} error={e}", flush=True)
            score = 0.0
        all_scores[task_id] = score
        time.sleep(3)

    mean_score = round(sum(all_scores.values()) / len(all_scores), 4)
    print(f"\n=== FINAL SCORES ===", flush=True)
    for k, v in all_scores.items():
        print(f"  {k}: {v}", flush=True)
    print(f"  mean: {mean_score}", flush=True)
