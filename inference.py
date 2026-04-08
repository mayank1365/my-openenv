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
MODEL_NAME   = os.environ.get("MODEL_NAME", "llama-3.3-70b-versatile")
ENV_URL      = os.getenv("ENV_URL") or "https://hollow-abyss-my-env.hf.space"
BENCHMARK    = "data-pipeline-repair"

# ── OpenAI client (lazy init) ──────────────────────────────────────────────────
_client = None

def get_client() -> OpenAI:
    global _client
    if _client is not None:
        return _client

    try:
        base_url = os.environ["API_BASE_URL"].strip()
        api_key  = os.environ["API_KEY"].strip()

        # DEBUG logs (added)
        print(f"DEBUG ENV BASE_URL: {base_url}", flush=True)
        print(f"DEBUG ENV API_KEY: {api_key[:5]}***", flush=True)
        print(f"DEBUG: Initializing proxy client with base_url={base_url[:15]}...", flush=True)

        _client = OpenAI(base_url=base_url, api_key=api_key)
        return _client
    except KeyError as e:
        raise EnvironmentError(
            f"Missing mandatory environment variable: {e}. "
            "Validator injection of API_BASE_URL and API_KEY is REQUIRED."
        )
    except Exception as e:
        raise RuntimeError(f"Failed to initialize LiteLLM proxy client: {e}")


# ── Stdout logging helpers ────────────────────────────────────────────────────
def log_start(task: str) -> None:
    print(f"DEBUG: Agent Version 2026-04-08-1456", flush=True)
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
1. READ error_log FIRST...
(unchanged)
Respond ONLY with valid JSON. No markdown, no explanations outside the JSON.
"""


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
    import requests

    base_url = os.environ["API_BASE_URL"].strip()
    api_key  = os.environ["API_KEY"].strip()

    url = f"{base_url}/chat/completions"

    initial_context = {
        "pipeline_config": obs["pipeline_config"],
        "error_log": obs["error_log"],
        "sample_input_rows": obs["sample_input_rows"],
        "current_output_rows": obs["current_output_rows"],
        "expected_output_schema": obs["expected_output_schema"],
        "comparison": obs["comparison"],
        "step_number": obs["step_number"],
    }

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(initial_context)}
    ]

    print("DEBUG: FORCING PROXY CALL...", flush=True)

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL_NAME,
            "messages": messages,
            "temperature": 0.0,
        },
        timeout=30,
    )

    print(f"DEBUG: STATUS {response.status_code}", flush=True)

    response.raise_for_status()
    data = response.json()

    return data["choices"][0]["message"]["content"]

# ── Task runner ────────────────────────────────────────────────────────────────
def run_task(task_id: str) -> float:
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

    rewards = []
    step = 0
    done = False
    max_steps = {"easy": 4, "medium": 6, "hard": 8}.get(task_id, 8)
    print("DEBUG: Starting task loop", flush=True)
    while not done and step < max_steps:
        # FIXED: separate try blocks
        try:
            raw_action = call_llm(obs, obs.get("fix_attempts", []))
        except Exception as e:
            log_step(step + 1, "LLM_CALL_ERROR", 0.0, False, str(e))
            rewards.append(0.0)
            step += 1
            continue

        try:
            action = json.loads(raw_action)
        except Exception as e:
            log_step(step + 1, "JSON_PARSE_ERROR", 0.0, False, str(e))
            rewards.append(0.0)
            step += 1
            continue

        try:
            result = requests.post(f"{ENV_URL}/step", json=action, timeout=30).json()
        except Exception as e:
            log_step(step + 1, "REQUEST_ERROR", 0.0, False, str(e))
            rewards.append(0.0)
            step += 1
            continue

        obs = result["observation"]
        reward = result["reward"]
        done = result["done"]
        error = (
            result.get("info", {}).get("pipeline_error")
            or result.get("info", {}).get("patch_error")
        )

        action_str = json.dumps({
            "diagnosis": action.get("diagnosis", "")[:70],
            "patch": action.get("patch"),
        })

        log_step(step + 1, action_str, reward, done, error)
        rewards.append(reward)
        step += 1
        time.sleep(0.5)

    min_possible = max_steps * -0.20
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