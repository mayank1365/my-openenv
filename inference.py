"""
inference.py — Baseline LLM agent for data-pipeline-repair.

Usage:
  export API_BASE_URL=https://api.openai.com/v1
  export MODEL_NAME=gpt-4o-mini
  export HF_TOKEN=your_token          # used as OpenAI api_key
  export ENV_URL=http://localhost:7860 # or your HF Space URL
  python inference.py
"""
import os
import json
import time
import requests
from openai import OpenAI

API_BASE = os.environ.get("API_BASE_URL", "https://api.openai.com/v1")
MODEL    = os.environ.get("MODEL_NAME",   "gpt-4o-mini")
HF_TOKEN = os.environ.get("HF_TOKEN",    "x")
ENV_URL  = os.environ.get("ENV_URL",     "http://localhost:7860")

client = OpenAI(base_url=API_BASE, api_key=HF_TOKEN)

SYSTEM_PROMPT = """You are a data engineering agent. You debug broken ETL pipeline configs.

At each step you receive a JSON observation. You must respond with a single JSON object:
{
  "diagnosis": "<brief hypothesis about the root cause>",
  "patch": {
    "step_index": <int: which step in pipeline_config.steps to fix>,
    "field": "<dot.notation.path e.g. params.null_handling>",
    "old_value": <current value, optional>,
    "new_value": <replacement value>
  },
  "validate_only": false
}

Rules:
1. Always include a "diagnosis" string explaining your hypothesis.
2. Only include "patch" when you have identified a specific fix.
3. Set "validate_only": true (and omit "patch") to inspect the pipeline output without making a change.
4. To fix the pipeline you must make the current_output_rows match the expected_output_schema.
5. When error_log is empty, compare current_output_rows vs expected_output_schema carefully — there may be silent bugs (wrong values, wrong row count, null fields).
6. The patch field uses dot-notation: "params.null_handling" edits pipeline_config.steps[step_index]["params"]["null_handling"].
7. Do not patch steps that are not causing the problem.
8. If row_match in comparison is 1.0, respond with {"diagnosis": "pipeline is fixed", "validate_only": true}.

Respond ONLY with valid JSON. No markdown, no preamble."""


def call_llm(messages: list[dict]) -> str:
    resp = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    return resp.choices[0].message.content


def run_task(task_id: str) -> float:
    # Reset environment
    obs = requests.post(f"{ENV_URL}/reset", json={"task": task_id}, timeout=30).json()
    pipeline_name = obs.get("pipeline_name", task_id)
    print(f"[START] task={task_id} pipeline={pipeline_name}")

    rewards = []
    step    = 0
    done    = False
    max_steps = {"easy": 4, "medium": 6, "hard": 8}.get(task_id, 8)

    while not done and step < max_steps:
        # Build LLM prompt
        user_content = json.dumps({
            "pipeline_config":        obs["pipeline_config"],
            "error_log":              obs["error_log"],
            "sample_input_rows":      obs["sample_input_rows"],
            "current_output_rows":    obs["current_output_rows"],
            "expected_output_schema": obs["expected_output_schema"],
            "comparison":             obs["comparison"],
            "fix_attempts":           obs["fix_attempts"],
            "step_number":            obs["step_number"],
        }, indent=2)

        messages = [
            {"role": "system",  "content": SYSTEM_PROMPT},
            {"role": "user",    "content": user_content},
        ]

        try:
            raw_action = call_llm(messages)
            action     = json.loads(raw_action)
        except Exception as e:
            print(f"[STEP] step={step+1} action=LLM_ERROR reward=0 done=False error={e}")
            rewards.append(0.0)
            step += 1
            continue

        # Send action to environment
        try:
            result  = requests.post(f"{ENV_URL}/step", json=action, timeout=30).json()
        except Exception as e:
            print(f"[STEP] step={step+1} action=REQUEST_ERROR reward=0 done=False error={e}")
            rewards.append(0.0)
            step += 1
            continue

        obs    = result["observation"]
        reward = result["reward"]
        done   = result["done"]
        error  = result.get("info", {}).get("pipeline_error") or result.get("info", {}).get("patch_error")

        action_summary = json.dumps({
            "diagnosis":  action.get("diagnosis", "")[:60],
            "patch":      action.get("patch"),
        })
        print(f"[STEP] step={step+1} action={action_summary} reward={reward} done={done} error={error}")
        rewards.append(reward)
        step += 1

        time.sleep(0.5)   # be polite to the LLM API

    # Normalize score to [0, 1]
    min_possible = max_steps * -0.20
    best_possible = 0.70
    raw_score = sum(rewards)
    score = round(min(1.0, max(0.0, (raw_score - min_possible) / (best_possible - min_possible))), 4)
    success = score >= {"easy": 0.80, "medium": 0.75, "hard": 0.70}.get(task_id, 0.70)

    print(f"[END] success={success} steps={step} score={score} rewards={rewards}")
    return score


if __name__ == "__main__":
    all_scores = {}
    for task_id in ["easy", "medium", "hard"]:
        score = run_task(task_id)
        all_scores[task_id] = score
        time.sleep(3)

    mean_score = round(sum(all_scores.values()) / len(all_scores), 4)
    print(f"\n=== FINAL SCORES ===")
    for k, v in all_scores.items():
        print(f"  {k}: {v}")
    print(f"  mean: {mean_score}")
