"""
grader.py — Computes per-step reward for the pipeline repair environment.

Reward components (values are additive, clamped to [-0.20, 0.35]):
  +0.10  diagnosis keyword hit (awarded once per episode)
  +0.10  schema now matches expected (awarded once per episode)
  +0.20  proportional to improvement in row_match since last step
  +0.30  bonus when row_match reaches 1.0 (awarded once)
  -0.10  patch applied but no row_match improvement (wasted patch)
  -0.20  patch targets a step index not in gold_patch_step_indices
  -0.05  any action taken after episode is already solved
"""

MIN_REWARD = -0.20
MAX_REWARD =  0.35


def compute_reward(
    action: dict,
    episode_state: dict,
    comparison: dict,
    task: dict,
) -> tuple[float, dict]:
    """
    Returns (reward_value, breakdown_dict).

    episode_state must contain:
        last_row_match: float        (row_match from previous step)
        schema_matched_once: bool    (True once schema has matched)
        diagnosis_rewarded: bool     (True once keyword hit has been given)
        solved: bool                 (True once row_match == 1.0)
    """
    reward = 0.0
    breakdown = {
        "diagnosis": 0.0,
        "schema":    0.0,
        "rows":      0.0,
        "bonus":     0.0,
        "penalty":   0.0,
    }

    # Already solved — penalize extra steps
    if episode_state["solved"]:
        r = -0.05
        breakdown["penalty"] += r
        reward += r
        return round(max(MIN_REWARD, min(MAX_REWARD, reward)), 4), breakdown

    # Diagnosis keyword reward (once per episode)
    if not episode_state["diagnosis_rewarded"]:
        diagnosis_lower = action.get("diagnosis", "").lower()
        keywords = task.get("gold_diagnosis_keywords", [])
        if any(kw.lower() in diagnosis_lower for kw in keywords):
            r = 0.10
            breakdown["diagnosis"] += r
            reward += r
            episode_state["diagnosis_rewarded"] = True

    # Schema match reward (once per episode)
    if not episode_state["schema_matched_once"] and comparison.get("schema_match"):
        r = 0.10
        breakdown["schema"] += r
        reward += r
        episode_state["schema_matched_once"] = True

    # Row match improvement reward (proportional)
    row_delta = comparison["row_match"] - episode_state["last_row_match"]
    if row_delta > 0:
        r = round(0.20 * row_delta, 4)
        breakdown["rows"] += r
        reward += r

    # Solve bonus (once)
    if comparison["row_match"] >= 1.0 and not episode_state["solved"]:
        r = 0.30
        breakdown["bonus"] += r
        reward += r
        episode_state["solved"] = True

    # Penalty: patch applied but no improvement
    if action.get("patch") and row_delta <= 0:
        r = -0.10
        breakdown["penalty"] += r
        reward += r

    # Penalty: patch targets wrong step index
    if action.get("patch"):
        patched_step = action["patch"].get("step_index", -1)
        gold_indices = task.get("gold_patch_step_indices", [])
        if gold_indices and patched_step not in gold_indices:
            r = -0.20
            breakdown["penalty"] += r
            reward += r

    final = round(max(MIN_REWARD, min(MAX_REWARD, reward)), 4)
    return final, breakdown


def normalize_episode_score(rewards: list[float], task: dict) -> float:
    """
    Normalize cumulative episode reward to [0.0, 1.0].
    Worst case: max_steps × MIN_REWARD
    Best case:  0.10 + 0.10 + 0.20 + 0.30 = 0.70 (single-bug tasks)
    """
    max_steps = task.get("max_steps", 8)
    worst = max_steps * MIN_REWARD          # e.g. 8 × -0.20 = -1.60
    best  = 0.70                            # theoretical max reward
    raw   = sum(rewards)
    score = (raw - worst) / (best - worst)
    return round(min(1.0, max(0.0, score)), 4)
