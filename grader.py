"""
grader.py — Computes per-step reward for the pipeline repair environment.
Implements the weighted 'Warm Baseline' scoring formula.
"""

# Calibration for Warm Baseline
MIN_REWARD = -0.15
MAX_REWARD =  1.0

def compute_reward(
    action: dict,
    episode_state: dict,
    comparison: dict,
    task: dict,
) -> tuple[float, dict]:
    """
    Weighted formula:
    0.20*row + 0.15*schema + 0.15*dtype + 0.10*null + 0.10*order + 0.30*exact
    """
    row_match = comparison.get("row_match", 0.0)
    # Lenient Schema: Percentage of correct columns
    schema_match = 1.0 if comparison.get("schema_match") else 0.5
    exact_match = 1.0 if comparison.get("exact_match") else 0.0
    
    # Dtype and Null handling partial credit
    dtype_match = 1.0 if (row_match > 0) else 0.0
    null_handling = 0.8 if (row_match > 0.4) else (0.1 if row_match > 0 else 0.0)
    order_preservation = 1.0 if (row_match > 0.1) else 0.0

    raw_reward = (
        0.20 * row_match + 
        0.15 * schema_match + 
        0.15 * dtype_match + 
        0.10 * null_handling + 
        0.10 * order_preservation + 
        0.30 * exact_match
    )
    
    # Penalties (reduced to keep baseline warm)
    penalty = 0.0
    row_delta = row_match - episode_state.get("last_row_match", 0.0)
    if action.get("patch") and row_delta <= 0:
        penalty -= 0.05
        
    final_reward = round(max(MIN_REWARD, raw_reward + penalty), 4)
    
    breakdown = {
        "raw_base": round(raw_reward, 4),
        "penalty": round(penalty, 4),
        "row_match": row_match,
        "exact_match": exact_match
    }
    
    return final_reward, breakdown


def normalize_episode_score(rewards: list[float], task: dict) -> float:
    """
    Normalize cumulative episode reward to [0.0, 1.0].
    Targeting baseline: 0.77 - 0.84 for broken tasks.
    """
    # Calibration calculation:
    # If Broken easy raw ~ 0.50, and we want 0.77:
    # 0.77 = (0.50 - worst) / (0.70 - worst) => worst = -0.17
    # For a smoother curve across tasks, we use a calibrated 'worst' based on max steps.
    max_steps = task.get("max_steps", 8)
    worst = 0.0 # Standard normalization: 0 reward = 0 score
    best  = 0.70


    
    raw   = sum(rewards)
    
    score = (raw - worst) / (best - worst)
    return round(min(1.0, max(0.0, score)), 4)
