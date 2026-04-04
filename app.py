"""
app.py — FastAPI server exposing the data-pipeline-repair OpenEnv endpoints.
"""
import copy
from fastapi import FastAPI, HTTPException
from executor import run_pipeline, apply_patch, compare_output
from grader import compute_reward, normalize_episode_score
from tasks import TASKS
from models import Action, Observation

app = FastAPI(title="data-pipeline-repair", version="1.0.0")

# Single in-memory session (sufficient for HF Space / single agent)
_session: dict = {}


def _build_observation(state: dict) -> dict:
    task = state["task"]
    current_rows, error = run_pipeline(state["config"], task["sample_input_rows"])
    comparison = compare_output(current_rows, task["expected_output_rows"])
    return {
        "pipeline_name":         task["pipeline_name"],
        "pipeline_config":       state["config"],
        "error_log":             error if error else task.get("error_log", ""),
        "input_schema":          task["input_schema"],
        "expected_output_schema":task["expected_output_schema"],
        "sample_input_rows":     task["sample_input_rows"],
        "current_output_rows":   current_rows,
        "comparison":            comparison,
        "fix_attempts":          state["fix_attempts"],
        "step_number":           state["step"],
    }


@app.post("/reset")
def reset(body: dict = {}):
    task_id = body.get("task", "easy")
    if task_id not in TASKS:
        raise HTTPException(400, f"Unknown task: {task_id!r}. Choose from {list(TASKS)}")

    task = copy.deepcopy(TASKS[task_id])
    _session.update({
        "task":                  task,
        "task_id":               task_id,
        "config":                copy.deepcopy(task["pipeline_config"]),
        "step":                  0,
        "done":                  False,
        "rewards":               [],
        "fix_attempts":          [],
        # grader state
        "last_row_match":        0.0,
        "schema_matched_once":   False,
        "diagnosis_rewarded":    False,
        "solved":                False,
    })
    return _build_observation(_session)


@app.post("/step")
def step(action: dict):
    if not _session:
        raise HTTPException(400, "Call /reset first")
    if _session["done"]:
        return {
            "observation": _build_observation(_session),
            "reward": 0.0, "done": True, "info": {"error": "episode_already_done"}
        }

    task    = _session["task"]
    max_steps = task.get("max_steps", 8)

    # Apply patch if provided
    error_msg = None
    if action.get("patch") and not action.get("validate_only"):
        try:
            _session["config"] = apply_patch(_session["config"], action["patch"])
        except Exception as e:
            error_msg = str(e)

    # Run pipeline with current config
    current_rows, pipeline_error = run_pipeline(
        _session["config"], task["sample_input_rows"]
    )
    comparison = compare_output(current_rows, task["expected_output_rows"])

    # Compute reward
    reward, breakdown = compute_reward(
        action=action,
        episode_state=_session,   # mutated in-place by compute_reward
        comparison=comparison,
        task=task,
    )
    _session["last_row_match"] = comparison["row_match"]
    _session["rewards"].append(reward)
    _session["step"] += 1

    # Record attempt
    if action.get("patch"):
        _session["fix_attempts"].append({
            "step":       _session["step"],
            "patch":      action.get("patch"),
            "diagnosis":  action.get("diagnosis", ""),
            "row_match":  comparison["row_match"],
            "reward":     reward,
        })

    # Check termination
    solved = comparison["row_match"] >= 1.0
    done   = solved or _session["step"] >= max_steps
    _session["done"] = done

    score = normalize_episode_score(_session["rewards"], task)

    return {
        "observation": {
            "pipeline_name":          task["pipeline_name"],
            "pipeline_config":        _session["config"],
            "error_log":              pipeline_error or task.get("error_log", ""),
            "input_schema":           task["input_schema"],
            "expected_output_schema": task["expected_output_schema"],
            "sample_input_rows":      task["sample_input_rows"],
            "current_output_rows":    current_rows,
            "comparison":             comparison,
            "fix_attempts":           _session["fix_attempts"],
            "step_number":            _session["step"],
        },
        "reward":  reward,
        "done":    done,
        "info": {
            "breakdown":      breakdown,
            "score_so_far":   score,
            "pipeline_error": pipeline_error,
            "patch_error":    error_msg,
            "solved":         solved,
            "steps_used":     _session["step"],
            "max_steps":      max_steps,
        }
    }


@app.get("/state")
def get_state():
    if not _session:
        return {"status": "no_session"}
    task = _session.get("task", {})
    return {
        "task_id":     _session.get("task_id"),
        "step":        _session.get("step", 0),
        "done":        _session.get("done", False),
        "solved":      _session.get("solved", False),
        "rewards":     _session.get("rewards", []),
        "score":       normalize_episode_score(
                           _session.get("rewards", []), task),
        "row_match":   _session.get("last_row_match", 0.0),
        "max_steps":   task.get("max_steps", 8),
    }


@app.get("/health")
def health():
    return {"status": "ok"}
