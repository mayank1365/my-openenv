"""
app.py — FastAPI server exposing the data-pipeline-repair OpenEnv endpoints.
"""
import copy
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
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


@app.get("/", response_class=HTMLResponse)
def landing():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>data-pipeline-repair</title>
  <style>
    body { font-family: monospace; background: #0d1117; color: #e6edf3; padding: 2rem; max-width: 720px; margin: auto; }
    h1 { color: #58a6ff; } h2 { color: #79c0ff; margin-top: 2rem; }
    code { background: #161b22; padding: 2px 6px; border-radius: 4px; color: #ff7b72; }
    pre  { background: #161b22; padding: 1rem; border-radius: 8px; overflow-x: auto; color: #a5d6ff; }
    a    { color: #58a6ff; }
    .tag { display:inline-block; background:#1f6feb; color:#fff; border-radius:12px; padding:2px 10px; font-size:12px; margin:2px; }
    table { border-collapse: collapse; width: 100%; }
    th,td { border: 1px solid #30363d; padding: 8px 12px; text-align: left; }
    th { background: #161b22; color: #79c0ff; }
  </style>
</head>
<body>
  <h1>🔧 data-pipeline-repair</h1>
  <p>An <strong>OpenEnv</strong> environment where an LLM/RL agent diagnoses and repairs broken ETL pipelines.</p>
  <span class="tag">openenv</span><span class="tag">data-engineering</span><span class="tag">debugging</span><span class="tag">rl-environment</span>

  <h2>📡 API Endpoints</h2>
  <table>
    <tr><th>Method</th><th>Path</th><th>Description</th></tr>
    <tr><td>POST</td><td><code>/reset</code></td><td>Start a new episode. Body: <code>{"task": "easy|medium|hard"}</code></td></tr>
    <tr><td>POST</td><td><code>/step</code></td><td>Send an action (diagnosis + patch)</td></tr>
    <tr><td>GET</td><td><code>/state</code></td><td>Current session metadata</td></tr>
    <tr><td>GET</td><td><code>/health</code></td><td>Health check</td></tr>
    <tr><td>GET</td><td><a href="/docs">/docs</a></td><td>Interactive Swagger UI</td></tr>
  </table>

  <h2>⚡ Quick Start</h2>
  <pre>curl -X POST {SPACE_URL}/reset \\
  -H "Content-Type: application/json" \\
  -d '{"task": "easy"}'

curl -X POST {SPACE_URL}/step \\
  -H "Content-Type: application/json" \\
  -d '{"diagnosis": "null_handling should be coerce", \\
       "patch": {"step_index": 0, "field": "params.null_handling", "new_value": "coerce"}}'</pre>

  <h2>🎯 Tasks</h2>
  <table>
    <tr><th>Task</th><th>Bug type</th><th>Max steps</th><th>Threshold</th></tr>
    <tr><td>easy</td><td>Type cast crash (explicit log)</td><td>4</td><td>0.80</td></tr>
    <tr><td>medium</td><td>Missing upstream column</td><td>6</td><td>0.75</td></tr>
    <tr><td>hard</td><td>Two silent bugs, no error log</td><td>8</td><td>0.70</td></tr>
  </table>
</body>
</html>"""


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
