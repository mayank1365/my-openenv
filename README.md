---
title: data-pipeline-repair
emoji: 🔧
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 7860
tags:
  - openenv
  - data-engineering
  - debugging
  - rl-environment
  - llm-agent
license: mit
---

# 🔧 data-pipeline-repair-RL

An [OpenEnv](https://github.com/openenv/openenv) environment where an LLM or Reinforcement Learning agent diagnoses and repairs broken ETL data pipelines.

In this environment, an agent is given a broken pipeline configuration (JSON), input data rows, an expected output schema, and an error log. The agent must interactively patch the pipeline step-by-step using structural mutations, validating its changes against a pure-Python embedded execution engine.

---

## 🏗️ Repository Architecture

The repository is modularized into distinct components, keeping environment execution entirely decoupled from the REST API layer:

| Component | Responsibility |
|---|---|
| [`executor.py`](./executor.py) | A pure-Python ETL interpreter (no pandas/numpy required). Implements operations like `cast`, `select`, `rename`, `filter`, `date_parse`, `join`, and `agg`. |
| [`tasks.py`](./tasks.py) | Definitive set of deterministic tasks (Easy, Medium, Hard). Includes the broken configs, inputs, expected states, and "gold" solutions. |
| [`grader.py`](./grader.py) | Reward computation engine. Grades the agent at each step based on schema fix, row matches, exact root cause keyword detection, and penalizes bad patches. |
| [`app.py`](./app.py) | FastAPI service wrapping the execution environment into the `/reset`, `/step`, and `/state` interaction loops compliant with OpenEnv constraints. |
| [`inference.py`](./inference.py) | The baseline agent script implementing an LLM loop reacting to OpenEnv API observations. By default uses `llama-3.3-70b-versatile`. |
| [`openenv.yaml`](./openenv.yaml) | Standardized metadata for LLM evaluation declaring observation schemas, action schemas, and reward structures. |

---

## 🎯 Task Scenarios

| Difficulty | Description | Max Steps | Success Threshold |
|---|---|:---:|:---:|
| **Easy** | **Explicit Type Error**: The pipeline crashes with a `ValueError` when casting missing values. The agent must locate the cast step and switch `null_handling` from "error" to "coerce". | 4 | 0.80 |
| **Medium** | **Upstream Keys Dropped**: An upstream `select` step silently drops a column needed by a downstream `agg` step. The agent must trace the error log back through the pipeline graph to find the root cause. | 6 | 0.75 |
| **Hard** | **Silent Bugs + State Bleed**: The pipeline completes but outputs incorrect data. Bug 1: Improper date format drops all timestamp elements. Bug 2: Duplicate join keys inflate sum aggregations. Two sequential patches are required. | 8 | 0.70 |

---

## 🚀 Quickstart: Running Locally

You can run the environment natively or via Docker. The application uses port `7860` locally.

### Option A: Docker (Recommended)
```bash
docker build -t pipeline-repair .
docker run -p 7860:7860 pipeline-repair
```

### Option B: Local Python Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 7860
```

Verify it's running by hitting the healthcheck or observing the landing page at `http://localhost:7860`.

---

## 🕹️ Interaction API (OpenEnv Compliant)

### 1. Reset Environment (`POST /reset`)
Initialize a task scenario.
```bash
curl -X POST http://localhost:7860/reset \
  -H "Content-Type: application/json" \
  -d '{"task":"easy"}'
```

### 2. Step Environment (`POST /step`)
Provide a diagnosis and a JSON patch payload modifying the pipeline definition.
```bash
curl -X POST http://localhost:7860/step \
  -H "Content-Type: application/json" \
  -d '{
        "diagnosis": "null_handling is throwing errors on empty records",
        "patch": {
          "step_index": 0,
          "field":      "params.null_handling",
          "new_value":  "coerce"
        }
      }'
```

### 3. Check State (`GET /state`)
Retrieve cumulative reward stats, step depths, task status.
```bash
curl http://localhost:7860/state
```

---

## 🧠 Running the Baseline Agent

We include an `inference.py` loop which acts as a robust baseline LLM agent. It features anti-oscillation behavior, history interleaving for multi-turn learning, and intelligent fallback on keys.

1. **Copy the environment template**
   ```bash
   cp .env.example .env
   # Update .env with your chosen LLM Key (e.g. OPENAI_API_KEY)
   ```
2. **Execute the agent loop:**
   ```bash
   python inference.py
   ```

A benchmark run across all modes (Easy, Medium, Hard) will output its final normalized percentage score matrix based on the reward engine evaluation.
