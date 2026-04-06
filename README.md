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

In this environment, an agent receives a broken pipeline configuration (JSON), input data rows, an expected output schema, and an error log. The agent interactively patches the pipeline step-by-step using structural mutations, validating its changes against a pure-Python embedded execution engine.

### Features
- **Deterministic Tasks**: Pre-configured Easy, Medium, and Hard data pipeline scenarios.
- **Embedded pure-Python Engine**: Executes ETL operations (`cast`, `join`, `agg`, etc.) with zero external data dependencies like Pandas or Numpy.
- **OpenEnv Compliant REST API**: Exposes standardized `/reset`, `/step`, and `/state` validation loops.
- **Strict Grading System**: Evaluates and rewards fixes based on root cause identification and exact row array matches.
- **Pre-built Baseline Agent**: Includes an agent framework (`inference.py`) with anti-oscillation behavior and multi-turn logic.

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker (Optional, for containerized execution)
- API Keys for the LLM inference engine (e.g., Groq, OpenAI).

### Installation

**Option A: Virtual Environment (Local Python)**
1. Clone the repository and navigate into it.
2. Initialize a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

**Option B: Docker**
1. Build the image:
   ```bash
   docker build -t pipeline-repair .
   ```

---

## Configuration

The baseline agent requires authentication tokens for the LLM execution. 

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and configure your keys:
   ```env
   API_BASE_URL=https://api.groq.com/openai/v1
   MODEL_NAME=llama-3.3-70b-versatile
   HF_TOKEN=your_api_key_here
   ENV_URL=http://localhost:7860
   ```

---

## Usage

### Running Locally
To launch the OpenEnv simulation server natively:
```bash
uvicorn app:app --host 0.0.0.0 --port 7860
```
Verify the server is running by viewing the landing page at `http://localhost:7860`.

### Running with Docker
```bash
docker run -p 7860:7860 pipeline-repair
```

### Running the LLM Agent
Once the environment server is running, execute the baseline agent to attempt the repairs automatically:
```bash
python inference.py
```

---

## Architecture / Design

The repository is highly modularized, separating environment execution mechanics from the REST API boundary:

- **`executor.py`**: Pure-Python ETL interpreter. Evaluates the pipeline logic and processes the rows based on the JSON configs.
- **`tasks.py`**: Source of truth for task scenarios. Contains broken configs, inputs, expected states, and "gold" solutions.
- **`grader.py`**: Reward engine. Analyzes the delta between the expected output and current output to distribute rewards or penalties.
- **`app.py`**: FastAPI wrapper managing session states and routing.
- **`inference.py`**: The baseline LLM agent loop that dynamically interacts with the `/reset` and `/step` endpoints.
- **`openenv.yaml`**: Standardized metadata declaring action schemas, observation schemas, and task outlines.

---

## API / Endpoints

The environment adheres to the strict OpenEnv interaction methodology.

### 1. `POST /reset`
Initialize a task scenario.
```bash
curl -X POST http://localhost:7860/reset \
  -H "Content-Type: application/json" \
  -d '{"task":"easy"}'
```

### 2. `POST /step`
Submit a diagnosis and a JSON patch payload modifying the current pipeline definition.
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

### 3. `GET /state`
Retrieve cumulative metadata including reward tracking, depth, and success status.
```bash
curl http://localhost:7860/state
```

---

## Tasks / Scenarios

| Difficulty | Description | Max Steps | Success Threshold |
|---|---|:---:|:---:|
| **Easy** | **Explicit Type Error**: The pipeline crashes with a `ValueError` when casting missing values. Patch `null_handling` to `coerce`. | 4 | 0.80 |
| **Medium** | **Upstream Keys Dropped**: An upstream `select` step drops a column needed downstream. Find the root step and include the column. | 6 | 0.75 |
| **Hard** | **Silent Bugs**: Pipeline completes but data is wrong. Requires fixing broken date strings and joining duplicate deduplication parameters. | 8 | 0.70 |

---

## Testing / Validation

To independently verify that the environment conforms to the strict OpenEnv evaluation specs:

1. Install the CLI validator:
   ```bash
   pip install openenv-cli
   ```
2. Validate against a local or remote environment:
   ```bash
   python -m openenv validate http://localhost:7860
   ```

### Benchmark Reference
Scores achieved using `llama-3.3-70b-versatile`:
- Easy: 0.77 (Solved 1 step)
- Medium: 0.82
- Hard: 0.96 
- **Mean: 0.85**

---

## Deployment 

This environment is pre-configured to deploy seamlessly to Hugging Face Spaces using the Docker SDK.

1. Ensure `app_port: 7860` and `sdk: docker` are present in the repository's Markdown Frontmatter.
2. Push your code directly to the underlying Git infrastructure of your Hugging Face Space. The Docker build will trigger automatically.

---

## Troubleshooting / FAQ

- **Agent rate limits**: If you encounter 429 status codes during the `inference.py` loop, increase the `time.sleep()` parameter inside the `inference.py` execution block.
- **Port conflicts**: If port 7860 is taken locally, alter the Uvicorn port binding (`--port 8000`), but ensure you update the `ENV_URL` in `.env` to match. 

---

## Links

- **HF Space (live):** [huggingface.co/spaces/Hollow-Abyss/data-pipeline-repair](https://huggingface.co/spaces/Hollow-Abyss/data-pipeline-repair)
- **Swagger UI:** [hollow-abyss-data-pipeline-repair.hf.space/docs](https://hollow-abyss-data-pipeline-repair.hf.space/docs)
- **GitHub:** [github.com/mayank1365/data-pipeline-repair-RL](https://github.com/mayank1365/data-pipeline-repair-RL)

---
