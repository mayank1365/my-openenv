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

# data-pipeline-repair-RL

An OpenEnv environment where an LLM/RL agent debugs broken ETL pipelines.

## Quickstart

```bash
docker build -t pipeline-repair .
docker run -p 7860:7860 pipeline-repair
```

Then in another terminal:

```bash
# Reset to easy task
curl -X POST http://localhost:7860/reset -H "Content-Type: application/json" -d '{"task":"easy"}'

# Send an action
curl -X POST http://localhost:7860/step -H "Content-Type: application/json" \
  -d '{"diagnosis":"null_handling should be coerce","patch":{"step_index":0,"field":"params.null_handling","new_value":"coerce"}}'
```

## Tasks

| Task   | Bug type                      | Max steps | Threshold |
|--------|-------------------------------|-----------|-----------|
| easy   | Type cast crash (explicit log)| 4         | 0.80      |
| medium | Missing upstream column       | 6         | 0.75      |
| hard   | Two silent bugs, no error log | 8         | 0.70      |

## Running the baseline agent

```bash
export API_BASE_URL=https://api.openai.com/v1
export MODEL_NAME=gpt-4o-mini
export HF_TOKEN=sk-...
export ENV_URL=http://localhost:7860
python inference.py
```

## Environment spec

See `openenv.yaml` for the full observation/action/reward schema.
