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

# 🔧 Data Pipeline Repair

[![OpenEnv](https://img.shields.io/badge/OpenEnv-Compatible-blueviolet)](https://github.com/openenv/spec)
[![Hugging Face](https://img.shields.io/badge/%F0%9F%A4%97-Hugging%20Face-yellow)](https://huggingface.co/spaces)

An **OpenEnv** compliant environment designed for training and evaluating autonomous data engineering agents. The agent's task is to diagnose and repair broken ETL (Extract, Transform, Load) pipelines so they produce correct, production-grade output.

---

## 📖 Table of Contents
1. [Overview](#-overview)
2. [Technical Architecture](#-technical-architecture)
3. [Tasks & Benchmarks](#-tasks--benchmarks)
4. [Environment Specifications](#-environment-specifications)
5. [Installation & Setup](#-installation--setup)
6. [API Reference](#-api-reference)
7. [Scoring & Reward Logic](#-scoring--reward-logic)

---

## 🔭 Overview
Modern data pipelines often fail silently or crash due to unexpected data anomalies. **Data Pipeline Repair** provides a structured sandbox where an agent interacts with a real Python-based ETL executor to patch configuration bugs.

### Key Capabilities
- **Diagnosis**: Agent receives Python exception logs and data samples.
- **Repair**: Agent applies patches to the `pipeline_config` JSON.
- **Validation**: Deterministic grading based on row-level value matches and schema verification.
- **Warm Baselines**: Realistic starting points ensuring agents solve non-trivial engineering problems.

---

## 🏗 Technical Architecture

The environment is built on three core pillars:

### ⚙️ The Executor (`executor.py`)
A robust Python engine that supports flat configuration schemas. It handles:
- `cast`: Type conversions with robust `null_handling` (keep, drop, coerce).
- `agg`: Aggregations (sum, count, mean) with explicit `output_name` requirements.
- `dedup`: Record deduplication using specific column subsets.
- `join`: Data merging between primary and lookup sets.

### 📝 Task Definitions (`tasks.py`)
Deterministic datasets and broken configurations. We provide 3 distinct scenarios:
- **Easy**: Explicit crash recovery (Date formatting errors).
- **Medium**: Silent schema mismatch (Aggregation column naming).
- **Hard**: Compound bugs (Deduplication key errors + Silent cast drops).

### ⚖️ The Grader (`grader.py`)
A multi-dimensional scoring engine that uses a weighted formula to evaluate the quality of a repair beyond just "does it run?"

---

## 📈 Tasks & Benchmarks

### The "Warm Baseline" Philosophy
Unlike "toy" environments that start at 0% success, this project is calibrated with a professional baseline. This proves the tasks are sophisticated enough that even a "mostly correct" pipeline requires expert-level intervention to reach 100% production readiness.

| Task | Difficulty | Baseline (Broken) | Target (Repaired) |
| :--- | :--- | :---: | :---: |
| **Task 1** | Easy | 0.83 | **1.00** |
| **Task 2** | Medium | 0.81 | **1.00** |
| **Task 3** | Hard | 0.86 | **1.00** |
| **MEAN** | | **0.833** | **1.00** |

---

## 📋 Environment Specifications

Detailed specs are available in `openenv.yaml`.

- **Runtime**: Python 3.11
- **Observation Space**: Dict containing `pipeline_config`, `error_log`, `current_output_rows`, and `comparison`.
- **Action Space**: Dict containing `diagnosis` (text) and `patch` (object).
- **Reward Range**: `[-0.20, 1.0]`

---

## 🚀 Installation & Setup

### Local Development
```bash
# 1. Clone and setup environment
git clone <your-repo>
cd data-pipeline-repair
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure .env
cp .env.example .env
# Edit .env with your HF_TOKEN / API_KEY

# 3. Start Environment Server
uvicorn server.app:app --host 0.0.0.0 --port 7860
```

### Run the Repair Agent
In a separate terminal:
```bash
python inference.py
```

### Run Validation Tests
```bash
pytest tests/ -v
```

---

## 📡 API Reference

| Method | Path | Description |
| :--- | :--- | :--- |
| `POST` | `/reset` | Initialize a task. Body: `{"task": "easy"}` |
| `POST` | `/step` | Apply a patch. Body: `{"diagnosis": "...", "patch": {...}}` |
| `GET` | `/state` | Get current session metadata and reward history. |
| `GET` | `/tasks` | List available task identifiers. |
| `GET` | `/health` | Liveness check for Hugging Face Spaces. |

---

## 💎 Scoring & Reward Logic

The environment provides a high-fidelity reward signal based on the following weights:

| Component | Weight | Description |
| :--- | :---: | :--- |
| **Row Match** | 20% | Correctness of row-level data values. |
| **Schema Match** | 15% | Correctness of column names and structural presence. |
| **DType Match** | 15% | Correctness of data types after casting. |
| **Null Handling** | 10% | Robustness against invalid/null values. |
| **Order** | 10% | Preservation of original data sequence. |
| **Exact Match** | 30% | Full 1-to-1 parity with ground truth metadata. |

**Score Normalization**:
The raw cumulative reward is normalized to `[0.0, 1.0]` where **0.70 raw reward** represents a perfect repair (Full Marks).

---

## 📜 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
