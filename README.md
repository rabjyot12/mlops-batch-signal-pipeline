# MLOps Batch Signal Pipeline

A reproducible, Dockerized MLOps batch pipeline that generates trading signals using rolling averages with structured logging and metrics tracking.

---

## 🚀 Overview

This project implements a minimal production-style batch job that:

* Loads configuration from YAML
* Processes OHLCV market data
* Computes rolling mean on closing prices
* Generates binary trading signals
* Outputs structured metrics (`metrics.json`)
* Logs execution details (`run.log`)
* Runs locally and inside Docker

---

## ⚙️ Features

* **Reproducibility**: Deterministic runs using seed from config
* **Observability**: Detailed logs + machine-readable metrics
* **Error Handling**: Graceful handling of invalid inputs
* **Dockerized**: One-command container execution
* **No Hardcoding**: All paths passed via CLI

---

## 📁 Project Structure

```
mlops-batch-signal-pipeline/
│
├── run.py              # Main pipeline script
├── config.yaml         # Configuration file
├── data.csv            # Input dataset (OHLCV)
├── requirements.txt    # Dependencies
├── Dockerfile          # Container setup
├── metrics.json        # Sample output (success)
├── run.log             # Sample logs
├── README.md           # Documentation
```

---

## ▶️ How to Run Locally

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the pipeline

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

---

## 🐳 Docker Usage

### Build image

```bash
docker build -t mlops-task .
```

### Run container

```bash
docker run --rm mlops-task
```

---

## 📊 Example Output

### `metrics.json`

```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 29,
  "seed": 42,
  "status": "success"
}
```

---

## 🧾 Logging (`run.log`)

Example:

```
2026-04-23 12:00:00 - INFO - Job started
2026-04-23 12:00:00 - INFO - Config loaded: seed=42, window=5, version=v1
2026-04-23 12:00:00 - INFO - Rows loaded: 10000
2026-04-23 12:00:00 - INFO - Rolling mean and signals computed
2026-04-23 12:00:00 - INFO - Metrics generated
2026-04-23 12:00:00 - INFO - Job completed successfully
```

---

## 🧠 Processing Logic

1. Load config (`seed`, `window`, `version`)
2. Read dataset and validate schema
3. Compute rolling mean on `close` column
4. Generate signal:

   * `1` if close > rolling_mean
   * `0` otherwise
5. Drop initial rows without rolling values
6. Compute:

   * `rows_processed`
   * `signal_rate`
   * `latency_ms`

---

## ❗ Error Handling

The pipeline handles:

* Missing input file
* Invalid CSV format
* Empty dataset
* Missing `close` column
* Invalid configuration

Even on failure, `metrics.json` is always generated:

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of error"
}
```

---

## 🛠️ Tech Stack

* Python 3.9
* pandas
* numpy
* PyYAML
* Docker

---

## 📌 Notes

* First `window-1` rows are excluded due to rolling computation
* Output is deterministic due to fixed seed
* Designed to mimic real-world ML pipeline behavior

---


