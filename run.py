import argparse
import yaml
import pandas as pd
import numpy as np
import json
import logging
import time
import sys


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_config(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    required_fields = ["seed", "window", "version"]
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing config field: {field}")

    return config


def load_data(input_path):
    try:
        df = pd.read_csv(input_path, header=None)
    except FileNotFoundError:
        raise FileNotFoundError("Input file not found")
    except Exception as e:
        raise ValueError(f"CSV error: {str(e)}")

    if df.empty:
        raise ValueError("CSV file is empty")

    # Manually split first column
    df = df[0].str.split(",", expand=True)

    # Set header
    df.columns = df.iloc[0]
    df = df[1:]

    # Clean column names
    df.columns = df.columns.str.strip().str.lower()

    # Convert close column to float
    if "close" not in df.columns:
        raise ValueError(f"Missing required column: close. Found: {df.columns.tolist()}")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["close"])

    return df


def compute_signals(df, window):
    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    df = df.dropna()
    return df


def write_metrics(output_path, metrics):
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("Job started")

    start_time = time.time()

    try:
        # Load config
        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)
        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # Load data
        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        # Processing
        df = compute_signals(df, window)
        logging.info("Rolling mean and signals computed")

        rows_processed = len(df)
        signal_rate = df["signal"].mean()

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        write_metrics(args.output, metrics)

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)

        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        logging.error(f"Error occurred: {str(e)}")
        write_metrics(args.output, error_metrics)

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
