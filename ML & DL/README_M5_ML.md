# M5 — ML Pipeline (5-Day Stock Return Prediction)

This covers the ML half of M5: `stock_return_pipeline.py`, which trains and
evaluates the return-prediction models on the clean data produced upstream
by M2 (Spark Streaming → HDFS/Parquet) and M3 (Spark SQL transforms), and
the Streamlit ML tab that reads its output.

## 1. What this pipeline does

For each ticker it builds leakage-safe 20-day feature windows, predicts the
**5-day forward return**, trains 6 models (Naive, Ridge, Huber,
RandomForest, HistGradientBoosting, XGBoost, LSTM), picks the best one by
mean test RMSE, and writes out predictions + evaluation reports.

This is the same modeling logic as the original R&D notebook
(`us-stock-prediction.ipynb`) — only the data source changed, from local
Kaggle CSVs to the team's Spark/Parquet pipeline.

## 2. Dependencies

Install on the machine / Spark cluster that runs the script:

```bash
pip install pyspark pandas numpy scikit-learn xgboost torch joblib
```

Notes:
- `pyspark` — only used to read the Parquet and hand each ticker's data to
  pandas; the actual model training is plain scikit-learn / XGBoost /
  PyTorch, same as the notebook.
- `torch` — CPU is fine. If a GPU is available on the machine, the script
  auto-detects it (`DEVICE = "cuda" if available else "cpu"`) — no changes
  needed either way.
- If you run this as a step in the Airflow DAG (M4), install these
  packages inside the Spark worker image / the Airflow task's Python
  environment, not just on your laptop.

## 3. What YOU need to edit before running it

Everything you need to change lives at the top of `stock_return_pipeline.py`.

### 3.1 `COLUMN_MAP` — match it to M3's actual Parquet schema

```python
COLUMN_MAP = {
    "ticker": "ticker",      # <- change to your actual column name
    "date": "date",          # <- change to your actual column name
    "close": "adj_close",    # <- change to your actual column name
    "volume": "volume",      # <- change to your actual column name
}
```

Ask the M3 owner for the final column names in the clean Parquet (fact
table / view) and update the values on the right-hand side only. The keys
(`ticker`, `date`, `close`, `volume`) must stay as they are — the rest of
the script uses those keys internally.

### 3.2 `CONFIG` — only touch this if you want to change the modeling setup

```python
CONFIG = {
    "window_size": 20,   # days of history used per prediction
    "horizon": 5,         # predicting 5 trading days ahead
    "train_ratio": 0.70,
    "val_ratio": 0.15,    # test_ratio is the remaining 0.15
    "min_rows": 800,      # tickers with fewer rows than this are skipped
    ...
}
```

You don't need to touch this unless you deliberately want to change the
prediction horizon, window size, or split ratios.

### 3.3 The HDFS input path — passed as a command-line argument, NOT hardcoded

You do **not** hardcode the HDFS path inside the script. Pass it with
`--input` when you run it (see section 4). Get this path from the M3
handover package — it's the "HDFS Parquet path" they deliver to you.

## 4. How to run it

```bash
spark-submit stock_return_pipeline.py \
    --input  hdfs://namenode:9000/clean/stock_history_parquet \
    --output /mnt/outputs/ml_run_1 \
    --tickers AAPL,MSFT,GOOG,AMZN,META,NVDA,JPM,JNJ,XOM,PG
```

- `--input` (required): HDFS (or local) path to the clean Parquet from M3.
- `--output` (required): local directory where CSVs + trained models are
  written. Create it anywhere convenient, e.g. `/mnt/outputs/ml_run_1`.
- `--tickers` (optional): comma-separated list. Omit it to auto-run on
  every ticker found in the Parquet.

Local test without a real HDFS/Parquet source yet: point `--input` at a
Parquet file/folder you export locally (e.g. converted from the Kaggle
CSVs) with the same 4 columns as `COLUMN_MAP`, just to confirm the script
runs end to end before M3 delivers the real data.

## 5. What it writes to `--output`

| File | Contents |
|---|---|
| `metrics_all_models.csv` | RMSE / MAE / DirAcc / etc. for every model, every ticker |
| `predictions_best_model.csv` | Predictions from the auto-selected best model — **this is what Streamlit reads** |
| `quantile_summary.csv` | Q10–Q90 coverage/width per ticker |
| `leakage_audit.csv` | Leakage/alignment audit results |
| `walk_forward.csv` | Walk-forward (expanding window) validation results |
| `models/<BestModel>/<TICKER>.joblib` or `.pt` | The trained model per ticker, ready to reload |
| `models/<BestModel>/<TICKER>_meta.joblib` | Scaler + target mean/std needed to un-scale that model's predictions |

## 6. Streamlit ML tab

The Streamlit tab reads a single file: `predictions_best_model.csv` (the
path is the only thing you configure). Nothing else about the tab depends
on HDFS, Parquet, or Spark — so it can be built and tested locally before
M3 finishes, by pointing it at a `predictions_best_model.csv` generated
from local test data.

```python
PREDICTIONS_CSV = "path/to/predictions_best_model.csv"  # <- only thing to edit
```

When the real M3 Parquet is ready: re-run `stock_return_pipeline.py` with
the real `--input`, overwrite `predictions_best_model.csv`, and the
Streamlit tab picks it up automatically — no code changes needed there.

*(Streamlit app code itself will be added here once the tab is built.)*
