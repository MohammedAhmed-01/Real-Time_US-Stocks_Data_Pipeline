"""
Stock 5-Day Return Prediction — full production pipeline (M5: ML part)
========================================================================

Same pipeline / same models / same libraries as `us-stock-prediction.ipynb`,
adapted to read from the team's Spark/Parquet pipeline (M2/M3 output)
instead of raw Kaggle CSVs.

Removed on purpose (per request):
    - all EDA / plotting cells
    - the live Yahoo Finance holdout test at the end of the notebook

Kept, unchanged in logic:
    - leakage-safe windowed dataset building (train-only scaling)
    - chronological train/val/test split (70/15/15)
    - the exact same model set: Naive, Ridge, Huber, RandomForest,
      HistGradientBoosting, XGBoost, LSTM (SmoothL1 loss, PyTorch)
    - per-ticker train + evaluate loop, auto best-model selection by mean RMSE
    - quantile regression (Q10/Q50/Q90) uncertainty bands
    - leakage / alignment audit
    - walk-forward (expanding window) validation with Huber

Run:
    spark-submit stock_return_pipeline.py \
        --input hdfs://namenode:9000/clean/stock_history_parquet \
        --output /mnt/outputs/ml_run_1 \
        --tickers AAPL,MSFT,GOOG,AMZN,META,NVDA,JPM,JNJ,XOM,PG

Meant to run as one task in the Airflow DAG (M4), right after the M3
Spark SQL -> PostgreSQL load step has produced the clean Parquet.
"""

import argparse
import os
import warnings

import numpy as np
import pandas as pd

from sklearn.linear_model import Ridge, HuberRegressor, QuantileRegressor
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

try:
    from xgboost import XGBRegressor
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "xgboost"])
    from xgboost import XGBRegressor

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

import joblib

warnings.filterwarnings("ignore")

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

CONFIG = {
    "window_size": 20,
    "horizon": 5,
    "train_ratio": 0.70,
    "val_ratio": 0.15,
    "min_rows": 800,
    "model": {"hidden_size": 16, "num_layers": 1, "dropout": 0.1},
    "training": {
        "batch_size": 64,
        "epochs": 30,
        "lr": 1e-3,
        "weight_decay": 1e-5,
        "seed": 42,
    },
}

# column names in the clean Parquet from M2/M3 -- adjust once you know the
# real schema
COLUMN_MAP = {
    "ticker": "ticker",
    "date": "date",
    "close": "adj_close",
    "volume": "volume",
}


# ---------------------------------------------------------------------------
# 1) Data loading — Spark/Parquet source instead of Kaggle CSV
# ---------------------------------------------------------------------------

def get_spark(app_name="stock-5d-return-pipeline"):
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_all_tickers(spark, parquet_path, tickers=None, config=CONFIG, col_map=COLUMN_MAP):
    """Read the clean Parquet, split by ticker, and build the same
    per-ticker feature columns `load_ticker()` built from the raw Kaggle
    CSV: date, adj_close, volume, log_volume, return_1d."""
    from pyspark.sql import functions as F

    sdf = spark.read.parquet(parquet_path)
    sdf = sdf.select(
        F.col(col_map["ticker"]).alias("ticker"),
        F.to_date(F.col(col_map["date"])).alias("date"),
        F.col(col_map["close"]).cast("double").alias("adj_close"),
        F.col(col_map["volume"]).cast("double").alias("volume"),
    ).dropna(subset=["ticker", "date", "adj_close"])

    if tickers:
        sdf = sdf.filter(F.col("ticker").isin(tickers))
    sdf = sdf.dropDuplicates(["ticker", "date"])

    all_tickers = tickers or [r["ticker"] for r in sdf.select("ticker").distinct().collect()]

    stock_data = {}
    for t in all_tickers:
        pdf = sdf.filter(F.col("ticker") == t).orderBy("date").toPandas()
        if pdf.empty:
            print(f"  [skip] {t}: no rows in Parquet")
            continue

        pdf["date"] = pd.to_datetime(pdf["date"])
        pdf = pdf.sort_values("date").drop_duplicates("date").reset_index(drop=True)
        pdf["log_volume"] = np.log1p(pdf["volume"].fillna(pdf["volume"].median()))
        pdf["return_1d"] = pdf["adj_close"].pct_change()
        pdf = pdf.dropna(subset=["return_1d"]).reset_index(drop=True)

        if len(pdf) < config["min_rows"]:
            print(f"  [skip] {t}: only {len(pdf)} rows (< min_rows={config['min_rows']})")
            continue

        pdf.attrs["ticker"] = t
        stock_data[t] = pdf
        print(f"  loaded {t}: {len(pdf)} rows ({pdf['date'].min().date()} -> {pdf['date'].max().date()})")

    return stock_data


# ---------------------------------------------------------------------------
# 2) Leakage-safe dataset building (identical to the notebook)
# ---------------------------------------------------------------------------

class TrainOnlyScaler:
    def __init__(self):
        self.mean_ = None
        self.std_ = None

    def fit(self, x):
        self.mean_ = x.mean(axis=0, keepdims=True)
        self.std_ = x.std(axis=0, keepdims=True)
        self.std_ = np.where(self.std_ < 1e-8, 1.0, self.std_)
        return self

    def transform(self, x):
        return (x - self.mean_) / self.std_

    def inverse(self, x_scaled, col=0):
        return x_scaled * self.std_[0, col] + self.mean_[0, col]


def chronological_split_indices(n, train_ratio, val_ratio):
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)
    return slice(0, n_train), slice(n_train, n_train + n_val), slice(n_train + n_val, n)


def prepare_ticker_datasets(df, config=CONFIG):
    """Build leakage-safe windows that predict the H-day forward return."""
    window = int(config["window_size"])
    horizon = int(config["horizon"])
    work = df.dropna(subset=["return_1d"]).sort_values("date").reset_index(drop=True)
    work.attrs["ticker"] = df.attrs.get("ticker", "UNK")

    price = work["adj_close"].to_numpy(np.float64)
    dates = work["date"].to_numpy()
    feats_raw = work[["return_1d", "log_volume"]].to_numpy(np.float64)

    fwd = np.full(len(work), np.nan, dtype=np.float64)
    fwd[: len(work) - horizon] = (price[horizon:] - price[:-horizon]) / np.maximum(price[:-horizon], 1e-12)

    train_idx, val_idx, test_idx = chronological_split_indices(
        len(work), config["train_ratio"], config["val_ratio"]
    )

    feat_scaler = TrainOnlyScaler().fit(feats_raw[train_idx])
    feats = feat_scaler.transform(feats_raw)

    train_label_idx = np.arange(train_idx.start, train_idx.stop)
    train_label_idx = train_label_idx[train_label_idx < (len(work) - horizon)]
    y_mean = float(np.nanmean(fwd[train_label_idx]))
    y_std = float(np.nanstd(fwd[train_label_idx]))
    if not np.isfinite(y_std) or y_std < 1e-8:
        y_std = 1.0
    y_scaled = (fwd - y_mean) / y_std

    xs, ys, t_list = [], [], []
    max_i = len(work) - window - horizon
    for i in range(max_i + 1):
        t = i + window - 1
        xs.append(feats[i:i + window])
        ys.append(y_scaled[t])
        t_list.append(t)

    X_all = np.asarray(xs, np.float32)
    y_all = np.asarray(ys, np.float32)
    t_idx = np.asarray(t_list, np.int64)

    y_return = fwd[t_idx]
    price_t = price[t_idx]
    price_future = price[t_idx + horizon]
    y_dates = dates[t_idx]
    feat_end_dates = dates[t_idx]
    target_end_dates = dates[t_idx + horizon]

    m_train = t_idx < train_idx.stop
    m_val = (t_idx >= train_idx.stop) & (t_idx < val_idx.stop)
    m_test = t_idx >= val_idx.stop

    bundles = {}
    for name, mask in [("train", m_train), ("val", m_val), ("test", m_test)]:
        X = X_all[mask]
        n = len(X)
        bundles[name] = {
            "X_seq": X,
            "X_flat": X.reshape(n, -1) if n else np.zeros((0, window * feats.shape[1]), np.float32),
            "y": y_all[mask],
            "y_return": y_return[mask],
            "price_t": price_t[mask],
            "price_future": price_future[mask],
            "dates": y_dates[mask],
            "feat_end_dates": feat_end_dates[mask],
            "target_end_dates": target_end_dates[mask],
            "abs_idx": t_idx[mask],
            "naive_return": np.zeros(n, dtype=np.float64),
        }

    meta = {
        "feat_scaler": feat_scaler,
        "y_mean": y_mean,
        "y_std": y_std,
        "n_features": feats.shape[1],
        "ticker": work.attrs.get("ticker", df.attrs.get("ticker", "UNK")),
        "n_rows": len(work),
        "train_end": int(train_idx.stop),
        "val_end": int(val_idx.stop),
        "window": window,
        "horizon": horizon,
    }
    return bundles, meta


def inverse_return(y_scaled, meta):
    return np.asarray(y_scaled, dtype=np.float64) * meta["y_std"] + meta["y_mean"]


def return_to_price(price_t, ret):
    return np.asarray(price_t, dtype=np.float64) * (1.0 + np.asarray(ret, dtype=np.float64))


# ---------------------------------------------------------------------------
# 3) Models + metrics (identical to the notebook)
# ---------------------------------------------------------------------------

def rmse(y_true, y_pred):
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def directional_accuracy_returns(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=np.float64)
    y_pred = np.asarray(y_pred, dtype=np.float64)
    true_dir = np.sign(y_true)
    pred_dir = np.sign(y_pred)
    mask = true_dir != 0
    if mask.sum() == 0:
        return np.nan
    return float((true_dir[mask] == pred_dir[mask]).mean())


def eval_returns(y_true, y_pred, model_name):
    y_true = np.asarray(y_true, dtype=np.float64)
    y_pred = np.asarray(y_pred, dtype=np.float64)
    act_std = float(np.std(y_true))
    pred_std = float(np.std(y_pred))
    return {
        "model": model_name,
        "rmse": rmse(y_true, y_pred),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "dir_acc": directional_accuracy_returns(y_true, y_pred),
        "pred_std": pred_std,
        "pred_std_ratio": pred_std / max(act_std, 1e-12),
    }


class SeqDataset(Dataset):
    def __init__(self, x, y):
        self.x = torch.tensor(x, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32)

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        return self.x[idx], self.y[idx]


class LSTMRegressor(nn.Module):
    def __init__(self, n_features, hidden_size=64, num_layers=2, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(nn.Dropout(dropout), nn.Linear(hidden_size, 1))

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.head(out[:, -1, :]).squeeze(-1)


def set_seed(seed=42):
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def train_lstm_smoothl1(bundles, meta, config=CONFIG, verbose=False):
    """Same LSTM as the notebook, trained with SmoothL1 (Huber-like) loss."""
    set_seed(config["training"]["seed"])
    train_loader = DataLoader(
        SeqDataset(bundles["train"]["X_seq"], bundles["train"]["y"]),
        batch_size=config["training"]["batch_size"], shuffle=True,
    )
    val_loader = DataLoader(
        SeqDataset(bundles["val"]["X_seq"], bundles["val"]["y"]),
        batch_size=config["training"]["batch_size"], shuffle=False,
    )
    test_loader = DataLoader(
        SeqDataset(bundles["test"]["X_seq"], bundles["test"]["y"]),
        batch_size=config["training"]["batch_size"], shuffle=False,
    )

    model = LSTMRegressor(
        n_features=meta["n_features"],
        hidden_size=config["model"]["hidden_size"],
        num_layers=config["model"]["num_layers"],
        dropout=config["model"]["dropout"],
    ).to(DEVICE)

    criterion = nn.SmoothL1Loss(beta=1.0)
    optimizer = torch.optim.Adam(
        model.parameters(), lr=config["training"]["lr"], weight_decay=config["training"]["weight_decay"]
    )

    best_state, best_val, bad, patience = None, float("inf"), 0, 6
    for epoch in range(1, config["training"]["epochs"] + 1):
        model.train()
        for xb, yb in train_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

        model.eval()
        vals = []
        with torch.no_grad():
            for xb, yb in val_loader:
                xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                vals.append(criterion(model(xb), yb).item())
        va = float(np.mean(vals)) if vals else np.inf
        if va < best_val - 1e-6:
            best_val = va
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            bad = 0
        else:
            bad += 1
        if verbose and epoch % 10 == 0:
            print(f"  LSTM-SmoothL1 epoch {epoch}: val={va:.5f}")
        if bad >= patience:
            break

    model.load_state_dict(best_state)
    model.eval()
    preds = []
    with torch.no_grad():
        for xb, _ in test_loader:
            preds.append(model(xb.to(DEVICE)).cpu().numpy())
    pred_scaled = np.concatenate(preds) if preds else np.array([])
    return inverse_return(pred_scaled, meta), model


def get_models(seed=42):
    return {
        "Ridge": Ridge(alpha=100.0),
        "Huber": HuberRegressor(epsilon=1.35, alpha=0.001),
        "RandomForest": RandomForestRegressor(
            n_estimators=100, max_depth=4, min_samples_leaf=20, random_state=seed
        ),
        "HistGradientBoosting": HistGradientBoostingRegressor(
            max_depth=3, learning_rate=0.05, min_samples_leaf=30, random_state=seed
        ),
        "XGBoost": XGBRegressor(
            n_estimators=100, max_depth=3, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, random_state=seed,
        ),
    }


def run_all_models_for_ticker(df, verbose=True, config=CONFIG):
    bundles, meta = prepare_ticker_datasets(df, config)
    ticker = meta["ticker"]
    y_true = bundles["test"]["y_return"]

    rows, preds_store, models_store = [], {}, {}

    naive = bundles["test"]["naive_return"].copy()
    rows.append(eval_returns(y_true, naive, "Naive"))
    preds_store["Naive"] = naive

    Xtr, ytr = bundles["train"]["X_flat"], bundles["train"]["y"]
    Xte = bundles["test"]["X_flat"]

    for name, model in get_models(config["training"]["seed"]).items():
        model.fit(Xtr, ytr)
        pred = inverse_return(model.predict(Xte), meta)
        rows.append(eval_returns(y_true, pred, name))
        preds_store[name] = pred
        models_store[name] = model
        if verbose:
            r = rows[-1]
            print(
                f"{ticker} | {name:14s} RMSE={r['rmse']:.5f}  "
                f"DirAcc={r['dir_acc']:.3f}  std_ratio={r['pred_std_ratio']:.3f}"
            )

    lstm_pred, lstm_model = train_lstm_smoothl1(bundles, meta, config, verbose=False)
    rows.append(eval_returns(y_true, lstm_pred, "LSTM(SmoothL1)"))
    preds_store["LSTM(SmoothL1)"] = lstm_pred
    models_store["LSTM(SmoothL1)"] = lstm_model
    if verbose:
        r = rows[-1]
        print(
            f"{ticker} | {'LSTM(SmoothL1)':14s} RMSE={r['rmse']:.5f}  "
            f"DirAcc={r['dir_acc']:.3f}  std_ratio={r['pred_std_ratio']:.3f}"
        )

    out = pd.DataFrame(rows)
    out.insert(0, "ticker", ticker)
    naive_rmse = float(out.loc[out["model"] == "Naive", "rmse"].iloc[0])
    out["rmse_vs_naive_pct"] = 100.0 * (naive_rmse - out["rmse"]) / max(naive_rmse, 1e-12)

    return {
        "metrics": out,
        "preds": preds_store,
        "models": models_store,
        "y_true": y_true,
        "price_t": bundles["test"]["price_t"],
        "dates": bundles["test"]["dates"],
        "meta": meta,
        "bundles": bundles,
    }


# ---------------------------------------------------------------------------
# 4) Train + evaluate all tickers, auto-pick best model (identical logic)
# ---------------------------------------------------------------------------

def train_and_evaluate_all(stock_data, tickers, config=CONFIG):
    all_metrics = []
    details = {}
    for i, ticker in enumerate(tickers, 1):
        print("\n" + "=" * 72)
        print(f"[{i}/{len(tickers)}] {ticker}")
        print("=" * 72)
        result = run_all_models_for_ticker(stock_data[ticker], verbose=True, config=config)
        details[ticker] = result
        all_metrics.append(result["metrics"])
    metrics_df = pd.concat(all_metrics, ignore_index=True)
    return metrics_df, details


def pick_best_model(metrics_df):
    avg = (
        metrics_df[metrics_df["model"] != "Naive"]
        .groupby("model", as_index=False)[["rmse", "mae", "dir_acc", "pred_std_ratio", "rmse_vs_naive_pct"]]
        .mean()
        .sort_values("rmse")
    )
    best_model_name = str(avg.iloc[0]["model"])
    return avg, best_model_name


# ---------------------------------------------------------------------------
# 5) Quantile regression — uncertainty bands (identical to the notebook)
# ---------------------------------------------------------------------------

def fit_quantiles_for_ticker(df, quantiles=(0.1, 0.5, 0.9), config=CONFIG):
    bundles, meta = prepare_ticker_datasets(df, config)
    Xtr, ytr = bundles["train"]["X_flat"], bundles["train"]["y"]
    Xte = bundles["test"]["X_flat"]
    y_true = bundles["test"]["y_return"]

    preds = {}
    for q in quantiles:
        qr = QuantileRegressor(quantile=q, alpha=1e-4, solver="highs")
        qr.fit(Xtr, ytr)
        preds[q] = inverse_return(qr.predict(Xte), meta)

    q10, q50, q90 = preds[0.1], preds[0.5], preds[0.9]
    cover = float(np.mean((y_true >= q10) & (y_true <= q90)))
    width = float(np.mean(q90 - q10))
    return {
        "ticker": meta["ticker"],
        "coverage_80ish": cover,
        "mean_width": width,
        "q50_dir_acc": directional_accuracy_returns(y_true, q50),
        "q50_rmse": rmse(y_true, q50),
    }


def run_quantile_regression(stock_data, tickers, config=CONFIG):
    print("\nFitting quantile models (Q10/Q50/Q90) per ticker...")
    rows = []
    for t in tickers:
        r = fit_quantiles_for_ticker(stock_data[t], config=config)
        rows.append(r)
        print(
            f"  {t}: coverage={r['coverage_80ish']:.3f}  width={r['mean_width']:.4f}  "
            f"Q50 DirAcc={r['q50_dir_acc']:.3f}"
        )
    q_df = pd.DataFrame(rows)
    print(
        f"Mean coverage={q_df['coverage_80ish'].mean():.3f} | "
        f"Mean width={q_df['mean_width'].mean():.4f} | "
        f"Mean Q50 DirAcc={q_df['q50_dir_acc'].mean():.3f}"
    )
    return q_df


# ---------------------------------------------------------------------------
# 6) Leakage & alignment audit (identical to the notebook)
# ---------------------------------------------------------------------------

def audit_ticker_leakage(bundles, meta, tol=1e-9):
    checks = {}
    for split in ("train", "val", "test"):
        b = bundles[split]
        if len(b["dates"]) == 0:
            checks[f"{split}_order"] = True
            continue
        checks[f"{split}_feat_before_target_end"] = bool(
            np.all(np.asarray(b["feat_end_dates"]) < np.asarray(b["target_end_dates"]))
        )
        if len(b["y_return"]):
            recon = (b["price_future"] - b["price_t"]) / np.maximum(b["price_t"], 1e-12)
            checks[f"{split}_return_identity"] = bool(np.allclose(recon, b["y_return"], atol=tol, rtol=0))
            checks[f"{split}_no_nan"] = bool(np.isfinite(b["X_seq"]).all() and np.isfinite(b["y_return"]).all())
        else:
            checks[f"{split}_return_identity"] = True
            checks[f"{split}_no_nan"] = True

    tr, va, te = map(lambda s: set(bundles[s]["abs_idx"].tolist()), ("train", "val", "test"))
    checks["disjoint"] = len(tr & va) == 0 and len(tr & te) == 0 and len(va & te) == 0
    checks["horizon_ok"] = meta["horizon"] == CONFIG["horizon"]
    checks["ticker"] = meta["ticker"]
    checks["all_passed"] = all(v for k, v in checks.items() if k not in {"ticker", "all_passed"} and isinstance(v, bool))
    return checks


def run_leakage_audit(details, tickers):
    print("\n=== Leakage / alignment audit ===")
    rows = []
    for t in tickers:
        rows.append(audit_ticker_leakage(details[t]["bundles"], details[t]["meta"]))
    audit_df = pd.DataFrame(rows)
    n_pass = int(audit_df["all_passed"].sum())
    print(f"Passed all checks: {n_pass}/{len(audit_df)}")
    if n_pass == len(audit_df):
        print("OK - no leakage flags detected.")
    else:
        print("WARNING - inspect the failed columns above.")
    return audit_df


# ---------------------------------------------------------------------------
# 7) Walk-forward (expanding window) validation (identical to the notebook)
# ---------------------------------------------------------------------------

def walk_forward_huber(df, n_splits=5, min_train_frac=0.5, config=CONFIG):
    window = int(config["window_size"])
    horizon = int(config["horizon"])
    work = df.dropna(subset=["return_1d"]).sort_values("date").reset_index(drop=True)
    price = work["adj_close"].to_numpy(np.float64)
    dates = work["date"].to_numpy()
    feats_raw = work[["return_1d", "log_volume"]].to_numpy(np.float64)

    fwd = np.full(len(work), np.nan, dtype=np.float64)
    fwd[: len(work) - horizon] = (price[horizon:] - price[:-horizon]) / np.maximum(price[:-horizon], 1e-12)

    xs, y_raw, t_list = [], [], []
    max_i = len(work) - window - horizon
    for i in range(max_i + 1):
        t = i + window - 1
        xs.append(feats_raw[i:i + window].reshape(-1))
        y_raw.append(fwd[t])
        t_list.append(t)
    y_raw = np.asarray(y_raw, np.float64)
    t_idx = np.asarray(t_list, np.int64)
    n_samples = len(t_idx)

    min_train = max(window * 3, int(n_samples * min_train_frac))
    fold_size = (n_samples - min_train) // n_splits
    if fold_size <= 0:
        return pd.DataFrame([])

    rows = []
    for fold in range(n_splits):
        test_start = min_train + fold * fold_size
        test_end = min_train + (fold + 1) * fold_size if fold < n_splits - 1 else n_samples
        raw_train_end = int(t_idx[test_start])

        mu = feats_raw[:raw_train_end].mean(axis=0, keepdims=True)
        sd = feats_raw[:raw_train_end].std(axis=0, keepdims=True)
        sd = np.where(sd < 1e-8, 1.0, sd)
        X_fold = np.asarray([
            ((feats_raw[i:i + window] - mu) / sd).reshape(-1) for i in range(max_i + 1)
        ], np.float32)

        y_mean = float(np.nanmean(fwd[:raw_train_end]))
        y_std = float(np.nanstd(fwd[:raw_train_end])) or 1.0
        y_scaled = (y_raw - y_mean) / y_std

        model = HuberRegressor(epsilon=1.35, max_iter=500)
        model.fit(X_fold[:test_start], y_scaled[:test_start])
        pred = model.predict(X_fold[test_start:test_end]) * y_std + y_mean
        yte = y_raw[test_start:test_end]
        naive = np.zeros_like(yte)

        rows.append({
            "fold": fold + 1,
            "train_samples": int(test_start),
            "test_samples": int(test_end - test_start),
            "test_start": pd.Timestamp(dates[t_idx[test_start]]),
            "test_end": pd.Timestamp(dates[t_idx[test_end - 1]]),
            "model_rmse": rmse(yte, pred),
            "naive_rmse": rmse(yte, naive),
            "dir_acc": directional_accuracy_returns(yte, pred),
            "pred_std_ratio": float(np.std(pred) / max(np.std(yte), 1e-12)),
            "rmse_vs_naive_pct": 100.0 * (rmse(yte, naive) - rmse(yte, pred)) / max(rmse(yte, naive), 1e-12),
        })
    return pd.DataFrame(rows)


def run_walk_forward(stock_data, tickers, n_splits=5, config=CONFIG):
    print("\n=== Walk-forward (expanding, Huber) — all tickers ===")
    frames = []
    for t in tickers:
        wf = walk_forward_huber(stock_data[t], n_splits=n_splits, config=config)
        if wf.empty:
            print(f"  [skip] {t}: not enough rows for {n_splits} walk-forward folds")
            continue
        wf.insert(0, "ticker", t)
        frames.append(wf)
        print(
            f"  {t}: mean RMSE-vs-naive={wf['rmse_vs_naive_pct'].mean():.2f}%  "
            f"mean DirAcc={wf['dir_acc'].mean():.3f}"
        )
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame([])


# ---------------------------------------------------------------------------
# 8) Save outputs: metrics, predictions CSV, quantile/audit/walk-forward
#    reports, and the trained best-model-type per ticker
# ---------------------------------------------------------------------------

def save_best_model_predictions(details, best_model_name, tickers, output_dir):
    frames = []
    for t in tickers:
        d = details[t]
        pred = d["preds"][best_model_name]
        frames.append(pd.DataFrame({
            "ticker": t,
            "date": d["dates"],
            "y_true_return": d["y_true"],
            "y_pred_return": pred,
            "price_t": d["price_t"],
            "implied_price_t+5": return_to_price(d["price_t"], pred),
            "model": best_model_name,
        }))
    preds_df = pd.concat(frames, ignore_index=True)
    path = f"{output_dir}/predictions_best_model.csv"
    preds_df.to_csv(path, index=False)
    print(f"Saved predictions   -> {path}")
    return preds_df


def save_best_models(details, best_model_name, tickers, output_dir):
    models_dir = f"{output_dir}/models/{best_model_name}"
    os.makedirs(models_dir, exist_ok=True)
    for t in tickers:
        model = details[t]["models"][best_model_name]
        if best_model_name == "LSTM(SmoothL1)":
            torch.save(model.state_dict(), f"{models_dir}/{t}.pt")
        else:
            joblib.dump(model, f"{models_dir}/{t}.joblib")
        # also persist the scaler + y_mean/y_std needed to use the model later
        joblib.dump(details[t]["meta"], f"{models_dir}/{t}_meta.joblib")
    print(f"Saved best models   -> {models_dir}/<TICKER>.*")


# ---------------------------------------------------------------------------
# 9) CLI entrypoint
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Stock 5-day return prediction — full pipeline")
    p.add_argument("--input", required=True, help="Path to clean Parquet (HDFS or local)")
    p.add_argument("--output", required=True, help="Output directory for CSVs + saved models")
    p.add_argument(
        "--tickers", default=None,
        help="Comma-separated ticker list (default: all tickers found in the Parquet)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    tickers_arg = args.tickers.split(",") if args.tickers else None
    os.makedirs(args.output, exist_ok=True)

    spark = get_spark()
    try:
        print("Loading tickers from Parquet...")
        stock_data = load_all_tickers(spark, args.input, tickers_arg)
    finally:
        spark.stop()

    tickers = list(stock_data.keys())
    if not tickers:
        raise SystemExit("No usable tickers loaded — check --input path and COLUMN_MAP.")

    print(f"\nDevice: {DEVICE}")
    print(f"Target: next-{CONFIG['horizon']}-day return | window={CONFIG['window_size']} | {len(tickers)} tickers")

    # --- train + evaluate every model, on every ticker ---
    metrics_df, details = train_and_evaluate_all(stock_data, tickers)
    metrics_path = f"{args.output}/metrics_all_models.csv"
    metrics_df.to_csv(metrics_path, index=False)
    print(f"\nSaved metrics       -> {metrics_path}")

    avg, best_model_name = pick_best_model(metrics_df)
    print("\n=== Average TEST metrics across tickers ===")
    print(avg.round(4).to_string(index=False))
    print(f"\n>>> BEST MODEL: {best_model_name} (lowest mean RMSE)")

    save_best_model_predictions(details, best_model_name, tickers, args.output)
    save_best_models(details, best_model_name, tickers, args.output)

    # --- quantile regression (Q10/Q50/Q90) ---
    q_df = run_quantile_regression(stock_data, tickers)
    q_path = f"{args.output}/quantile_summary.csv"
    q_df.to_csv(q_path, index=False)
    print(f"Saved quantiles     -> {q_path}")

    # --- leakage / alignment audit ---
    audit_df = run_leakage_audit(details, tickers)
    audit_path = f"{args.output}/leakage_audit.csv"
    audit_df.to_csv(audit_path, index=False)
    print(f"Saved audit         -> {audit_path}")

    # --- walk-forward validation ---
    wf_df = run_walk_forward(stock_data, tickers)
    wf_path = f"{args.output}/walk_forward.csv"
    wf_df.to_csv(wf_path, index=False)
    print(f"Saved walk-forward  -> {wf_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
