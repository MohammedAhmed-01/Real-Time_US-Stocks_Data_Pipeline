# ============================================================
# prediction.py
# Local Stock Prediction using Saved Per-Ticker Models
# ============================================================

from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd
import chromadb
import joblib


# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

CHROMA_DB_PATH = BASE_DIR / "chroma_db"

MODELS_DIR = BASE_DIR / "models" / "Huber"


# ============================================================
# Configuration
# ============================================================

COLLECTION_NAME = "stocks_knowledge_base"

WINDOW_SIZE = 20
HORIZON = 5


# ============================================================
# Exact scaler used during training
# ============================================================

class TrainOnlyScaler:

    def __init__(self):
        self.mean_ = None
        self.std_ = None

    def fit(self, x):

        self.mean_ = x.mean(
            axis=0,
            keepdims=True
        )

        self.std_ = x.std(
            axis=0,
            keepdims=True
        )

        self.std_ = np.where(
            self.std_ < 1e-8,
            1.0,
            self.std_
        )

        return self

    def transform(self, x):

        return (
            x - self.mean_
        ) / self.std_

    def inverse(self, x_scaled, col=0):

        return (
            x_scaled * self.std_[0, col]
            + self.mean_[0, col]
        )


# ============================================================
# Fix old notebook pickle
# ============================================================

setattr(
    sys.modules["__main__"],
    "TrainOnlyScaler",
    TrainOnlyScaler
)


# ============================================================
# ChromaDB connection
# ============================================================

def get_collection():

    if not CHROMA_DB_PATH.exists():

        raise FileNotFoundError(
            f"ChromaDB not found at:\n"
            f"{CHROMA_DB_PATH}"
        )

    client = chromadb.PersistentClient(
        path=str(CHROMA_DB_PATH)
    )

    collection = client.get_collection(
        name=COLLECTION_NAME
    )

    return collection


# ============================================================
# Parse stock document
# ============================================================

def parse_stock_document(
    document: str,
    ticker: str
):
    """
    Convert a Chroma stock document back into:

    date
    open
    high
    low
    close
    volume
    """

    pattern = re.compile(
        rf"On (.*?), "
        rf"{re.escape(ticker)} stock opened at "
        rf"\$([0-9eE+\-.]+), "
        rf"reached a high of "
        rf"\$([0-9eE+\-.]+) "
        rf"and a low of "
        rf"\$([0-9eE+\-.]+), "
        rf"and closed at "
        rf"\$([0-9eE+\-.]+), "
        rf"with a trading volume of "
        rf"([0-9,]+) shares\.",
        re.IGNORECASE
    )

    match = pattern.search(document)

    if not match:
        return None

    date_str = match.group(1)

    open_price = float(
        match.group(2)
    )

    high_price = float(
        match.group(3)
    )

    low_price = float(
        match.group(4)
    )

    close_price = float(
        match.group(5)
    )

    volume = int(
        match.group(6).replace(",", "")
    )

    date = pd.to_datetime(
        date_str,
        errors="coerce"
    )

    if pd.isna(date):
        return None

    return {
        "Date": date,
        "Open": open_price,
        "High": high_price,
        "Low": low_price,
        "Close": close_price,
        "Volume": volume,
    }


# ============================================================
# Get stock history from Chroma
# ============================================================

def get_stock_history(
    ticker: str
) -> pd.DataFrame:

    ticker = ticker.upper().strip()

    collection = get_collection()

    result = collection.get(
        where={
            "ticker": ticker
        },
        include=[
            "documents",
            "metadatas"
        ]
    )

    documents = result.get(
        "documents",
        []
    )

    if not documents:

        raise ValueError(
            f"No stock documents found "
            f"in ChromaDB for ticker: {ticker}"
        )

    rows = []

    for document in documents:

        parsed = parse_stock_document(
            document,
            ticker
        )

        if parsed is not None:
            rows.append(parsed)

    if not rows:

        raise ValueError(
            f"Could not parse stock documents "
            f"for ticker: {ticker}"
        )

    df = pd.DataFrame(rows)

    # Sort chronologically
    df = df.sort_values(
        "Date"
    )

    # Remove duplicate dates
    df = df.drop_duplicates(
        subset=["Date"],
        keep="last"
    )

    # Reset index
    df = df.reset_index(
        drop=True
    )

    return df


# ============================================================
# Prepare latest 20 days
# ============================================================

def prepare_prediction_features(
    df: pd.DataFrame
):

    if len(df) < WINDOW_SIZE + 1:

        raise ValueError(
            f"Not enough stock history. "
            f"Need at least {WINDOW_SIZE + 1} "
            f"rows but found {len(df)}."
        )

    work = df.copy()

    # --------------------------------------------------------
    # EXACTLY like training
    # --------------------------------------------------------

    work["log_volume"] = np.log1p(
        work["Volume"]
        .fillna(
            work["Volume"].median()
        )
    )

    work["return_1d"] = (
        work["Close"].pct_change()
    )

    work = work.dropna(
        subset=[
            "return_1d",
            "log_volume"
        ]
    ).reset_index(
        drop=True
    )

    if len(work) < WINDOW_SIZE:

        raise ValueError(
            f"Not enough valid rows after "
            f"feature calculation. "
            f"Need {WINDOW_SIZE}, "
            f"found {len(work)}."
        )

    # Latest 20 rows
    latest = work.tail(
        WINDOW_SIZE
    ).copy()

    features_raw = latest[
        [
            "return_1d",
            "log_volume"
        ]
    ].to_numpy(
        dtype=np.float64
    )

    return latest, features_raw


# ============================================================
# Load model + metadata
# ============================================================

def load_ticker_model(
    ticker: str
):

    ticker = ticker.upper().strip()

    model_path = (
        MODELS_DIR
        / f"{ticker}.joblib"
    )

    meta_path = (
        MODELS_DIR
        / f"{ticker}_meta.joblib"
    )

    if not model_path.exists():

        raise FileNotFoundError(
            f"Model not found:\n"
            f"{model_path}"
        )

    if not meta_path.exists():

        raise FileNotFoundError(
            f"Metadata not found:\n"
            f"{meta_path}"
        )

    model = joblib.load(
        model_path
    )

    meta = joblib.load(
        meta_path
    )

    return model, meta


# ============================================================
# Main prediction function
# ============================================================

def predict_ticker(
    ticker: str
):

    ticker = ticker.upper().strip()

    if not re.fullmatch(
        r"[A-Z]{1,5}",
        ticker
    ):

        raise ValueError(
            "Invalid ticker symbol."
        )

    # --------------------------------------------------------
    # 1. Load saved model
    # --------------------------------------------------------

    model, meta = load_ticker_model(
        ticker
    )

    # --------------------------------------------------------
    # 2. Get historical data from Chroma
    # --------------------------------------------------------

    history = get_stock_history(
        ticker
    )

    # --------------------------------------------------------
    # 3. Build exact training features
    # --------------------------------------------------------

    latest, features_raw = (
        prepare_prediction_features(
            history
        )
    )

    # --------------------------------------------------------
    # 4. Load training scaler
    # --------------------------------------------------------

    feat_scaler = meta[
        "feat_scaler"
    ]

    y_mean = float(
        meta["y_mean"]
    )

    y_std = float(
        meta["y_std"]
    )

    # --------------------------------------------------------
    # 5. Apply EXACT feature scaling
    # --------------------------------------------------------

    features_scaled = (
        feat_scaler.transform(
            features_raw
        )
    )

    # --------------------------------------------------------
    # 6. Flatten exactly like classical models
    #
    # Training:
    #
    # (20, 2) -> (1, 40)
    # --------------------------------------------------------

    X = features_scaled.reshape(
        1,
        WINDOW_SIZE * 2
    )

    # --------------------------------------------------------
    # 7. Model prediction
    # --------------------------------------------------------

    predicted_scaled = model.predict(
        X
    )

    predicted_scaled = float(
        np.asarray(
            predicted_scaled
        ).reshape(-1)[0]
    )

    # --------------------------------------------------------
    # 8. Inverse target scaling
    #
    # Training:
    #
    # y_scaled = (return - y_mean) / y_std
    #
    # Therefore:
    #
    # return = prediction * y_std + y_mean
    # --------------------------------------------------------

    predicted_return = (
        predicted_scaled * y_std
        + y_mean
    )

    # --------------------------------------------------------
    # 9. Current/latest price
    # --------------------------------------------------------

    current_price = float(
        latest.iloc[-1]["Close"]
    )

    latest_date = (
        latest.iloc[-1]["Date"]
    )

    # --------------------------------------------------------
    # 10. Implied future price
    # --------------------------------------------------------

    predicted_price = (
        current_price
        * (1.0 + predicted_return)
    )

    # --------------------------------------------------------
    # 11. Direction
    # --------------------------------------------------------

    if predicted_return > 0:

        direction = "UP"

    elif predicted_return < 0:

        direction = "DOWN"

    else:

        direction = "FLAT"

    # --------------------------------------------------------
    # 12. Result
    # --------------------------------------------------------

    result = {

        "ticker": ticker,

        "model": "Huber",

        "latest_date": (
            latest_date.strftime(
                "%Y-%m-%d"
            )
        ),

        "current_price": (
            current_price
        ),

        "predicted_return": (
            float(predicted_return)
        ),

        "predicted_return_pct": (
            float(
                predicted_return * 100
            )
        ),

        "predicted_price": (
            float(predicted_price)
        ),

        "direction": direction,

        "horizon_days": HORIZON,

        "window_days": WINDOW_SIZE,

        "rows_used": WINDOW_SIZE,

        "history_rows_available": len(
            history
        ),
    }

    return result


# ============================================================
# Pretty standalone test
# ============================================================

if __name__ == "__main__":

    ticker = input(
        "Enter ticker: "
    ).strip().upper()

    try:

        result = predict_ticker(
            ticker
        )

        print(
            "\n"
            + "=" * 60
        )

        print(
            f"Ticker: "
            f"{result['ticker']}"
        )

        print(
            f"Model: "
            f"{result['model']}"
        )

        print(
            f"Latest date: "
            f"{result['latest_date']}"
        )

        print(
            f"Current price: "
            f"${result['current_price']:.2f}"
        )

        print(
            f"Predicted 5-day return: "
            f"{result['predicted_return_pct']:.2f}%"
        )

        print(
            f"Implied future price: "
            f"${result['predicted_price']:.2f}"
        )

        print(
            f"Direction: "
            f"{result['direction']}"
        )

        print(
            f"Rows used: "
            f"{result['rows_used']}"
        )

        print(
            "=" * 60
        )

    except Exception as e:

        print(
            "\nERROR:"
        )

        print(e)