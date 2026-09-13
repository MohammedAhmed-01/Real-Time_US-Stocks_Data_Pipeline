"""
text_converter.py
==================
Converts tabular stock-market data (CSV rows) into natural-language text
suitable for embedding. Structured rows like:

    ticker=AAPL, date=2024-01-01, close=226.85

are converted into descriptive sentences, since embedding models are
trained on natural language and produce much higher-quality vector
representations from prose than from raw tabular values.

Two converters are provided:
    - stock_row_to_text()        -> converts one OHLCV price row
    - prediction_row_to_text()   -> converts one ML model prediction row

Configuration
-------------
If your actual column names differ from the defaults below, update only
the values (right-hand side) in STOCK_COLUMN_MAP / PREDICTION_COLUMN_MAP.
Do not rename the dictionary keys — they are used internally.
"""

from __future__ import annotations

import pandas as pd


# ============================================================
# Column mapping — edit the VALUES only if your CSV headers differ
# ============================================================
STOCK_COLUMN_MAP = {
    "ticker": "ticker",
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}

PREDICTION_COLUMN_MAP = {
    "ticker": "ticker",
    "date": "date",
    "predicted_return": "predicted_return",
    "model_name": "model_name",
    "confidence": "confidence",
}


def stock_row_to_text(row: pd.Series) -> str:
    """Convert a single OHLCV price row into a natural-language sentence."""
    c = STOCK_COLUMN_MAP
    try:
        return (
            f"On {row[c['date']]}, {row[c['ticker']]} stock opened at "
            f"${row[c['open']]:.2f}, reached a high of ${row[c['high']]:.2f} "
            f"and a low of ${row[c['low']]:.2f}, and closed at "
            f"${row[c['close']]:.2f}, with a trading volume of "
            f"{int(row[c['volume']]):,} shares."
        )
    except KeyError as e:
        raise KeyError(
            f"Column {e} was not found in the file. "
            f"Check STOCK_COLUMN_MAP at the top of this file."
        )


def prediction_row_to_text(row: pd.Series) -> str:
    """Convert a single ML prediction row (predictions_best_model.csv) into text."""
    c = PREDICTION_COLUMN_MAP
    try:
        direction = "upward" if row[c["predicted_return"]] > 0 else "downward"
        return (
            f"The {row[c['model_name']]} model predicts a {direction} trend for "
            f"{row[c['ticker']]} on {row[c['date']]}, with an expected return of "
            f"{row[c['predicted_return']]:.4f} and a confidence level of "
            f"{row[c['confidence']]:.2f}."
        )
    except KeyError as e:
        raise KeyError(
            f"Column {e} was not found in the file. "
            f"Check PREDICTION_COLUMN_MAP at the top of this file."
        )


def csv_to_documents(csv_path: str, row_type: str = "stock") -> list[str]:
    """
    Read a full CSV file and return a list of natural-language sentences,
    one per row.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file.
    row_type : str
        Either "stock" (OHLCV price data) or "prediction" (ML model output).
    """
    df = pd.read_csv(csv_path)
    converter = stock_row_to_text if row_type == "stock" else prediction_row_to_text
    return [converter(row) for _, row in df.iterrows()]


if __name__ == "__main__":
    # Quick smoke test. Run this from the project root so the relative
    # path resolves correctly:
    #   python src/text_converter.py
    docs = csv_to_documents("data/sample_stock_data.csv", row_type="stock")
    print(f"Converted {len(docs)} rows to text. First 3 examples:\n")
    for d in docs[:3]:
        print("-", d)
