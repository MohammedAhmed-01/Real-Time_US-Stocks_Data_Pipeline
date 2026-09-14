import pandas as pd


# ============================================================
# Column mapping
# ============================================================

STOCK_COLUMN_MAP = {
    "date": "Date",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}


# ============================================================
# Find column
# ============================================================

def _find_column(df: pd.DataFrame, wanted: str) -> str:

    if wanted in df.columns:
        return wanted

    lowered = {
        str(c).lower(): c
        for c in df.columns
    }

    if wanted.lower() in lowered:
        return lowered[wanted.lower()]

    raise KeyError(
        f"Column '{wanted}' not found. "
        f"Available columns: {list(df.columns)}"
    )


# ============================================================
# Stock row → text
# ============================================================

def stock_row_to_text(
    row: pd.Series,
    ticker: str,
    column_map: dict = STOCK_COLUMN_MAP
) -> str:

    c = column_map

    return (
        f"On {row[c['date']]}, "
        f"{ticker} stock opened at "
        f"${float(row[c['open']]):.2f}, "
        f"reached a high of "
        f"${float(row[c['high']]):.2f} "
        f"and a low of "
        f"${float(row[c['low']]):.2f}, "
        f"and closed at "
        f"${float(row[c['close']]):.2f}, "
        f"with a trading volume of "
        f"{int(row[c['volume']]):,} shares."
    )


# ============================================================
# Generic row → text
# ============================================================

def generic_row_to_text(
    row: pd.Series,
    label: str
) -> str:

    parts = []

    for col, val in row.items():

        if pd.isna(val):
            continue

        clean_col = (
            str(col)
            .replace("_", " ")
            .strip()
        )

        parts.append(
            f"{clean_col}: {val}"
        )

    return (
        f"{label} record — "
        + ", ".join(parts)
        + "."
    )


# ============================================================
# Stock CSV → documents
# ============================================================

def stock_csv_to_documents(
    csv_path: str,
    ticker: str,
    years_back: int | None = None
) -> list[str]:

    print(f"Reading: {csv_path}")

    df = pd.read_csv(csv_path)

    print(
        f"{ticker}: "
        f"{len(df):,} rows loaded"
    )

    date_col = _find_column(
        df,
        STOCK_COLUMN_MAP["date"]
    )

    df[date_col] = pd.to_datetime(
        df[date_col],
        errors="coerce"
    )

    df = df.dropna(
        subset=[date_col]
    )

    if years_back is not None and not df.empty:

        cutoff = (
            df[date_col].max()
            - pd.DateOffset(years=years_back)
        )

        df = df[
            df[date_col] >= cutoff
        ]

        print(
            f"{ticker}: "
            f"{len(df):,} rows after "
            f"{years_back}-year filter"
        )

    documents = []

    for _, row in df.iterrows():

        try:

            text = stock_row_to_text(
                row,
                ticker
            )

            documents.append(text)

        except Exception as e:

            print(
                f"WARNING: Failed to convert "
                f"{ticker} row: {e}"
            )

    return documents


# ============================================================
# Generic CSV → documents
# ============================================================

def generic_csv_to_documents(
    csv_path: str,
    label: str
) -> list[str]:

    print(f"Reading: {csv_path}")

    df = pd.read_csv(csv_path)

    print(
        f"{label}: "
        f"{len(df):,} rows loaded"
    )

    documents = []

    for _, row in df.iterrows():

        text = generic_row_to_text(
            row,
            label
        )

        documents.append(text)

    return documents
