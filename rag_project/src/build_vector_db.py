"""
build_vector_db.py

Build ChromaDB from:

1. StockHistory CSV files
2. metrics_all_models.csv
3. walk_forward.csv

Stock documents include metadata:
    ticker
    type

This allows ticker-aware retrieval.
"""

from __future__ import annotations

import glob
import os

import chromadb
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

from text_converter import (
    stock_csv_to_documents,
    generic_csv_to_documents,
)


# ============================================================
# PATHS
# ============================================================

STOCKHISTORY_DIR = (
    "/kaggle/input/datasets/"
    "footballjoe789/us-stock-dataset/"
    "Data/StockHistory"
)

OUTPUTS_DIR = (
    "/kaggle/input/datasets/"
    "mohamedyounis15/data-used/"
    "Outputs"
)

METRICS_CSV_PATH = os.path.join(
    OUTPUTS_DIR,
    "metrics_all_models.csv"
)

WALKFORWARD_CSV_PATH = os.path.join(
    OUTPUTS_DIR,
    "walk_forward.csv"
)

CHROMA_DB_PATH = "/kaggle/working/chroma_db"

COLLECTION_NAME = "stocks_knowledge_base"

EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"


# ============================================================
# SETTINGS
# ============================================================

CHUNK_SIZE = 5

YEARS_BACK = 2

ENCODE_BATCH_SIZE = 256

# None = all tickers
LIMIT_TICKERS = None


# ============================================================
# CHUNKING
# ============================================================

def make_chunks(
    documents: list[str],
    chunk_size: int = CHUNK_SIZE
) -> list[str]:

    chunks = []

    for i in range(
        0,
        len(documents),
        chunk_size
    ):

        chunks.append(
            " ".join(
                documents[
                    i:i + chunk_size
                ]
            )
        )

    return chunks


# ============================================================
# ADD STOCK CHUNKS
# ============================================================

def add_stock_chunks(
    collection,
    chunks,
    embedder,
    ticker,
    start_id
):

    next_id = start_id

    for i in range(
        0,
        len(chunks),
        ENCODE_BATCH_SIZE
    ):

        batch = chunks[
            i:i + ENCODE_BATCH_SIZE
        ]

        embeddings = embedder.encode(
            batch,
            show_progress_bar=False
        ).tolist()

        ids = [
            f"stock_{ticker}_{next_id + j}"
            for j in range(len(batch))
        ]

        metadatas = [
            {
                "ticker": ticker,
                "type": "stock"
            }
            for _ in batch
        ]

        collection.add(
            documents=batch,
            embeddings=embeddings,
            ids=ids,
            metadatas=metadatas
        )

        next_id += len(batch)

    return next_id


# ============================================================
# ADD GENERIC CHUNKS
# ============================================================

def add_generic_chunks(
    collection,
    chunks,
    embedder,
    data_type,
    start_id
):

    next_id = start_id

    for i in range(
        0,
        len(chunks),
        ENCODE_BATCH_SIZE
    ):

        batch = chunks[
            i:i + ENCODE_BATCH_SIZE
        ]

        embeddings = embedder.encode(
            batch,
            show_progress_bar=False
        ).tolist()

        ids = [
            f"{data_type}_{next_id + j}"
            for j in range(len(batch))
        ]

        metadatas = [
            {
                "type": data_type
            }
            for _ in batch
        ]

        collection.add(
            documents=batch,
            embeddings=embeddings,
            ids=ids,
            metadatas=metadatas
        )

        next_id += len(batch)

    return next_id


# ============================================================
# BUILD DATABASE
# ============================================================

def build_database():

    print("=" * 60)
    print("BUILDING RAG VECTOR DATABASE")
    print("=" * 60)

    # --------------------------------------------------------
    # Embedding model
    # --------------------------------------------------------

    print("\nLoading embedding model...")

    embedder = SentenceTransformer(
        EMBEDDING_MODEL_NAME
    )

    # --------------------------------------------------------
    # ChromaDB
    # --------------------------------------------------------

    print("Setting up ChromaDB...")

    client = chromadb.PersistentClient(
        path=CHROMA_DB_PATH
    )

    try:

        client.delete_collection(
            COLLECTION_NAME
        )

        print(
            "Old collection deleted."
        )

    except Exception:

        pass

    collection = client.create_collection(
        name=COLLECTION_NAME
    )

    next_id = 0

    # --------------------------------------------------------
    # STOCK FILES
    # --------------------------------------------------------

    if not os.path.isdir(
        STOCKHISTORY_DIR
    ):

        raise FileNotFoundError(
            f"Stock directory not found:\n"
            f"{STOCKHISTORY_DIR}"
        )

    ticker_files = sorted(
        glob.glob(
            os.path.join(
                STOCKHISTORY_DIR,
                "*.csv"
            )
        )
    )

    print(
        f"\nFound {len(ticker_files):,} "
        f"stock CSV files."
    )

    if LIMIT_TICKERS is not None:

        ticker_files = (
            ticker_files[:LIMIT_TICKERS]
        )

    print(
        f"Using {len(ticker_files):,} "
        f"tickers for this build."
    )

    total_rows = 0

    # --------------------------------------------------------
    # Process stocks
    # --------------------------------------------------------

    for path in tqdm(
        ticker_files,
        desc="Processing tickers"
    ):

        ticker = os.path.splitext(
            os.path.basename(path)
        )[0]

        try:

            docs = stock_csv_to_documents(
                path,
                ticker,
                years_back=YEARS_BACK
            )

        except Exception as e:

            print(
                f"WARNING: Skipping "
                f"{ticker}: {e}"
            )

            continue

        if not docs:
            continue

        total_rows += len(docs)

        chunks = make_chunks(
            docs,
            CHUNK_SIZE
        )

        next_id = add_stock_chunks(
            collection=collection,
            chunks=chunks,
            embedder=embedder,
            ticker=ticker,
            start_id=next_id
        )

    print(
        f"\nTotal stock rows embedded: "
        f"{total_rows:,}"
    )

    # --------------------------------------------------------
    # Other datasets
    # --------------------------------------------------------

    files_to_process = [

        (
            METRICS_CSV_PATH,
            "Model evaluation metric",
            "metrics"
        ),

        (
            WALKFORWARD_CSV_PATH,
            "Walk-forward validation",
            "walkfwd"
        )

    ]

    for (
        path,
        label,
        prefix
    ) in files_to_process:

        if not os.path.exists(path):

            print(
                f"NOTE: File not found: "
                f"{path}"
            )

            continue

        docs = generic_csv_to_documents(
            path,
            label
        )

        chunks = make_chunks(
            docs,
            CHUNK_SIZE
        )

        next_id = add_generic_chunks(
            collection=collection,
            chunks=chunks,
            embedder=embedder,
            data_type=prefix,
            start_id=next_id
        )

        print(
            f"{os.path.basename(path)}:"
        )

        print(
            f"  Rows: {len(docs):,}"
        )

        print(
            f"  Chunks: {len(chunks):,}"
        )

    # --------------------------------------------------------
    # Final information
    # --------------------------------------------------------

    print("\n" + "=" * 60)
    print("DATABASE BUILD COMPLETE")
    print("=" * 60)

    print(
        f"Location: {CHROMA_DB_PATH}"
    )

    print(
        f"Collection: {COLLECTION_NAME}"
    )

    print(
        f"Total chunks: {next_id:,}"
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    build_database()
