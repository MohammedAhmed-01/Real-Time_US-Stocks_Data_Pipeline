"""
build_vector_db.py
===================
Builds the local vector knowledge base from source CSV files. Pipeline:

    1. Read source CSV files (stock prices + ML predictions)
    2. Convert each row into a natural-language sentence (text_converter.py)
    3. Group sentences into chunks (simple fixed-size grouping)
    4. Generate embeddings for each chunk (sentence-transformers)
    5. Persist chunks + embeddings to ChromaDB on disk (chroma_db/)

Usage
-----
    python src/build_vector_db.py

Run this once whenever the source data changes, before starting the
Streamlit app. The resulting chroma_db/ directory is what the app queries
at runtime.
"""

from __future__ import annotations

import os

import chromadb
from sentence_transformers import SentenceTransformer

from text_converter import csv_to_documents

# ============================================================
# Configuration — adjust paths/settings as needed
# ============================================================
STOCK_CSV_PATH = "data/sample_stock_data.csv"
PREDICTIONS_CSV_PATH = "data/sample_predictions.csv"  # optional
CHROMA_DB_PATH = "chroma_db"
COLLECTION_NAME = "stocks_knowledge_base"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

# Number of sentences grouped into a single chunk (richer context per chunk)
CHUNK_SIZE = 5


def make_chunks(documents: list[str], chunk_size: int = CHUNK_SIZE) -> list[str]:
    """Group every `chunk_size` sentences into a single chunk of text."""
    chunks = []
    for i in range(0, len(documents), chunk_size):
        group = documents[i : i + chunk_size]
        chunks.append(" ".join(group))
    return chunks


def build_database() -> None:
    print("[1/6] Loading the embedding model (downloads once, then cached)...")
    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    all_documents: list[str] = []

    # -- Stock price data --
    if os.path.exists(STOCK_CSV_PATH):
        print(f"[2/6] Converting stock price data from '{STOCK_CSV_PATH}'...")
        stock_docs = csv_to_documents(STOCK_CSV_PATH, row_type="stock")
        all_documents.extend(stock_docs)
        print(f"      Converted {len(stock_docs)} rows.")
    else:
        print(f"      WARNING: '{STOCK_CSV_PATH}' not found — skipping.")

    # -- ML prediction data (optional) --
    if os.path.exists(PREDICTIONS_CSV_PATH):
        print(f"[3/6] Converting prediction data from '{PREDICTIONS_CSV_PATH}'...")
        pred_docs = csv_to_documents(PREDICTIONS_CSV_PATH, row_type="prediction")
        all_documents.extend(pred_docs)
        print(f"      Converted {len(pred_docs)} rows.")
    else:
        print(f"      NOTE: '{PREDICTIONS_CSV_PATH}' not found yet — skipping for now.")

    if not all_documents:
        print("No documents were generated. Check that your CSV files exist in data/.")
        return

    print(f"[4/6] Chunking {len(all_documents)} sentences (group size = {CHUNK_SIZE})...")
    chunks = make_chunks(all_documents)
    print(f"      Total chunks: {len(chunks)}")

    print("[5/6] Generating embeddings for all chunks (may take a moment)...")
    embeddings = embedder.encode(chunks, show_progress_bar=True).tolist()

    print("[6/6] Persisting chunks + embeddings to ChromaDB...")
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)

    # Drop any existing collection first to avoid duplicate entries on rebuild
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass

    collection = client.create_collection(COLLECTION_NAME)
    collection.add(
        documents=chunks,
        embeddings=embeddings,
        ids=[f"chunk_{i}" for i in range(len(chunks))],
    )

    print(f"\nDone. Vector database saved to '{CHROMA_DB_PATH}/'")
    print(f"Chunks stored: {len(chunks)}")


if __name__ == "__main__":
    build_database()
