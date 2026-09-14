# RAG Stock Assistant

A Retrieval-Augmented Generation (RAG) chat assistant for querying US stock
market data in natural language. Built entirely with free and open-source
tooling, as the RAG component of a larger data engineering pipeline
(Kafka → Spark → HDFS/Parquet → PostgreSQL → ML → RAG → Streamlit).

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [Running the Assistant](#running-the-assistant)
- [Configuration](#configuration)
- [Swapping in Real Data](#swapping-in-real-data)
- [Troubleshooting](#troubleshooting)
- [Tech Stack](#tech-stack)

---

## Overview

This assistant lets a user ask free-text questions about stock data (e.g.
*"What was AAPL's closing price trend in January?"*) and get answers
grounded in an actual knowledge base — rather than relying on the LLM's
general (and potentially outdated or hallucinated) knowledge.

It works in two phases:

1. **Indexing** (`build_vector_db.py`, run once per data update): source
   CSVs are converted to natural-language text, split into chunks,
   embedded, and stored in a local vector database.
2. **Querying** (`app.py`, run continuously): a user's question is
   embedded, the most relevant chunks are retrieved by similarity search,
   and both are sent to an LLM to produce a grounded answer.

## Architecture

```
                     ┌─────────────────────┐
  Source CSVs   ───► │  text_converter.py  │  row → natural-language sentence
 (stock prices,      └──────────┬──────────┘
  ML predictions)                │
                                  ▼
                       ┌────────────────────┐
                       │  build_vector_db.py │  chunk → embed → persist
                       └──────────┬─────────┘
                                  ▼
                          ┌───────────────┐
                          │   ChromaDB    │  local vector store (chroma_db/)
                          │ (persisted)   │
                          └───────┬───────┘
                                  │  similarity search
                                  ▼
   User question ──►  ┌─────────────────┐        ┌─────────────┐
                       │   rag_chat.py   │ ─────► │  Groq API   │
                       │ (retrieval +    │        │ (Llama 3.1) │
                       │  prompt build)  │ ◄───── │             │
                       └────────┬────────┘        └─────────────┘
                                  ▼
                          ┌───────────────┐
                          │    app.py     │  Streamlit chat UI
                          └───────────────┘
```

## Project Structure

```
rag_project/
├── app.py                        # Streamlit chat UI (entry point)
├── requirements.txt              # Python dependencies
├── .env.example                  # Template for API key configuration
├── README.md                     # This file
├── data/
│   ├── sample_stock_data.csv         # Sample OHLCV price data
│   └── sample_predictions.csv        # Sample ML model prediction data
├── src/
│   ├── text_converter.py         # CSV rows → natural-language text
│   ├── build_vector_db.py        # Chunking + embeddings + vector store build
│   └── rag_chat.py               # Retrieval + LLM query logic
└── chroma_db/                    # Persisted vector database (auto-generated)
```

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.10+ | [Download](https://www.python.org/downloads/) |
| Internet connection | Required once, to download the embedding model and install packages |
| Groq API key (free) | Sign up at [console.groq.com](https://console.groq.com/keys) |

## Setup & Installation

**1. Unzip the project** anywhere on your machine.

**2. Open a terminal in the project root:**
```bash
cd rag_project
```

**3. Install dependencies:**
```bash
pip install -r requirements.txt
```

**4. Get a free Groq API key:**
- Go to [console.groq.com/keys](https://console.groq.com/keys)
- Sign up / log in
- Click "Create API Key" and copy it

**5. Configure your environment:**
Copy `.env.example` to a new file named `.env`, then edit it:
```
GROQ_API_KEY=your_actual_key_here
```

## Running the Assistant

**Step 1 — Build the knowledge base** (required before first run, and
again any time the source data changes):
```bash
python src/build_vector_db.py
```
This downloads the embedding model on first run (~90 MB, cached
afterward), converts the CSV data to text, generates embeddings, and
writes them to `chroma_db/`.

**Step 2 — Launch the chat interface:**
```bash
streamlit run app.py
```
This opens the assistant at `http://localhost:8501`.

**Optional — test retrieval from the command line** (no Streamlit needed):
```bash
python src/rag_chat.py
```

## Configuration

All tunable settings live at the top of `src/build_vector_db.py` and
`src/rag_chat.py` as module-level constants — no need to dig through the
code:

| Setting | File | Default | Purpose |
|---|---|---|---|
| `EMBEDDING_MODEL_NAME` | both | `all-MiniLM-L6-v2` | Sentence-transformer model used for embeddings |
| `CHUNK_SIZE` | `build_vector_db.py` | `5` | Sentences grouped per chunk |
| `N_RESULTS` | `rag_chat.py` | `5` | Chunks retrieved per question |
| `COLLECTION_NAME` | both | `stocks_knowledge_base` | ChromaDB collection name |
| LLM model | `rag_chat.py` | `llama-3.1-8b-instant` | Groq-hosted model used for generation |

## Swapping in Real Data

The project currently ships with **sample data** (`data/sample_stock_data.csv`,
`data/sample_predictions.csv`) so it can be run end-to-end immediately.
To switch to the real pipeline data once it's available (from PostgreSQL,
the M5 ML pipeline's `predictions_best_model.csv`, etc.):

1. Place the new CSV file(s) in `data/`.
2. If column names differ from the defaults, update `STOCK_COLUMN_MAP` /
   `PREDICTION_COLUMN_MAP` at the top of `src/text_converter.py` — only
   the values need to change, not the keys.
3. Update `STOCK_CSV_PATH` / `PREDICTIONS_CSV_PATH` at the top of
   `src/build_vector_db.py` if the file names or locations changed.
4. Rebuild the knowledge base:
   ```bash
   python src/build_vector_db.py
   ```

No changes to `app.py` or `rag_chat.py` are needed — they read from
whatever is currently in `chroma_db/`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `GROQ_API_KEY is not set` | Make sure you created `.env` (not just `.env.example`) and added your key |
| `Collection does not exist` when launching the app | Run `python src/build_vector_db.py` before `streamlit run app.py` |
| First run is slow | Expected — the embedding model (~90 MB) downloads once and is cached locally afterward |
| `pip install` fails | Confirm Python is 3.10+: `python --version` |
| Answers seem generic / not grounded in the data | Rebuild the vector DB after any data change; check that `data/` contains the expected files |

## Tech Stack

| Layer | Tool |
|---|---|
| Embeddings | `sentence-transformers` (`all-MiniLM-L6-v2`) |
| Vector store | ChromaDB (persistent, local) |
| Orchestration | LangChain |
| LLM | Groq API — Llama 3.1 (free tier) |
| UI | Streamlit |

All components are free and open-source; no paid services are required.
