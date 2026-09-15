from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import chromadb
from dotenv import load_dotenv
from langchain_core.messages import (
    HumanMessage,
    SystemMessage,
)
from langchain_groq import ChatGroq
from sentence_transformers import SentenceTransformer


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

CHROMA_DB_PATH = (
    BASE_DIR / "chroma_db"
)

COLLECTION_NAME = (
    "stocks_knowledge_base"
)

EMBEDDING_MODEL_NAME = (
    "all-MiniLM-L6-v2"
)

N_RESULTS = 5

GROQ_MODEL = (
    "openai/gpt-oss-20b"
)


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# GLOBAL OBJECTS
# ============================================================

_embedder = None
_collection = None
_llm = None


# ============================================================
# GROQ API KEY
# ============================================================

def _get_api_key():

    api_key = os.getenv(
        "GROQ_API_KEY"
    )

    if not api_key:

        raise ValueError(
            "GROQ_API_KEY was not found. "
            "Add it to the .env file."
        )

    return api_key


# ============================================================
# LOAD RESOURCES
# ============================================================

def _load_resources():

    global _embedder
    global _collection
    global _llm

    if _embedder is None:

        print(
            "Loading embedding model..."
        )

        _embedder = SentenceTransformer(
            EMBEDDING_MODEL_NAME
        )

    if _collection is None:

        print(
            "Loading ChromaDB..."
        )

        if not CHROMA_DB_PATH.exists():

            raise FileNotFoundError(
                f"ChromaDB directory not found:\n"
                f"{CHROMA_DB_PATH}"
            )

        client = chromadb.PersistentClient(
            path=str(CHROMA_DB_PATH)
        )

        _collection = client.get_collection(
            COLLECTION_NAME
        )

    if _llm is None:

        print(
            "Connecting to Groq..."
        )

        _llm = ChatGroq(
            groq_api_key=_get_api_key(),
            model_name=GROQ_MODEL,
            temperature=0.2,
        )


# ============================================================
# EXTRACT TICKER
# ============================================================

def extract_ticker(
    question: str
):

    candidates = re.findall(
        r"\b[A-Z]{1,5}\b",
        question.upper()
    )

    ignored_words = {
        "WHAT",
        "INFO",
        "INFORMATION",
        "ABOUT",
        "THE",
        "AND",
        "FOR",
        "FROM",
        "WITH",
        "STOCK",
        "PRICE",
        "DATA",
        "AVAILABLE",
        "IS",
        "ARE",
        "CAN",
        "YOU",
        "TELL",
        "ME",
        "SHOW",
        "GIVE",
        "HOW",
        "MUCH",
        "LAST",
        "DAYS",
        "DAY",
        "CLOSE",
        "CLOSING",
        "OPEN",
        "HIGH",
        "LOW",
        "VOLUME",
        "AVERAGE",
        "OVER",
        "TRADING",
    }

    for candidate in candidates:

        if candidate not in ignored_words:

            return candidate

    return None


# ============================================================
# RETRIEVE CONTEXT
# ============================================================

def retrieve_context(
    question: str,
    n_results: int = N_RESULTS
):

    _load_resources()

    ticker = extract_ticker(
        question
    )

    question_embedding = (
        _embedder
        .encode([question])
        .tolist()
    )

    if ticker:

        print(
            f"Detected ticker: {ticker}"
        )

        results = _collection.query(

            query_embeddings=question_embedding,

            n_results=n_results,

            where={
                "ticker": ticker
            }

        )

    else:

        print(
            "No ticker detected. "
            "Using general semantic search."
        )

        results = _collection.query(

            query_embeddings=question_embedding,

            n_results=n_results

        )

    documents = results.get(
        "documents",
        [[]]
    )

    if not documents:

        return []

    return documents[0]


# ============================================================
# ASK RAG
# ============================================================

def ask_rag(
    question: str
):

    _load_resources()

    context_chunks = retrieve_context(
        question
    )

    if not context_chunks:

        return {
            "answer": (
                "I could not find relevant "
                "information in the knowledge base."
            ),
            "sources": [],
        }

    context_text = "\n\n".join(

        f"- {chunk}"

        for chunk in context_chunks

    )

    system_prompt = """

You are an expert assistant for US stock
market data and machine learning model
evaluation.

You are part of a Retrieval-Augmented
Generation (RAG) system.

IMPORTANT RULES:

1. Answer ONLY using the retrieved context.

2. Do not use outside knowledge.

3. Do not invent stock prices, metrics,
   dates, models, or results.

4. If the retrieved context does not
   contain enough information, clearly
   say that the information is insufficient.

5. When discussing stock data, preserve
   the correct ticker symbol.

6. Do not mix information from different
   ticker symbols.

7. If the question is about one ticker,
   only use information belonging to that
   ticker.

8. Keep the answer clear and concise.

9. You may organize the retrieved data
   into a table when appropriate.

10. This RAG system is extractive/factual.
    Do NOT make future stock predictions.

11. Do not infer or calculate information
    that is not supported by the retrieved
    context.

"""

    user_prompt = f"""

Retrieved information:

{context_text}


User question:

{question}

Answer based ONLY on the retrieved
information.
"""

    response = _llm.invoke(
        [
            SystemMessage(
                content=system_prompt
            ),

            HumanMessage(
                content=user_prompt
            ),
        ]
    )

    return {
        "answer": response.content,
        "sources": context_chunks,
    }


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":

    question = (
        "What information is available "
        "about AACB?"
    )

    result = ask_rag(
        question
    )

    print("\nQUESTION:")
    print(question)

    print("\nANSWER:")
    print(result["answer"])

    print("\nSOURCES:")

    for source in result["sources"]:

        print("-")
        print(source)
