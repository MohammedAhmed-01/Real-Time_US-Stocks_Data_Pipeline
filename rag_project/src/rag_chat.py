"""
rag_chat.py
===========
Runtime query pipeline. For each user question:

    1. Embed the question with the same embedding model used at index time
    2. Retrieve the top-N most similar chunks from ChromaDB
    3. Build a prompt containing the question + retrieved chunks
    4. Send the prompt to Groq's Llama 3.1 and return the generated answer

This module is not normally run directly — it's imported by app.py
(the Streamlit UI). A minimal CLI smoke test is included at the bottom.
"""

from __future__ import annotations

import os

import chromadb
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from sentence_transformers import SentenceTransformer

load_dotenv()

CHROMA_DB_PATH = "chroma_db"
COLLECTION_NAME = "stocks_knowledge_base"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
N_RESULTS = 5  # number of chunks retrieved per question

# Lazily-loaded singletons so the model/DB/LLM are only initialized once,
# not on every question.
_embedder: SentenceTransformer | None = None
_collection = None
_llm: ChatGroq | None = None


def _load_resources() -> None:
    """Load the embedding model, vector DB collection, and LLM client (once)."""
    global _embedder, _collection, _llm

    if _embedder is None:
        _embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    if _collection is None:
        client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        _collection = client.get_collection(COLLECTION_NAME)

    if _llm is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError(
                "GROQ_API_KEY is not set. Copy .env.example to .env and add your key."
            )
        _llm = ChatGroq(
            groq_api_key=api_key,
            model_name="llama-3.1-8b-instant",
            temperature=0.2,
        )


def retrieve_context(question: str, n_results: int = N_RESULTS) -> list[str]:
    """Retrieve the top-N most relevant chunks for a given question."""
    _load_resources()
    question_embedding = _embedder.encode([question]).tolist()
    results = _collection.query(
        query_embeddings=question_embedding,
        n_results=n_results,
    )
    return results["documents"][0]


def ask_rag(question: str) -> dict:
    """
    Main entry point: takes a user question and returns a dict with the
    generated answer and the source chunks used to produce it.
    """
    _load_resources()

    # 1) Retrieve the most relevant chunks
    context_chunks = retrieve_context(question)
    context_text = "\n\n".join(f"- {c}" for c in context_chunks)

    # 2) Build the prompt
    system_prompt = (
        "You are an expert assistant for US stock market data. "
        "Answer the user's question using ONLY the information provided below. "
        "If the available information is not sufficient to answer, say so clearly "
        "instead of guessing."
    )
    user_prompt = f"Available information:\n{context_text}\n\nQuestion: {question}"

    # 3) Call the LLM
    response = _llm.invoke(
        [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
    )

    return {
        "answer": response.content,
        "sources": context_chunks,
    }


if __name__ == "__main__":
    # Quick CLI smoke test
    q = "What was the highest closing price recorded in the data?"
    result = ask_rag(q)
    print("Question:", q)
    print("\nAnswer:", result["answer"])
    print("\nSource chunks used:")
    for s in result["sources"]:
        print("-", s)
