import os
import re
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer
from groq import Groq
from dotenv import load_dotenv


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

# Local ChromaDB
CHROMA_DB_PATH = BASE_DIR / "chroma_db"

# Chroma collection name used when building the database
COLLECTION_NAME = "stocks_knowledge_base"

# Embedding model
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

# Number of retrieved chunks
N_RESULTS = 5

# Groq model
GROQ_MODEL = "openai/gpt-oss-20b"


# ============================================================
# 2. LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv(BASE_DIR / ".env")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")


# ============================================================
# 3. GLOBAL RESOURCES
# ============================================================

_embedder = None
_collection = None
_llm = None


# ============================================================
# 4. LOAD EMBEDDING MODEL
# ============================================================

def get_embedder():
    global _embedder

    if _embedder is None:
        print("Loading embedding model...")
        _embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    return _embedder


# ============================================================
# 5. CONNECT TO CHROMADB
# ============================================================

def get_collection():
    global _collection

    if _collection is None:

        if not CHROMA_DB_PATH.exists():
            raise FileNotFoundError(
                f"ChromaDB folder was not found:\n{CHROMA_DB_PATH}"
            )

        print("Connecting to ChromaDB...")

        client = chromadb.PersistentClient(
            path=str(CHROMA_DB_PATH)
        )

        _collection = client.get_collection(
            name=COLLECTION_NAME
        )

        print(
            f"Connected to collection: "
            f"{COLLECTION_NAME}"
        )

        print(
            f"Number of documents: "
            f"{_collection.count()}"
        )

    return _collection


# ============================================================
# 6. INITIALIZE GROQ
# ============================================================

def get_llm():
    global _llm

    if _llm is None:

        if not GROQ_API_KEY:
            raise ValueError(
                "GROQ_API_KEY was not found.\n\n"
                "Create a .env file next to app.py and rag_chat.py "
                "and add:\n\n"
                "GROQ_API_KEY=your_api_key"
            )

        _llm = Groq(
            api_key=GROQ_API_KEY
        )

    return _llm


# ============================================================
# 7. TICKER DETECTION
# ============================================================

COMMON_WORDS = {
    "WHAT",
    "WAS",
    "THE",
    "IS",
    "ARE",
    "WERE",
    "HOW",
    "MUCH",
    "MANY",
    "DOES",
    "DID",
    "HAS",
    "HAVE",
    "FOR",
    "FROM",
    "TO",
    "OF",
    "ON",
    "IN",
    "AND",
    "OR",
    "WITH",
    "ABOUT",
    "PRICE",
    "CLOSE",
    "CLOSING",
    "OPEN",
    "HIGH",
    "LOW",
    "VOLUME",
    "STOCK",
    "STOCKS",
    "DATE",
    "MODEL",
    "MODELS",
    "RMSE",
    "MAE",
    "R2",
    "RETURN",
    "PREDICTION",
    "PREDICT",
    "NEXT",
    "DAY",
    "PERFORMANCE",
}


def extract_ticker(question):
    """
    Extract a possible US stock ticker from the question.

    Example:
        'What was AACB closing price?'
        -> 'AACB'
    """

    candidates = re.findall(
        r"\b[A-Z]{1,5}\b",
        question.upper()
    )

    for candidate in candidates:

        if candidate not in COMMON_WORDS:
            return candidate

    return None


# ============================================================
# 8. RETRIEVE CONTEXT
# ============================================================

def retrieve_context(question, n_results=N_RESULTS):

    embedder = get_embedder()
    collection = get_collection()

    ticker = extract_ticker(question)

    # Create question embedding
    query_embedding = embedder.encode(
        question,
        normalize_embeddings=True
    ).tolist()

    # --------------------------------------------------------
    # Ticker-specific retrieval
    # --------------------------------------------------------

    if ticker:

        try:

            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where={
                    "ticker": ticker
                }
            )

        except Exception as e:

            print(
                f"Ticker filtering failed: {e}"
            )

            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results
            )

    # --------------------------------------------------------
    # General retrieval
    # --------------------------------------------------------

    else:

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results
        )

    # ========================================================
    # Convert Chroma result into simple list
    # ========================================================

    context_chunks = []

    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    for i, document in enumerate(documents):

        metadata = (
            metadatas[i]
            if i < len(metadatas)
            else {}
        )

        distance = (
            distances[i]
            if i < len(distances)
            else None
        )

        context_chunks.append(
            {
                "text": document,
                "metadata": metadata,
                "distance": distance,
            }
        )

    return context_chunks


# ============================================================
# 9. BUILD CONTEXT FOR LLM
# ============================================================

def build_context(context_chunks):

    if not context_chunks:
        return "No relevant information was retrieved."

    context_parts = []

    for i, chunk in enumerate(context_chunks, start=1):

        metadata = chunk["metadata"]

        ticker = metadata.get(
            "ticker",
            "Unknown"
        )

        data_type = metadata.get(
            "type",
            "Unknown"
        )

        text = chunk["text"]

        context_parts.append(
            f"""
--- Source {i} ---
Ticker: {ticker}
Type: {data_type}

{text}
"""
        )

    return "\n".join(context_parts)


# ============================================================
# 10. ASK RAG
# ============================================================

def ask_rag(question):

    # Retrieve documents
    context_chunks = retrieve_context(
        question,
        n_results=N_RESULTS
    )

    # Build context
    context = build_context(
        context_chunks
    )

    # Get Groq client
    llm = get_llm()

    # --------------------------------------------------------
    # System prompt
    # --------------------------------------------------------

    system_prompt = """
You are a financial data assistant working strictly
with the retrieved knowledge base.

IMPORTANT RULES:

1. Answer ONLY using the retrieved context.
2. Do NOT use outside financial knowledge.
3. Do NOT invent stock prices, dates, volumes,
   returns, metrics, models, or predictions.
4. If the retrieved context does not contain enough
   information to answer the question, clearly say
   that the information was not found in the knowledge base.
5. Do NOT mix information from different tickers.
6. If a ticker is mentioned in the question, prioritize
   information belonging to that ticker.
7. Keep answers concise and clear.
8. When numerical values are available, preserve
   the values from the retrieved context accurately.
9. For model-performance questions, only use the
   retrieved model metrics.
10. Never generate an ML prediction yourself.
"""

    user_prompt = f"""
Question:
{question}

Retrieved Knowledge Base:
{context}

Answer the question using ONLY the retrieved knowledge base.
"""

    # --------------------------------------------------------
    # Groq request
    # --------------------------------------------------------

    response = llm.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ],
        temperature=0
    )

    answer = response.choices[0].message.content

    return {
        "answer": answer,
        "sources": context_chunks
    }


# ============================================================
# 11. SIMPLE TEST
# ============================================================

if __name__ == "__main__":

    print("=" * 60)
    print("Local RAG Test")
    print("=" * 60)

    question = input(
        "\nEnter your question: "
    ).strip()

    if not question:
        print("No question entered.")
        exit()

    try:

        ticker = extract_ticker(question)

        print(
            f"\nDetected ticker: {ticker}"
        )

        result = ask_rag(
            question
        )

        print("\n" + "=" * 60)
        print("ANSWER")
        print("=" * 60)

        print(
            result["answer"]
        )

        print("\n" + "=" * 60)
        print("SOURCES")
        print("=" * 60)

        for i, source in enumerate(
            result["sources"],
            start=1
        ):

            print(
                f"\nSource {i}"
            )

            print(
                "Metadata:",
                source["metadata"]
            )

            print(
                "Distance:",
                source["distance"]
            )

            print(
                source["text"]
            )

    except Exception as e:

        print(
            "\nERROR:"
        )

        print(e)