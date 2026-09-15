"""
app.py
======
Streamlit chat interface for the RAG Stock Assistant.

Usage
-----
    streamlit run app.py

Prerequisite: run `python src/build_vector_db.py` first to build the
vector database — this app only queries it, it does not build it.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

import streamlit as st

from rag_chat import ask_rag

st.set_page_config(page_title="RAG Stock Assistant", page_icon="📈")

st.title("📈 RAG Stock Assistant")
st.caption(
    "Ask any question about the available stock data — answers are "
    "grounded in the project's knowledge base."
)

# Conversation history, kept in session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Render past messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# User input
question = st.chat_input("Ask a question...")

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Searching the knowledge base..."):
            try:
                result = ask_rag(question)
                st.markdown(result["answer"])

                with st.expander("📚 Sources used for this answer"):
                    for i, src in enumerate(result["sources"], 1):
                        st.markdown(f"**{i}.** {src}")

                st.session_state.messages.append(
                    {"role": "assistant", "content": result["answer"]}
                )
            except Exception as e:
                error_msg = f"An error occurred: {e}"
                st.error(error_msg)
                st.session_state.messages.append(
                    {"role": "assistant", "content": error_msg}
                )
