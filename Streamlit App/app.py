# ============================================================
# Stock AI Assistant
# RAG + ML Prediction + Model Performance
# ============================================================

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path


# ============================================================
# Page Configuration
# ============================================================

st.set_page_config(
    page_title="Stock AI Assistant",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

METRICS_PATH = BASE_DIR / "metrics_all_models.csv"
WALK_FORWARD_PATH = BASE_DIR / "walk_forward.csv"


# ============================================================
# Imports
# ============================================================

from rag_chat import ask_rag
import prediction
from prediction import predict_ticker

# Fix for metadata files created in Kaggle
# where TrainOnlyScaler was stored under __main__
import __main__

__main__.TrainOnlyScaler = prediction.TrainOnlyScaler


# ============================================================
# Styling
# ============================================================

st.markdown(
    """
    <style>

    .main-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }

    .subtitle {
        font-size: 1.05rem;
        color: #777;
        margin-bottom: 1.5rem;
    }

    .prediction-box {
        padding: 1.2rem;
        border-radius: 12px;
        border: 1px solid #ddd;
        margin-top: 1rem;
    }

    /* ========================================================
       BIG TABS
       ======================================================== */

    button[data-baseweb="tab"] {
        font-size: 1.15rem !important;
        font-weight: 600 !important;
        padding: 0.8rem 1.8rem !important;
        min-height: 55px !important;
    }

    button[data-baseweb="tab"] p {
        font-size: 1.15rem !important;
        font-weight: 600 !important;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# Header
# ============================================================

st.markdown(
    '<div class="main-title">📈 Stock AI Assistant</div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="subtitle">
    Hybrid Stock Analysis System using RAG, Machine Learning,
    and historical market data.
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# Sidebar
# ============================================================

with st.sidebar:

    st.header("⚙️ Settings")

    ticker = st.text_input(
        "Stock Ticker",
        value="AACG",
        max_chars=5,
        help="Example: AACG, AAPL, MSFT",
    ).strip().upper()

    st.divider()

    st.subheader("System Status")

    chroma_path = BASE_DIR / "chroma_db"

    if chroma_path.exists():
        st.success("🟢 ChromaDB: Connected")
    else:
        st.error("🔴 ChromaDB: Not Found")

    models_path = BASE_DIR / "models" / "Huber"

    if models_path.exists():
        st.success("🟢 ML Models: Available")
    else:
        st.error("🔴 ML Models: Not Found")

    if METRICS_PATH.exists():
        st.success("🟢 ML Metrics: Available")
    else:
        st.warning("🟡 ML Metrics: Not Found")

    st.divider()

    st.caption(
        "Prediction model: Huber Regressor"
    )

    st.caption(
        "Forecast horizon: 5 trading days"
    )

    st.caption(
        "Input window: 20 trading days"
    )


# ============================================================
# Tabs
# ============================================================

tab_chat, tab_prediction, tab_ml = st.tabs(
    [
        "💬 Chat / RAG",
        "🤖 Prediction",
        "📊 ML Performance",
    ]
)


# ============================================================
# TAB 1 — CHAT / RAG
# ============================================================

with tab_chat:

    st.header("💬 Stock RAG Assistant")

    st.write(
        "Ask questions about historical stock data, "
        "model results, and walk-forward evaluation."
    )

    st.info(
        f"Current ticker: **{ticker}**"
    )

    question = st.text_area(
        "Ask your question",
        placeholder=(
            "Example:\n"
            "What happened to AACG in 2025?\n"
            "Show me the historical prices of AACG.\n"
            "What are the model results for AACG?"
        ),
        height=120,
    )

    ask_button = st.button(
        "🔎 Ask",
        type="primary",
        key="rag_button",
    )

    if ask_button:

        if not question.strip():

            st.warning(
                "Please enter a question."
            )

        else:

            with st.spinner(
                "Searching stock knowledge base..."
            ):

                try:

                    query = question.strip()

                    words = query.upper().split()

                    ticker_in_question = any(
                        word.strip(".,!?()[]")
                        == ticker
                        for word in words
                    )

                    if not ticker_in_question:

                        query = (
                            f"{query} "
                            f"for stock {ticker}"
                        )

                    result = ask_rag(query)

                    answer = result.get(
                        "answer",
                        "No answer returned."
                    )

                    sources = result.get(
                        "sources",
                        []
                    )

                    st.subheader(
                        "Answer"
                    )

                    st.write(answer)

                    if sources:

                        with st.expander(
                            "📚 Retrieved Sources"
                        ):

                            for i, source in enumerate(
                                sources,
                                start=1
                            ):

                                st.markdown(
                                    f"**Source {i}**"
                                )

                                if isinstance(
                                    source,
                                    dict
                                ):

                                    document = source.get(
                                        "document",
                                        source.get(
                                            "text",
                                            ""
                                        )
                                    )

                                    metadata = source.get(
                                        "metadata",
                                        {}
                                    )

                                    if document:
                                        st.write(document)

                                    if metadata:
                                        st.caption(
                                            str(metadata)
                                        )

                                else:
                                    st.write(source)

                                if i < len(sources):
                                    st.divider()

                except Exception as e:

                    st.error("RAG Error")
                    st.exception(e)


# ============================================================
# TAB 2 — ML PREDICTION
# ============================================================

with tab_prediction:

    st.header("🤖 ML Stock Prediction")

    st.write(
        """
        This prediction is generated directly by the saved
        per-ticker **Huber Regression model**.

        The LLM is **not** used to generate the numerical
        prediction.
        """
    )

    st.warning(
        "⚠️ This is a machine-learning estimate, "
        "not financial advice."
    )

    predict_button = st.button(
        f"🚀 Predict {ticker}",
        type="primary",
        key="prediction_button",
    )

    if predict_button:

        if not ticker:

            st.error(
                "Please enter a ticker."
            )

        else:

            with st.spinner(
                f"Running ML prediction for {ticker}..."
            ):

                try:

                    result = predict_ticker(ticker)

                    st.subheader(
                        f"📊 {ticker} Prediction"
                    )

                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric(
                            "Current Price",
                            f"${result['current_price']:.2f}",
                        )

                    with col2:

                        return_pct = (
                            result[
                                "predicted_return_pct"
                            ]
                        )

                        st.metric(
                            "Predicted 5-Day Return",
                            f"{return_pct:.2f}%",
                        )

                    with col3:

                        st.metric(
                            "Implied Future Price",
                            f"${result['predicted_price']:.2f}",
                        )

                    with col4:

                        direction = result["direction"]

                        if direction == "UP":
                            direction_display = "🟢 UP"

                        elif direction == "DOWN":
                            direction_display = "🔴 DOWN"

                        else:
                            direction_display = "⚪ FLAT"

                        st.metric(
                            "Direction",
                            direction_display,
                        )

                    st.divider()

                    col1, col2, col3 = st.columns(3)

                    with col1:

                        st.write("**Model**")
                        st.write(result["model"])

                    with col2:

                        st.write("**Latest Data**")
                        st.write(result["latest_date"])

                    with col3:

                        st.write("**Historical Rows Used**")
                        st.write(result["rows_used"])

                    st.divider()

                    st.subheader(
                        "🧠 Prediction Pipeline"
                    )

                    st.markdown(
                        """
                        **ChromaDB historical data**

                        ↓

                        **Latest 20 trading days**

                        ↓

                        **return_1d + log_volume**

                        ↓

                        **Training scaler**

                        ↓

                        **Huber Regressor**

                        ↓

                        **5-day predicted return**

                        ↓

                        **Implied future price**
                        """
                    )

                    with st.expander(
                        "🔍 Technical Prediction Details"
                    ):

                        st.json(result)

                except FileNotFoundError as e:

                    st.error(
                        "❌ Model/Data Not Found"
                    )

                    st.write(str(e))

                except ValueError as e:

                    st.error(
                        "❌ Prediction Error"
                    )

                    st.write(str(e))

                except Exception as e:

                    st.error(
                        "❌ Unexpected Error"
                    )

                    st.exception(e)


# ============================================================
# TAB 3 — ML PERFORMANCE
# ============================================================

with tab_ml:

    st.header("📊 Machine Learning Performance")

    st.write(
        """
        Evaluation results from the ML training pipeline.
        Models were evaluated per ticker using the held-out
        test set.
        """
    )

    # ========================================================
    # Load metrics
    # ========================================================

    if not METRICS_PATH.exists():

        st.error(
            f"Could not find:\n{METRICS_PATH}"
        )

    else:

        try:

            metrics_df = pd.read_csv(
                METRICS_PATH
            )

            # ------------------------------------------------
            # Make sure numeric columns are numeric
            # ------------------------------------------------

            numeric_columns = [
                "rmse",
                "mae",
                "dir_acc",
                "pred_std",
                "pred_std_ratio",
                "rmse_vs_naive_pct",
            ]

            for col in numeric_columns:

                if col in metrics_df.columns:

                    metrics_df[col] = pd.to_numeric(
                        metrics_df[col],
                        errors="coerce"
                    )

            # ==================================================
            # Dataset overview
            # ==================================================

            st.subheader(
                "📋 Evaluation Dataset"
            )

            c1, c2, c3, c4 = st.columns(4)

            with c1:

                if "ticker" in metrics_df.columns:

                    ticker_count = (
                        metrics_df["ticker"]
                        .nunique()
                    )

                    st.metric(
                        "Tickers",
                        f"{ticker_count:,}"
                    )

                else:

                    st.metric(
                        "Rows",
                        f"{len(metrics_df):,}"
                    )

            with c2:

                if "model" in metrics_df.columns:

                    model_count = (
                        metrics_df["model"]
                        .nunique()
                    )

                    st.metric(
                        "Models",
                        f"{model_count:,}"
                    )

                else:

                    st.metric(
                        "Models",
                        "N/A"
                    )

            # ------------------------------------------------
            # Median instead of mean
            # ------------------------------------------------

            with c3:

                if "rmse" in metrics_df.columns:

                    median_rmse = (
                        metrics_df["rmse"]
                        .replace(
                            [np.inf, -np.inf],
                            np.nan
                        )
                        .median()
                    )

                    st.metric(
                        "Median RMSE",
                        f"{median_rmse:.4f}"
                    )

            with c4:

                if "mae" in metrics_df.columns:

                    median_mae = (
                        metrics_df["mae"]
                        .replace(
                            [np.inf, -np.inf],
                            np.nan
                        )
                        .median()
                    )

                    st.metric(
                        "Median MAE",
                        f"{median_mae:.4f}"
                    )

            st.caption(
                "Median is used because a small number of "
                "extreme ticker-level errors strongly distort "
                "the arithmetic mean."
            )

            # ==================================================
            # Model comparison
            # ==================================================

            st.divider()

            st.subheader(
                "🏆 Model Comparison"
            )

            if "model" in metrics_df.columns:

                numeric_metrics = [
                    col
                    for col in [
                        "rmse",
                        "mae",
                        "dir_acc",
                        "pred_std_ratio",
                        "rmse_vs_naive_pct",
                    ]
                    if col in metrics_df.columns
                ]

                if numeric_metrics:

                    # ------------------------------------------------
                    # Use MEDIAN across tickers
                    # ------------------------------------------------

                    grouped = (
                        metrics_df
                        .groupby(
                            "model",
                            as_index=False
                        )[numeric_metrics]
                        .median()
                    )

                    if "rmse" in grouped.columns:

                        grouped = grouped.sort_values(
                            "rmse"
                        )

                    # ------------------------------------------------
                    # Display comparison table
                    # ------------------------------------------------

                    st.dataframe(
                        grouped,
                        use_container_width=True,
                        hide_index=True,
                    )

                    # ------------------------------------------------
                    # Best model
                    # ------------------------------------------------

                    if "rmse" in grouped.columns:

                        best_row = grouped.iloc[0]

                        st.success(
                            f"🏆 Best model by median RMSE: "
                            f"**{best_row['model']}** "
                            f"with RMSE = "
                            f"**{best_row['rmse']:.4f}**"
                        )

                    # ==================================================
                    # RMSE BAR CHART
                    # ==================================================

                    if "rmse" in grouped.columns:

                        st.subheader(
                            "📉 RMSE by Model"
                        )

                        rmse_chart = (
                            grouped[
                                [
                                    "model",
                                    "rmse"
                                ]
                            ]
                            .set_index("model")
                        )

                        st.bar_chart(
                            rmse_chart,
                            use_container_width=True,
                        )

                    # ==================================================
                    # MAE BAR CHART
                    # ==================================================

                    if "mae" in grouped.columns:

                        st.subheader(
                            "📊 MAE by Model"
                        )

                        mae_chart = (
                            grouped[
                                [
                                    "model",
                                    "mae"
                                ]
                            ]
                            .set_index("model")
                        )

                        st.bar_chart(
                            mae_chart,
                            use_container_width=True,
                        )

                    # ==================================================
                    # Direction Accuracy BAR CHART
                    # ==================================================

                    if "dir_acc" in grouped.columns:

                        st.subheader(
                            "🎯 Directional Accuracy by Model"
                        )

                        dir_df = (
                            grouped[
                                [
                                    "model",
                                    "dir_acc"
                                ]
                            ]
                            .set_index("model")
                            .copy()
                        )

                        dir_df["dir_acc"] = (
                            dir_df["dir_acc"] * 100
                        )

                        st.bar_chart(
                            dir_df,
                            use_container_width=True,
                        )

                    # ==================================================
                    # Prediction Stability BAR CHART
                    # ==================================================

                    if "pred_std_ratio" in grouped.columns:

                        st.subheader(
                            "📐 Prediction Standard Deviation Ratio"
                        )

                        std_chart = (
                            grouped[
                                [
                                    "model",
                                    "pred_std_ratio"
                                ]
                            ]
                            .set_index("model")
                        )

                        st.bar_chart(
                            std_chart,
                            use_container_width=True,
                        )

            # ==================================================
            # Selected ticker performance
            # ==================================================

            st.divider()

            st.subheader(
                f"🔎 Performance for {ticker}"
            )

            if "ticker" in metrics_df.columns:

                ticker_df = metrics_df[
                    metrics_df["ticker"]
                    .astype(str)
                    .str.upper()
                    == ticker
                ].copy()

                if ticker_df.empty:

                    st.info(
                        f"No metrics found for {ticker}."
                    )

                else:

                    # ------------------------------
                    # Table
                    # ------------------------------

                    display_columns = [
                        col
                        for col in [
                            "ticker",
                            "model",
                            "rmse",
                            "mae",
                            "dir_acc",
                            "pred_std_ratio",
                            "rmse_vs_naive_pct",
                        ]
                        if col in ticker_df.columns
                    ]

                    st.dataframe(
                        ticker_df[
                            display_columns
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )

                    # ------------------------------
                    # Best model for ticker
                    # ------------------------------

                    if "rmse" in ticker_df.columns:

                        ticker_clean = (
                            ticker_df
                            .replace(
                                [np.inf, -np.inf],
                                np.nan
                            )
                            .dropna(
                                subset=["rmse"]
                            )
                        )

                        if not ticker_clean.empty:

                            best_ticker = (
                                ticker_clean
                                .sort_values("rmse")
                                .iloc[0]
                            )

                            st.success(
                                f"🏆 Best model for "
                                f"**{ticker}**: "
                                f"**{best_ticker['model']}** "
                                f"(RMSE = "
                                f"{best_ticker['rmse']:.4f})"
                            )

                            # ==========================================
                            # Selected ticker charts
                            # ==========================================

                            st.subheader(
                                f"📊 {ticker} Model Comparison"
                            )

                            chart_cols = st.columns(2)

                            # RMSE
                            if "rmse" in ticker_df.columns:

                                with chart_cols[0]:

                                    ticker_rmse_chart = (
                                        ticker_df[
                                            [
                                                "model",
                                                "rmse"
                                            ]
                                        ]
                                        .set_index("model")
                                    )

                                    st.write(
                                        "**RMSE**"
                                    )

                                    st.bar_chart(
                                        ticker_rmse_chart,
                                        use_container_width=True,
                                    )

                            # MAE
                            if "mae" in ticker_df.columns:

                                with chart_cols[1]:

                                    ticker_mae_chart = (
                                        ticker_df[
                                            [
                                                "model",
                                                "mae"
                                            ]
                                        ]
                                        .set_index("model")
                                    )

                                    st.write(
                                        "**MAE**"
                                    )

                                    st.bar_chart(
                                        ticker_mae_chart,
                                        use_container_width=True,
                                    )

                            # Direction Accuracy
                            if "dir_acc" in ticker_df.columns:

                                st.write(
                                    "**Directional Accuracy**"
                                )

                                ticker_dir_chart = (
                                    ticker_df[
                                        [
                                            "model",
                                            "dir_acc"
                                        ]
                                    ]
                                    .set_index("model")
                                    .copy()
                                )

                                ticker_dir_chart[
                                    "dir_acc"
                                ] *= 100

                                st.bar_chart(
                                    ticker_dir_chart,
                                    use_container_width=True,
                                )

            # ==================================================
            # Raw metrics
            # ==================================================

            with st.expander(
                "📄 View Complete Metrics Dataset"
            ):

                st.dataframe(
                    metrics_df,
                    use_container_width=True,
                    hide_index=True,
                )

        except Exception as e:

            st.error(
                "Could not load ML metrics."
            )

            st.exception(e)


# ============================================================
# WALK-FORWARD ANALYSIS
# ============================================================

with tab_ml:

    st.divider()

    st.subheader(
        "🔄 Walk-Forward Evaluation"
    )

    st.write(
        """
        Walk-forward evaluation shows how the selected model
        behaves across chronological evaluation periods,
        rather than relying only on a single test split.
        """
    )

    if not WALK_FORWARD_PATH.exists():

        st.info(
            "walk_forward.csv was not found."
        )

    else:

        try:

            wf_df = pd.read_csv(
                WALK_FORWARD_PATH
            )

            if "ticker" in wf_df.columns:

                ticker_wf = wf_df[
                    wf_df["ticker"]
                    .astype(str)
                    .str.upper()
                    == ticker
                ].copy()

            else:

                ticker_wf = wf_df.copy()

            if ticker_wf.empty:

                st.info(
                    f"No walk-forward results found "
                    f"for {ticker}."
                )

            else:

                st.dataframe(
                    ticker_wf,
                    use_container_width=True,
                    hide_index=True,
                )

                numeric_cols = (
                    ticker_wf
                    .select_dtypes(
                        include=np.number
                    )
                    .columns
                    .tolist()
                )

                if numeric_cols:

                    st.subheader(
                        "📈 Walk-Forward Metrics"
                    )

                    # Use median here as well
                    summary = (
                        ticker_wf[
                            numeric_cols
                        ]
                        .median()
                        .to_frame("Median")
                    )

                    st.dataframe(
                        summary,
                        use_container_width=True,
                    )

                    # ------------------------------------------------
                    # Walk-forward chart
                    # ------------------------------------------------

                    chart_cols = [
                        col
                        for col in [
                            "rmse",
                            "mae",
                            "dir_acc"
                        ]
                        if col in ticker_wf.columns
                    ]

                    if chart_cols:

                        st.subheader(
                            "📊 Walk-Forward Performance"
                        )

                        wf_chart = (
                            ticker_wf[
                                chart_cols
                            ]
                            .copy()
                        )

                        if "dir_acc" in wf_chart.columns:

                            wf_chart["dir_acc"] *= 100

                        st.line_chart(
                            wf_chart,
                            use_container_width=True,
                        )

            with st.expander(
                "📄 View Complete Walk-Forward Dataset"
            ):

                st.dataframe(
                    wf_df,
                    use_container_width=True,
                    hide_index=True,
                )

        except Exception as e:

            st.error(
                "Could not load walk-forward results."
            )

            st.exception(e)


# ============================================================
# Footer
# ============================================================

st.divider()

st.caption(
    "Stock AI Assistant • RAG + Machine Learning"
)

st.caption(
    "Numerical predictions are generated by the saved ML "
    "models, not by the language model."
)
