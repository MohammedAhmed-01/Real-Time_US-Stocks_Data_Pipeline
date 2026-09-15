# 📈 Stock RAG & ML Prediction Assistant

A local **AI-powered Stock Analysis Assistant** that combines **Retrieval-Augmented Generation (RAG)** with **machine learning-based stock return prediction**.

The application provides three main capabilities:

* 💬 **RAG Chat** — Ask questions about historical stock data and retrieve relevant information from a ChromaDB knowledge base.
* 🤖 **ML Prediction** — Generate a 5-day future return prediction using the pre-trained model selected for each ticker.
* 📊 **ML Performance** — Explore model performance, comparison metrics, and walk-forward evaluation results.

The application is built with **Streamlit**, **ChromaDB**, **Sentence Transformers**, **Groq**, and **Scikit-learn**.

---

## 🚀 Project Overview

The project combines two complementary approaches:

### 1. Retrieval-Augmented Generation

The RAG system retrieves relevant stock information from a pre-built vector database and provides it to an LLM as context.

The LLM is used to:

* Understand the user's question.
* Use the retrieved stock data.
* Generate a natural-language response.
* Avoid answering using information that is not present in the retrieved context.

The RAG system is designed primarily for **historical and factual stock-data questions**.

### 2. Machine Learning Prediction

A separate machine learning pipeline was trained on historical stock data.

For each ticker:

1. Historical stock data is processed.
2. Features are generated.
3. A 20-day rolling window is created.
4. The model predicts the **next 5-day return**.
5. Multiple ML models are evaluated.
6. The best-performing model is selected.
7. The selected model and its metadata are saved for later inference.

The Streamlit application loads these saved models and performs prediction locally without retraining.

---

# 🏗️ System Architecture

```text
                    ┌─────────────────────────┐
                    │      Streamlit App      │
                    │         app.py          │
                    └────────────┬────────────┘
                                 │
                ┌────────────────┼────────────────┐
                │                │                │
                ▼                ▼                ▼
        ┌──────────────┐ ┌──────────────┐ ┌───────────────┐
        │   Chat/RAG   │ │  Prediction  │ │ ML Performance│
        └──────┬───────┘ └──────┬───────┘ └───────┬───────┘
               │                │                 │
               ▼                ▼                 ▼
        ┌──────────────┐ ┌──────────────┐ ┌────────────────┐
        │   ChromaDB   │ │ Saved Models │ │ Metrics CSV    │
        │ Vector Store │ │  .joblib     │ │ Walk-forward   │
        └──────┬───────┘ │  + metadata  │ └────────────────┘
               │          └──────┬───────┘
               ▼                 │
        ┌──────────────┐         │
        │ Sentence     │         │
        │ Transformer  │         │
        │ Embeddings   │         │
        └──────┬───────┘         │
               │                 │
               ▼                 ▼
        ┌──────────────┐   ┌──────────────┐
        │ Groq LLM     │   │ ML Models    │
        │ gpt-oss-20b  │   │ Huber/Ridge/ │
        └──────────────┘   │ RF/XGB/etc.  │
                           └──────────────┘
```

---

# 📂 Project Structure

```text
Stock-RAG/
│
├── app.py
├── rag_chat.py
├── prediction.py
├── requirements.txt
├── .env
├── .gitignore
│
├── chroma_db/
│   ├── ...
│   └── ...
│
├── models/
│   └── Huber/
│       ├── AACG.joblib
│       ├── AACG_meta.joblib
│       ├── AAL.joblib
│       ├── AAL_meta.joblib
│       ├── ...
│       └── ...
│
├── metrics_all_models.csv
│
└── walk_forward.csv
```

---

# 📁 Main Files

## `app.py`

The main Streamlit application.

It provides the complete user interface with three main tabs:

### 💬 Chat / RAG

Allows the user to ask natural-language questions about the stock dataset.

The application:

1. Detects a ticker from the question.
2. Searches the ChromaDB collection.
3. Retrieves relevant documents.
4. Sends the retrieved context to the LLM.
5. Generates the final answer.
6. Displays retrieved sources.

---

### 🤖 Prediction

Provides ML-based predictions for a selected ticker.

The application:

1. Loads the ticker's saved model.
2. Loads its saved metadata.
3. Loads the required historical data.
4. Applies the same feature engineering used during training.
5. Uses the saved scaler.
6. Creates the latest 20-day feature window.
7. Predicts the next 5-day return.
8. Calculates the implied future price.
9. Displays the predicted direction.

Example:

```text
Ticker: AACG
Model: Huber

Latest Date: 2025-07-01
Current Price: $0.80

Predicted 5-Day Return: -0.39%
Implied Future Price: $0.80

Direction: DOWN
Rows Used: 20
```

> **Important:** The prediction is an ML model output and should not be interpreted as financial advice.

---

### 📊 ML Performance

The application provides an interactive analysis of the trained models.

The dashboard includes:

* Median RMSE
* Median MAE
* Median Directional Accuracy
* Model comparison
* RMSE bar chart
* MAE bar chart
* Directional Accuracy chart
* Prediction standard deviation ratio
* Selected ticker performance
* Walk-forward evaluation
* Raw metrics table

Median values are used for high-level comparisons because the dataset contains some extreme ticker/model outliers that can heavily distort the arithmetic mean.

---

# 🧠 RAG Pipeline

## Data

The original stock dataset contains historical data for thousands of US stock tickers.

The raw dataset contains fields such as:

```text
Date
Open
High
Low
Close
Volume
```

along with additional technical-indicator columns.

The RAG system focuses on the relevant stock information and converts it into text chunks.

---

## Text Conversion

Stock rows are converted into text representations such as:

```text
Ticker: AAPL
Date: 2025-07-01
Open: ...
High: ...
Low: ...
Close: ...
Volume: ...
```

This allows the data to be embedded and searched semantically.

---

# 🔎 ChromaDB

The project uses **ChromaDB** as the vector database.

Configuration:

```text
Collection:
stocks_knowledge_base

Embedding Model:
all-MiniLM-L6-v2

Chunk Size:
5 rows

Number of Results:
5
```

The ChromaDB database was created in the Kaggle environment and then downloaded for local use.

Therefore:

> The local Streamlit application does **not** need to rebuild the vector database or re-process the original 17GB dataset.

The application directly loads:

```text
chroma_db/
```

---

# 🤖 Embedding Model

The RAG system uses:

```text
all-MiniLM-L6-v2
```

from Sentence Transformers.

The embedding model converts both:

* stored stock-data chunks
* user queries

into vector representations.

The system then searches for semantically relevant information.

---

# 🧠 LLM

The RAG response generation uses Groq with:

```text
Model:
openai/gpt-oss-20b
```

The LLM receives the retrieved information as context.

The RAG prompt is designed to restrict the model to the retrieved information and avoid hallucinating unsupported stock-data facts.

---

# 🔍 Ticker Detection

The application automatically attempts to detect stock tickers from user questions.

For example:

```text
What happened to AAPL in the last few days?
```

The system detects:

```text
AAPL
```

and uses ticker-filtered retrieval when possible.

This helps prevent unrelated tickers from being returned by the vector search.

---

# 🤖 Machine Learning Pipeline

The prediction system was trained using historical stock data.

## Features

The final model uses two main features:

```text
return_1d
log_volume
```

### Daily Return

```text
return_1d = adj_close.pct_change()
```

### Log Volume

```text
log_volume = log1p(volume)
```

---

# ⏱️ Prediction Target

The prediction target is the **5-day forward return**.

Conceptually:

```text
5-Day Return =
(Price[t+5] - Price[t]) / Price[t]
```

The model therefore predicts the expected percentage return over the following five trading days.

---

# 🪟 Input Window

The model uses:

```text
Window Size = 20 trading days
Horizon = 5 trading days
```

Therefore, every prediction is based on the latest:

```text
20 days × 2 features
```

For classical ML models, the window is flattened:

```text
20 × 2 = 40 features
```

The LSTM model uses the original sequential shape:

```text
20 × 2
```

---

# 📊 Train / Validation / Test Split

The data is divided chronologically:

```text
70% → Training
15% → Validation
15% → Test
```

No random shuffling is used for the chronological stock forecasting split.

This preserves the temporal structure of the financial data.

---

# ⚖️ Feature Scaling

A custom `TrainOnlyScaler` is used.

The scaler is fitted only on the training data.

```text
Training Data
      │
      ▼
Fit Scaler
      │
      ├───────────────┐
      ▼               ▼
Validation          Test
Transform           Transform
```

This prevents future information from being used during scaler fitting.

---

# 🧪 Models

The training pipeline evaluates multiple models:

```text
Ridge
Huber
Random Forest
HistGradientBoosting
XGBoost
LSTM
```

The classical models operate on flattened 20-day windows.

The LSTM operates directly on the sequential input.

---

# 🏆 Model Selection

For each ticker, the models are evaluated using several metrics.

The primary selection criterion is:

```text
Lowest RMSE
```

The best model for each ticker is then saved.

In the current project, the available selected models are stored under:

```text
models/
```

For example:

```text
models/
└── Huber/
    ├── AACG.joblib
    ├── AACG_meta.joblib
    ├── AAL.joblib
    ├── AAL_meta.joblib
    └── ...
```

---

# 📏 Evaluation Metrics

The project evaluates models using:

## RMSE

Root Mean Squared Error.

Lower is better.

```text
RMSE ↓
```

---

## MAE

Mean Absolute Error.

Lower is better.

```text
MAE ↓
```

---

## Directional Accuracy

Measures how often the model correctly predicts whether the future return is positive or negative.

Higher is better.

```text
Directional Accuracy ↑
```

---

## Prediction Standard Deviation Ratio

Measures the variability of model predictions relative to the actual target variability.

It is used as an additional diagnostic metric.

---

# 🚶 Walk-Forward Evaluation

The project also includes walk-forward evaluation.

Instead of evaluating only one static split, walk-forward evaluation examines model performance across sequential periods.

Conceptually:

```text
Train → Test
Train --------→ Test
Train ----------------→ Test
Train ------------------------→ Test
```

This provides a more realistic view of how a model behaves over different time periods.

The results are stored in:

```text
walk_forward.csv
```

and visualized inside the Streamlit dashboard when the required columns are available.

---

# 📈 ML Performance Dashboard

The performance dashboard contains several visualizations.

### Model RMSE

```text
RMSE by Model
```

Lower values indicate better performance.

### Model MAE

```text
MAE by Model
```

Lower values indicate better performance.

### Directional Accuracy

```text
Directional Accuracy by Model
```

Higher values indicate better direction prediction.

### Prediction Standard Deviation Ratio

Used to inspect whether model predictions are unusually volatile or conservative.

---

# 📊 Why Median Metrics?

The dataset contains thousands of ticker/model combinations.

A small number of extreme observations can produce extremely large RMSE and MAE values.

For example, some ticker/model combinations contain very large errors compared with the majority of observations.

Using the arithmetic mean in such a situation can make the overall model comparison misleading.

Therefore, the Streamlit dashboard uses:

```python
median()
```

for the main aggregate performance summaries.

This gives a more robust representation of the typical model performance across tickers.

The raw metrics remain available in the dashboard for detailed inspection.

---

# 🔐 Environment Variables

The application uses a Groq API key stored in a `.env` file.

Create:

```text
.env
```

with:

```env
GROQ_API_KEY=YOUR_GROQ_API_KEY
```

Do **not** upload the `.env` file to GitHub.

---

# 🚫 `.gitignore`

The repository should contain a `.gitignore` similar to:

```gitignore
# Environment
.env

# Python
__pycache__/
*.py[cod]
*.pyo

# Virtual environments
venv/
.venv/
env/

# Streamlit
.streamlit/

# Jupyter
.ipynb_checkpoints/

# OS
.DS_Store
Thumbs.db

# Temporary files
*.tmp
*.log
```

---

# ⚙️ Installation

## 1. Clone the repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL
cd Stock-RAG
```

---

## 2. Create a virtual environment

Windows:

```bash
python -m venv venv
```

Activate it:

```bash
venv\Scripts\activate
```

---

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

# 📦 Requirements

The main dependencies are:

```text
streamlit
chromadb
sentence-transformers
groq
python-dotenv
pandas
numpy
joblib
scikit-learn
```

Install them using:

```bash
pip install -r requirements.txt
```

---

# 🔑 Configure Groq API

Create a `.env` file:

```env
GROQ_API_KEY=YOUR_GROQ_API_KEY
```

Make sure the file is listed in `.gitignore`.

---

# ▶️ Run the Application

From the project directory:

```bash
streamlit run app.py
```

The application will open in your browser.

---

# 🖥️ Application Interface

The application contains three main tabs:

```text
💬 Chat / RAG
🤖 Prediction
📊 ML Performance
```

---

## 💬 Example RAG Questions

### Historical price question

```text
What was the average closing price of AAPL over the last 5 trading days available in the dataset?
```

### Stock-data question

```text
What was the trading volume of AAPL on the latest available date?
```

### Another ticker

```text
What was the recent closing price of MSFT?
```

The system retrieves relevant information from ChromaDB before generating the response.

---

# 🤖 Example Prediction

Enter a ticker such as:

```text
AACG
```

The application loads:

```text
AACG.joblib
AACG_meta.joblib
```

and generates the prediction using the latest available 20-day window.

The output includes:

```text
Current Price
Predicted 5-Day Return
Implied Future Price
Direction
Model Used
Latest Date
Rows Used
```

---

# 🧩 Important Design Decision

The project separates **information retrieval** from **numerical prediction**.

The LLM is responsible for:

```text
Understanding questions
+
Using retrieved context
+
Generating natural-language answers
```

The ML model is responsible for:

```text
Numerical stock-return prediction
```

The LLM does **not** generate the numerical ML prediction.

This separation makes the architecture easier to understand and reduces the risk of treating an LLM-generated statement as a trained forecasting output.

---

# 🔄 End-to-End Workflow

```text
                    USER
                      │
                      ▼
              ┌───────────────┐
              │ Streamlit UI  │
              └───────┬───────┘
                      │
          ┌───────────┼───────────┐
          │           │           │
          ▼           ▼           ▼
        Chat      Prediction    Metrics
          │           │           │
          ▼           ▼           ▼
      ChromaDB     Joblib       CSV Files
          │         Models          │
          ▼           │             ▼
    Retrieved Data    │       Performance
          │           │         Analysis
          ▼           ▼
        Groq       ML Model
          │           │
          ▼           ▼
       Answer      Prediction
```

---

# 📌 Data Requirements

The local application does **not** require the original full stock dataset to rebuild the RAG system.

The pre-built vector database is already included/provided through:

```text
chroma_db/
```

The prediction module, however, requires access to the historical stock data used to create the latest 20-day feature window.

Therefore, depending on the final local deployment configuration, the required stock CSV files must be available to `prediction.py`.

The project intentionally avoids rebuilding the entire RAG database locally.

---

# 🧠 Model Artifacts

Each selected model is accompanied by metadata.

Example:

```text
AACG.joblib
AACG_meta.joblib
```

The metadata stores information such as:

```text
Feature scaler
Target mean
Target standard deviation
Number of features
Ticker
Number of rows
Training boundary
Validation boundary
Window size
Prediction horizon
```

This allows the application to reproduce the preprocessing required by the trained model during inference.

---

# ⚠️ Limitations

This project has several important limitations.

### 1. Stock prediction uncertainty

Financial markets are highly noisy and affected by many external factors.

A model trained only on historical market data cannot reliably predict all future market movements.

---

### 2. Extreme data points

Some tickers contain abnormal observations that can produce extremely large error metrics.

The dashboard therefore uses robust median-based summaries while preserving the raw metrics.

---

### 3. Historical information

The RAG system answers questions based on the information available in the indexed dataset.

It is not a real-time stock market data feed.

---

### 4. No financial advice

The prediction output is an experimental machine learning result and should not be considered financial advice or a recommendation to buy or sell securities.

---

# 🔮 Future Improvements

Possible future improvements include:

* Real-time market data integration.
* More advanced financial features.
* Technical indicator selection.
* Transformer-based time-series models.
* Better anomaly detection.
* More robust outlier handling.
* Hyperparameter optimization.
* Per-ticker model analysis.
* Confidence intervals for predictions.
* Better RAG reranking.
* Hybrid semantic + metadata retrieval.
* Multi-ticker comparison.
* Portfolio-level analysis.
* Real-time dashboard updates.
* Cloud deployment.

---

# 🛠️ Technologies Used

| Technology            | Purpose                   |
| --------------------- | ------------------------- |
| Python                | Core development          |
| Streamlit             | Web application           |
| ChromaDB              | Vector database           |
| Sentence Transformers | Text embeddings           |
| `all-MiniLM-L6-v2`    | Embedding model           |
| Groq                  | LLM inference             |
| `gpt-oss-20b`         | RAG response generation   |
| Scikit-learn          | Classical ML models       |
| XGBoost               | Gradient boosting         |
| PyTorch               | LSTM model                |
| Pandas                | Data processing           |
| NumPy                 | Numerical computation     |
| Joblib                | Model serialization       |
| dotenv                | Environment configuration |

---

# 📁 Recommended GitHub Repository

A clean final repository can look like:

```text
Stock-RAG/
│
├── app.py
├── rag_chat.py
├── prediction.py
│
├── requirements.txt
├── README.md
├── .gitignore
│
├── chroma_db/
│
├── models/
│   └── Huber/
│       ├── AACG.joblib
│       ├── AACG_meta.joblib
│       ├── AAL.joblib
│       ├── AAL_meta.joblib
│       └── ...
│
├── metrics_all_models.csv
└── walk_forward.csv
```

---

# 🔒 Before Pushing to GitHub

Before running:

```bash
git add .
```

make sure that you **do not include**:

```text
.env
GROQ API keys
passwords
private credentials
large temporary files
Python virtual environments
```

Check your repository with:

```bash
git status
```

---

# 📌 Project Highlights

### RAG

* Extractive/grounded retrieval pipeline
* ChromaDB vector database
* Sentence Transformer embeddings
* Ticker-aware retrieval
* Groq-powered natural-language responses
* Source/context display

### Machine Learning

* Multiple ML models evaluated per ticker
* 20-day historical window
* 5-day return forecasting
* Train-only feature scaling
* Chronological train/validation/test split
* Per-ticker model selection
* Saved model artifacts

### Evaluation

* RMSE
* MAE
* Directional Accuracy
* Prediction standard deviation ratio
* Walk-forward evaluation
* Interactive visualizations
* Robust median-based aggregate metrics

### Application

* Streamlit interface
* RAG chatbot
* Stock prediction dashboard
* ML performance dashboard
* Local inference
* No local RAG database rebuilding required

---

# 👨‍💻 Goal

The main goal of this project is to demonstrate how **Retrieval-Augmented Generation and traditional machine learning can be integrated into a single practical AI application**.

Instead of relying on an LLM alone, the system separates responsibilities:

```text
RAG → Historical Data Understanding
ML  → Numerical Prediction
LLM → Natural Language Interaction
```

This architecture demonstrates a practical approach to building an AI assistant that combines **data retrieval, machine learning, and generative AI** in one application.
