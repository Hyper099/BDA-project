# Smart Financial Inclusion Analytics System (Alternative Credit Scoring)

End-to-end Big Data + ML project to identify underbanked users, predict creditworthiness using alternative data, generate risk scores, expose APIs, and visualize insights.

## 1) Project Structure

```text
.
├── api/
│   ├── __init__.py
│   ├── main.py
│   └── schemas.py
├── dashboard/
│   └── app.py
├── data/
│   ├── __init__.py
│   ├── generate_data.py
│   ├── pyspark_pipeline.py
│   ├── raw/
│   │   ├── financial_inclusion_data.csv         # generated
│   │   └── financial_inclusion_sample.csv       # sample provided
│   ├── processed/
│   │   └── financial_inclusion_features.csv     # generated
│   └── warehouse/
│       └── s3_simulated/
│           └── processed/
│               └── financial_inclusion_features.csv
├── logs/
├── models/
│   ├── __init__.py
│   ├── predict.py
│   ├── train_model.py
│   └── artifacts/
│       ├── best_model.joblib
│       ├── preprocessor.joblib
│       └── metrics.json
├── storage/
│   └── financial_inclusion.db
├── utils/
│   ├── __init__.py
│   ├── config.py
│   ├── db.py
│   ├── logger.py
│   └── risk.py
├── docker-compose.spark.yml
├── Dockerfile.api
├── Dockerfile.dashboard
├── requirements.txt
└── run_pipeline.py
```

## 2) Features and Target

### Input Features
- Transaction data:
  - monthly_upi_transactions
  - avg_transaction_amount
  - bill_payment_timeliness
  - mobile_recharge_frequency
- Demographic data:
  - age
  - location_type (urban/rural)
  - income
  - occupation
- Behavioral data:
  - savings_ratio
  - spending_ratio
  - payment_delay_days

### Target
- loan_repaid (0 or 1)

## 3) Pipeline

Data Ingestion -> PySpark Cleaning -> Feature Engineering -> Model Training -> Model Saving -> FastAPI Deployment -> Streamlit Dashboard

## 4) ML Models and Metrics

Models trained:
- Logistic Regression
- Random Forest
- XGBoost

Evaluation:
- Accuracy
- F1-score

Best model is selected by highest F1 and saved with `joblib`.

## 5) Risk Score Logic

- `risk_score = probability_of_repayment * 100`
- `0-40`: High Risk
- `40-70`: Medium Risk
- `70-100`: Low Risk

## 6) Setup and Run (Local)

### Step 1: Create environment and install dependencies

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

### Step 2: Generate synthetic data

```bash
python data/generate_data.py --rows 5000 --seed 42
```

### Step 3: Run PySpark ETL locally

```bash
python data/pyspark_pipeline.py --input data/raw/financial_inclusion_data.csv --output data/processed/financial_inclusion_features.csv
```

### Step 4: Train models and save best model

```bash
python models/train_model.py
```

### Step 5: Start FastAPI

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### Step 6: Start Streamlit dashboard (new terminal)

```bash
streamlit run dashboard/app.py
```

## 7) Run Spark with Docker (as requested)

Make sure `financial_inclusion_data.csv` exists first.

```bash
docker compose -f docker-compose.spark.yml up --abort-on-container-exit
```

This runs the PySpark ETL inside a Spark container and writes output to:
- `data/processed/financial_inclusion_features.csv`
- simulated S3 path: `data/warehouse/s3_simulated/processed/financial_inclusion_features.csv`

## 8) API Endpoints

### Health
```http
GET /health
```

### Predict
```http
POST /predict
Content-Type: application/json

{
  "monthly_upi_transactions": 12,
  "avg_transaction_amount": 900,
  "bill_payment_timeliness": 0.82,
  "mobile_recharge_frequency": 7,
  "age": 30,
  "location_type": "urban",
  "income": 38000,
  "occupation": "small_business",
  "savings_ratio": 0.24,
  "spending_ratio": 0.68,
  "payment_delay_days": 4
}
```

Response includes:
- `risk_score`
- `risk_category`
- `probability_of_repayment`

## 9) Testing

### Quick API test with curl

```bash
curl -X POST "http://127.0.0.1:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{"monthly_upi_transactions":12,"avg_transaction_amount":900,"bill_payment_timeliness":0.82,"mobile_recharge_frequency":7,"age":30,"location_type":"urban","income":38000,"occupation":"small_business","savings_ratio":0.24,"spending_ratio":0.68,"payment_delay_days":4}'
```

### Dashboard checks
- Single user prediction should show score and category.
- CSV upload should return downloadable scored output.
- Charts should display risk distribution, income vs risk, and urban/rural comparison.

## 10) Optional Docker Run for API and Dashboard

```bash
docker build -f Dockerfile.api -t fi-api .
docker run -p 8000:8000 fi-api

# new terminal
docker build -f Dockerfile.dashboard -t fi-dashboard .
docker run -p 8501:8501 fi-dashboard
```

## 11) One-command Orchestration

```bash
python run_pipeline.py
```

This executes generate -> ETL -> train.
