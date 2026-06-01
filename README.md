
# 🛒 Real-Time Retail Analytics & Demand Prediction Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-black.svg)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.3-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-0.14-blue.svg)](https://delta.io)
[![MLflow](https://img.shields.io/badge/MLflow-2.11.0-red.svg)](https://mlflow.org)
[![Grafana](https://img.shields.io/badge/Grafana-latest-orange.svg)](https://grafana.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.1-green.svg)](https://airflow.apache.org)

> **Z5008 Big Data Lab — IIT Madras Zanzibar — Even Semester 2026**
>
> **Author:** Vineet Joshi | **Roll Number:** ZDA25M007
>
> **Supervisor:** Dr. Innocent Nyalala

---

## 📋 Project Overview

This project builds a **production-grade, end-to-end Big Data system** that:

- Ingests real-time retail transaction data via **Apache Kafka**
- Stores data in a **Lakehouse** architecture using **Delta Lake on MinIO**
- Processes data at scale with **Apache Spark**
- Trains ML models tracked with **MLflow**
- Serves predictions via a **Flask REST API**
- Monitors everything with **Grafana + Prometheus**
- Orchestrates the pipeline with **Apache Airflow**
- Provides a **Natural Language Query Interface** (Bonus +5%) using Groq Llama 3

**Dataset:** Online Retail II (UCI ML Repository) — 15.3 million records, GBP 10.88M revenue, 40 countries

---

## 🏗️ 8-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  8-Layer Lakehouse Architecture              │
├──────┬──────────────────────┬────────────────────────────────┤
│Layer │ Component            │ Tool                           │
├──────┼──────────────────────┼────────────────────────────────┤
│  1   │ Data Ingestion       │ Apache Kafka 3.7 (KRaft)       │
│  2   │ Object Storage       │ MinIO + Delta Lake              │
│  3   │ Batch Processing     │ Apache Spark 3.5.3             │
│  4   │ Orchestration        │ Apache Airflow 2.9.1           │
│  5   │ ML Training          │ Scikit-learn + MLflow          │
│  6   │ Experiment Tracking  │ MLflow v2.11.0                 │
│  7   │ Model Serving        │ Flask REST API                 │
│  8   │ Monitoring           │ Grafana + Prometheus           │
│BONUS │ NL Query Interface   │ Groq Llama 3 + DuckDB          │
└──────┴──────────────────────┴────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop installed and running
- Git
- At least 16GB RAM recommended
- At least 20GB free disk space

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/z5008-retail-analytics.git
cd z5008-retail-analytics
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env if needed (defaults work out of the box)
```

### 3. Download Dataset

Download the **Online Retail II** dataset from:
https://archive.ics.uci.edu/dataset/502/online+retail+ii

Place the file at:
```
data/online_retail_II.csv
```

### 4. Start All Services

```bash
docker-compose up -d
```

Wait 60 seconds for all services to initialise, then verify:

```bash
docker-compose ps
```

All containers should show **Healthy** or **Running**.

### 5. Open JupyterLab

Open your browser and go to:
```
http://localhost:8888
```
Token: `bigdata`

### 6. Run Notebooks in Order

Run each notebook sequentially:

| Notebook | Name | Description |
|----------|------|-------------|
| NB1 | Setup & EDA | Load, clean, explore data |
| NB2 | Data Scaling | Scale to 15M records |
| NB3 | Kafka Streaming | Stream data via Kafka |
| NB4 | Delta Lake Storage | Store in Lakehouse |
| NB5 | Spark Processing | Distributed batch processing |
| NB6 | ML Training | Train demand prediction models |
| NB7 | Model Evaluation | Evaluate and save best model |
| NB8 | SQL Analytics | 13 business analytics queries |
| NB9 | Dashboard | Real-time interactive dashboard |
| NB10 | REST API | Deploy model as REST API |
| NB11 | Grafana | Monitoring dashboard |
| NB12 | Airflow DAG | Pipeline orchestration |
| NB13 | NL Query | Natural language query interface |

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| JupyterLab | http://localhost:8888 | Token: `bigdata` |
| Spark UI | http://localhost:8083 | None |
| MinIO Console | http://localhost:9001 | admin / bigdata123 |
| MLflow UI | http://localhost:5000 | None |
| Airflow UI | http://localhost:8090 | admin / admin |
| Grafana | http://localhost:3002 | admin / admin |
| Prometheus | http://localhost:9090 | None |
| REST API | http://localhost:3001 | None |
| Dashboard | http://localhost:8050 | None |
| NL Query UI | http://localhost:8051 | None |

---

## 📁 Repository Structure

```
z5008-retail-analytics/
│
├── 📓 notebooks/              # Jupyter notebooks (NB1-NB13)
│   ├── NB1_setup_eda.ipynb
│   ├── NB2_data_scaling.ipynb
│   ├── NB3_kafka_streaming.ipynb
│   ├── NB4_delta_lake.ipynb
│   ├── NB5_spark_processing.ipynb
│   ├── NB6_ml_training.ipynb
│   ├── NB7_model_evaluation.ipynb
│   ├── NB8_sql_analytics.ipynb
│   ├── NB9_dashboard.ipynb
│   ├── NB10_rest_api.ipynb
│   ├── NB11_grafana.ipynb
│   ├── NB12_airflow_dag.ipynb
│   └── NB13_nl_query.ipynb
│
├── ⚡ spark_jobs/             # Production Spark Python files
│   ├── retail_batch_job.py
│   └── retail_streaming_job.py
│
├── 🔄 dags/                   # Airflow DAG definitions
│   └── retail_analytics_dag.py
│
├── 🤖 ml/                     # ML training scripts
│   ├── train.py
│   └── mlflow_experiment.py
│
├── 🌐 api/                    # REST API service
│   ├── retail_api.py
│   └── Dockerfile
│
├── 📊 dashboards/             # Grafana dashboard exports
│   └── grafana_dashboard.json
│
├── 📦 data/                   # Sample data and generators
│   └── generate_data.py
│
├── 🐳 docker-compose.yml      # All Docker services
├── 📄 .env.example            # Environment variables template
└── 📖 README.md               # This file
```

---

## 🐳 Docker Services

All services are defined in `docker-compose.yml`:

```yaml
Services:
  kafka          - Apache Kafka 3.7 (KRaft mode, no Zookeeper)
  spark-master   - Spark Master node
  spark-worker-1 - Spark Worker 1
  spark-worker-2 - Spark Worker 2
  minio          - MinIO object storage (S3-compatible)
  minio-init     - MinIO bucket initialisation
  mlflow         - MLflow tracking server
  airflow        - Airflow webserver
  airflow-scheduler - Airflow scheduler
  airflow-init   - Airflow initialisation
  grafana        - Grafana dashboards
  prometheus     - Prometheus metrics
  postgres       - PostgreSQL metadata store
  jupyter        - JupyterLab environment
```

### Start/Stop Commands

```bash
# Start all services
docker-compose up -d

# Stop all services (data preserved)
docker-compose stop

# Stop and remove containers (data preserved in volumes)
docker-compose down

# View logs for a service
docker-compose logs -f jupyter

# Restart a specific service
docker-compose restart grafana
```

---

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Dataset (scaled) | 15.3 million records |
| Total Revenue | GBP 10.88 million |
| Countries Served | 40 |
| Unique Customers | 4,649 |
| Unique Products | 4,045 |
| Kafka Throughput | 2,000 records/second |
| Best ML Model | Linear Regression |
| R² Score | **0.9289** (92.89%) |
| RMSE | 87.97 units |
| API Load Test | 100x requests — 100% success |
| Airflow Tasks | 8 tasks — all green in 13 seconds |
| Grafana Panels | 11 live panels |
| SQL Queries | 13 (10 DuckDB + 3 Spark SQL) |

---

## 🤖 ML Model

**Target:** TotalQuantity (demand prediction)

**Features:**
- Year, Month, WeekOfYear, DayOfWeek
- AvgUnitPrice, NumInvoices, UniqueCustomers, TotalRevenue
- StockCode_idx (encoded), Country_idx (encoded)

**Models trained:**
| Model | R² Score | RMSE |
|-------|----------|------|
| Linear Regression | **0.9289** ✅ | 87.97 |
| Random Forest | 0.8912 | 102.45 |
| Gradient Boosting | 0.8756 | 109.23 |

**API Prediction Example:**
```bash
curl -X POST http://localhost:3001/predict \
  -H "Content-Type: application/json" \
  -d '{
    "Year": 2010,
    "Month": 11,
    "WeekOfYear": 46,
    "DayOfWeek": 3,
    "AvgUnitPrice": 3.75,
    "NumInvoices": 5,
    "UniqueCustomers": 4,
    "TotalRevenue": 185.0,
    "StockCode_idx": 42,
    "Country_idx": 0
  }'
```

Response:
```json
{
  "predicted_quantity": 125.73,
  "model": "Linear Regression",
  "status": "ok"
}
```

---

## 🔄 Airflow Pipeline

The `retail_analytics_pipeline` DAG runs every 6 hours:

```
T1: check_data_ingestion
        ↓
T2: run_data_quality_check
        ↓
T3: run_spark_processing
        ↓
T4: run_feature_engineering
        ↓
T5: run_ml_training
        ↓
T6: run_model_validation
        ↓
T7: check_api_health
        ↓
T8: generate_pipeline_report
```

---

## 🌟 Bonus Feature: Natural Language Query Interface (+5%)

Ask questions about retail data in plain English:

- *"What is the total revenue for each country?"*
- *"Which are the top 5 best selling products?"*
- *"What is the busiest hour of the day for sales?"*

The system uses **Groq Llama 3** AI to convert English to SQL, runs it on Delta Lake data via DuckDB, with SQL injection prevention.

Access at: **http://localhost:8051**

---

## 🔒 Environment Variables

See `.env.example` for all required variables. Key variables:

```bash
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=bigdata123
JUPYTER_TOKEN=bigdata
AIRFLOW_UID=50000
GROQ_API_KEY=your-groq-api-key-here
```

---

## 🤝 Academic Integrity Declaration

This project uses AI coding tools (GitHub Copilot, Claude AI) as declared here. All AI-assisted code has been understood, tested, and is fully explainable during Q&A.

---

## 📜 License

This project is submitted as coursework for Z5008 Big Data Lab, IIT Madras Zanzibar, Even Semester 2026.

---

<div align="center">
<strong>IIT Madras Zanzibar | Z5008 Big Data Lab | Even Semester 2026</strong><br>
<strong>Vineet Joshi | ZDA25M007 | Dr. Innocent Nyalala</strong>
</div>
