# Uber Data Engineering Pipeline on GCP (Mage + BigQuery)

### Introduction

This repository presents an **end-to-end Uber data analytics project** built on **Google Cloud Platform(GCP)**. The pipeline leverages **Mage** for ETL orchestration, **BigQuery** for scalable data warehousing and SQL-based analytics, and **Looker Studio** to deliver interactive, dashboard-driven insights from Uber trip data.

### Architecture diagram
- Raw Uber trip data is stored in **Google Cloud Storage(GCS)**, then loaded into **Mage** running on a **Compute Engine VM** through a Python data loader block.
- Mage transforms the raw dataset into **dimension + fact tables (star schema)** and exports the analytics-ready tables into **BigQuery**, which is then used for **SQL analysis** and **Looker Studio dashboards**.
![Architecture diagram](https://github.com/datahub-by-urmi/uber-data-engineering-etl-project/blob/main/uber_architecture_diagram.jpg)
### Tech Stack
- **Google Cloud Storage (GCS)**: Used as the source storage for the raw Uber trip CSV dataset ingested by the pipeline.
- **Google Compute Engine (VM)**: Hosted the Mage environment and executed the end-to-end ETL pipeline on a virtual machine.
- **Mage (Mage.ai)**: Used as the ETL orchestration tool to build a modular, block-based pipeline (data loader → transformer → exporter) with reproducible runs.
- **Python**: Implemented data ingestion, transformation, and export logic inside Mage blocks using Pandas for data processing.
- **BigQuery (SQL + Data Warehouse)**: Stored analytics-ready fact and dimension tables; used SQL to validate data and answer business questions.
- **Looker Studio**: Built dashboards and KPIs by directly connecting to BigQuery analytics tables for data exploration and reporting.
  
### Pipeline Steps
**1) Data Ingestion (GCS → Mage):** Uploaded the Uber trip dataset to **Google Cloud Storage**.
Used a **Mage Data Loader block** (uber_loader_block.py) to load data from GCS into a Pandas DataFrame and validate row counts and schema.

**2) Data Transformation:** Implemented transformations in a **Mage Transformer block** (uber_transformation_block.py) to clean and standardize the raw data.
Parsed pickup and dropoff timestamps, extracted **time features**, and standardized categorical fields such as rate codes and payment types.

**3) Data Modeling (Star Schema):** Modeled the data into an analytics-friendly **star schema** with a central **fact table** representing individual Uber trips and containing **trip-level metrics** such as **fare amount, tips, tolls, and total amount**. Supporting **dimension tables** were created for **datetime, payment type, rate code, passenger count**, and **pickup/dropoff locations**, enabling efficient slicing and aggregation for analytics and dashboarding.

**4) Loading to BigQuery:** Exported transformed fact and dimension tables to **BigQuery** using a **Mage Data Exporter block** (uber_bigquery_block.py).
Verified successful table creation, schema correctness, and successful data load completion.

**5) Analytics & Reporting:** Wrote **BigQuery SQL queries** to analyze **fare trends**, trip patterns by time, and payment behavior.
Connected BigQuery tables to **Looker Studio** to build dashboards and KPIs for data exploration.

![Looker Studio Dashboard](https://github.com/datahub-by-urmi/uber-data-engineering-etl-project/blob/main/looker_dashboard.png)

### What I Learned / Challenges
- Debugged **BigQuery export issues** related to Python dependencies and schema/data-type alignment.
- Faced performance bottlenecks when transforming a **large dataset (~100K rows)** in Mage due to memory-intensive dictionary conversions. Isolated the issue by validating transformations   on a reduced dataset, then optimized the pipeline by retaining **Pandas DataFrames** instead of dictionaries, enabling reliable processing of the full dataset.
- Designed a **star schema in BigQuery** to support efficient analytics and dashboarding.

