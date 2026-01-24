# Uber Data Engineering Pipeline on GCP (Mage + BigQuery)

### Introduction

This repository presents an end-to-end Uber data analytics project built on Google Cloud Platform. The pipeline leverages Mage for ETL orchestration, BigQuery for scalable data warehousing and SQL-based analytics, and Looker Studio to deliver interactive, dashboard-driven insights from Uber trip data.

### Architecture diagram
- Raw Uber trip data is stored in Google Cloud Storage, then loaded into Mage running on a Compute Engine VM through a Python data loader block.
- Mage transforms the raw dataset into dimension + fact tables (star schema) and exports the analytics-ready tables into BigQuery, which is then used for SQL analysis and Looker Studio dashboards.
![Architecture diagram](https://github.com/datahub-by-urmi/uber-data-engineering-etl-project/blob/main/uber_architecture_diagram.jpg)
### Tech Stack
- Python: Data ingestion and transformation logic inside Mage blocks (data loader, transformer, exporter).
- Mage (Mage.ai): Pipeline orchestration for ETL (block-based workflow, reproducible runs, modular code).
- Google Cloud Storage (GCS): Source storage for the raw CSV dataset used by the pipeline.
- Google Compute Engine (VM): Hosted Mage instance and executed the pipeline on a VM environment.
- BigQuery (SQL + Data Warehouse): Loaded fact + dimension tables for analytics; wrote SQL queries to solve business questions and validate outputs.
- Looker Studio: Built dashboards directly connected to BigQuery analytics tables.
### Pipeline Steps
1) Data Ingestion (GCS → Mage): Uploaded the Uber trip dataset to Google Cloud Storage.
Used a Mage Data Loader block (uber_loader_block.py) to load data from GCS into a Pandas DataFrame and validate row counts and schema.

2) Data Transformation: Implemented transformations in a Mage Transformer block (uber_transformation_block.py) to clean and standardize the raw data.
Parsed pickup and dropoff timestamps, extracted time features, and standardized categorical fields such as rate codes and payment types.

3) Data Modeling (Star Schema):Modeled the data into an analytics-friendly star schema with:
A fact table containing trip-level metrics (fare, tips, distance, vendor, payment type, etc.).
Multiple dimension tables (datetime, passenger count, trip distance, rate code, payment type, and location where applicable).

4) Loading to BigQuery: Exported transformed fact and dimension tables to BigQuery using a Mage Data Exporter block (uber_bigquery_block.py).
Verified table creation, schema correctness, and successful data loads.

5) Analytics & Reporting: Wrote BigQuery SQL queries to analyze fare trends, trip patterns by time, and payment behavior.
Connected BigQuery tables to Looker Studio to build dashboards and KPIs for data exploration.

### What I Learned / Challenges

- Debugged BigQuery export issues related to Python dependencies and schema/data-type alignment.

- Faced performance bottlenecks when transforming a large dataset (~100K rows) in Mage due to converting outputs into dictionaries, which caused pipeline freezes.

- Isolated the issue by testing transformations on a smaller dataset and learned that dictionary conversions are memory-intensive for large volumes.

- Improved pipeline reliability by keeping transformations in Pandas DataFrames and modularizing the workflow into loader → transformer → exporter blocks.

- Designed a star schema in BigQuery to support efficient analytics and dashboarding.

