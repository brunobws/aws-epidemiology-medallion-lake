# Architecture

This document outlines the data engineering pipeline for the EpiMind project, covering data extraction, processing, and consumption.

## Architecture Diagram

![EpiMind Architecture Diagram](img/bws-data-epi-architutecture.jpg)

---

## Orchestration — AWS Step Functions

Instead of managing an Airflow cluster, this project uses **AWS Step Functions** for serverless orchestration. It provides visual workflows, error handling, and automated retries out-of-the-box.

The state machine coordinates the workflow: extracting data via Lambda, passing execution parameters, and triggering Glue jobs for transformation.

![Step Functions Graph](img/stepfunctions_graph.png)

---

## Ingestion — AWS Lambda

[AWS Lambda](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html) is used for data ingestion. It hits external APIs to fetch the latest epidemiological data and stores it in the Bronze S3 bucket. The functions are fully configuration-driven, meaning the API endpoints, pagination logic, and target paths are retrieved dynamically from DynamoDB at runtime.

---

## Data Lake Layers (Medallion Architecture)

### Bronze Layer — Raw Data

Raw data is landed in S3 in its original format (e.g., JSON or CSV). This acts as a historical source of truth, enabling reprocessing of the data without re-fetching from the source API.

### Silver Layer — AWS Glue (PySpark)

AWS Glue runs serverless PySpark jobs to clean, standardize, and format the data.
The Silver layer is stored in **Parquet** format. This columnar structure highly optimizes Athena queries and significantly reduces storage and processing costs.
Transformations include handling nulls, standardizing column names, and partitioning data by location or date.

![Athena Silver Query](img/athena_silver_select_query.png)

### Gold Layer — Aggregation and Analytics

In the Gold layer, clean data from Silver is transformed into business-ready metrics using SQL executed via Athena. These pre-aggregated tables are directly queried by the Streamlit application.

![Athena Gold Query](img/athena_gold_select_query_1.png)

---

## Observability and Configuration

### Configuration via DynamoDB

To ensure the codebase remains DRY (Don't Repeat Yourself), all pipelines pull their configuration from **DynamoDB**.

![DynamoDB Tables](img/dynamo_tables.png)

- **Execution Parameters**: Table definitions, paths, and metadata.
- **Notifications**: Control over who receives alerts on job success or failure.

### Centralized Logging

All Lambdas and Glue jobs use a centralized logging standard. Execution metadata (start time, end time, status, bytes processed) is written directly to an Athena-queryable table (`execution_logs`).

![Execution Logs Table](img/athena_logs_select_query.png)

---

## Next Steps

While Data Quality checks are not currently fully implemented, the foundation is laid out. Future iterations will introduce automated validation to block bad data from reaching the Silver and Gold layers.
