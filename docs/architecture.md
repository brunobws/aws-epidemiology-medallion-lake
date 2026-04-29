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

**Target bucket:** `bws-dl-bronze-sae-prd`

**S3 path pattern:**
```text
<source>/<table_name>/ingestion_date=YYYY-MM-DD/data_HHMMSS.json
```

After the upload completes, the function returns `filename` and `ingestion_date` to Step Functions, which then passes these arguments into the downstream Glue jobs.

**Retry logic:** On HTTP errors or timeouts, each request retries up to 3 times with exponential backoff before raising an exception.

**Notifications** are configured via the `notification_params` table in DynamoDB. This table controls which email addresses receive alerts on failure, warning, or success.

> [!NOTE]
> For DynamoDB parameter details, see [dynamo_params.md](dynamo_params.md). For shared module documentation, see [modules.md](modules.md).

**Scripts:** `aws/scripts/` 

---

## Data Lake Layers (Medallion Architecture)

### Bronze Layer — Raw Data

Raw data is landed in S3 in its original format (e.g., JSON or CSV). This acts as a historical source of truth, enabling reprocessing of the data without re-fetching from the source API.

### Silver Layer — AWS Glue (PySpark)

The Glue job `bronze_to_silver` reads the JSON file from the Bronze layer and converts it into [Parquet](https://parquet.apache.org/) format. Parquet is a columnar storage format that reduces query costs and execution time on Athena significantly compared to JSON — particularly when filtering on specific columns.

Data is written to the Silver bucket partitioned by **date** (and location where applicable), so queries only scan the relevant partitions instead of the full dataset.

**What this job does:**
- Reads the exact file passed by Step Functions (`--file_name` and `--dt_ref`)
- Applies schema casting, null handling, and column standardization
- Runs data quality checks configured in DynamoDB (`quality_params`) using the Quality module
- Writes the clean result as Parquet, properly partitioned.

**Designed as a generic processing engine:** 
This job reads all its configuration from DynamoDB (`ingestion_params`) — source paths, schema definitions, quality rules. Pass it different parameters and it processes a completely different dataset without any code changes. This makes it reusable across multiple ingestion pipelines.

> [!NOTE]
> For DynamoDB parameter details, see [dynamo_params.md](dynamo_params.md). For shared module documentation, see [modules.md](modules.md).

**Script:** `aws/scripts/glue_scripts/bronze_to_silver.py`

![Athena Silver Query](img/athena_silver_select_query.png)

### Gold Layer — Aggregation and Analytics

The Glue job `silver_to_gold` reads clean Silver data and produces pre-aggregated tables in the Gold layer. Instead of hardcoding the transformation logic inside the job, the SQL query is stored as a `.sql` file in S3 and loaded at runtime. This keeps business logic versioned and separated from execution code.

The job runs that SQL using Athena to create business-ready metrics. These pre-aggregated tables are stored in Parquet format, ensuring fast and cost-efficient reads. 

**What the SQL does:**
Groups epidemiological cases by city, state, and disease type, calculating thresholds, incidence rates, and population metrics. This powers the main analytics view and the AI module in the Streamlit dashboard.

**Also a generic engine:**
Like the Bronze to Silver job, configuration is pulled from DynamoDB (`refined_params`). Point it at a different SQL file and target table, and it processes an entirely different aggregation without touching the code.

![Athena Gold Query](img/athena_gold_select_query_1.png)

---

## Dashboard — Streamlit & AI

After the Gold layer is ready, the data is immediately available for the Streamlit application. 

The dashboard provides visual filters, maps, and an **AI Analyst** capable of answering natural language questions by dynamically translating them into Athena SQL queries based on the database schema.

> [!NOTE]
> For detailed instructions on the UI, see [Dashboard Guide](dashboard.md).
> To understand how the Artificial Intelligence connects to Athena, see [AI Guide](ai_guide.md).

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

## Security

Access control is handled through [AWS IAM](https://docs.aws.amazon.com/iam/latest/userguide/introduction.html) roles and policies, following the principle of least privilege. Each service (Lambda, Glue, Step Functions, EC2) has its own IAM role with only the permissions it actually needs.

The EC2 instance sits inside a [VPC](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html) with a [security group](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-groups.html) that controls which ports and IPs can reach it. Port 80 (Nginx Proxy) is public to serve the application securely. A fixed [Elastic IP](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/elastic-ip-addresses-eip.html) ensures the instance address stays stable for DNS resolution via Registro.br. 

All S3 data is encrypted at rest using [AWS SSE](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryption.html). DynamoDB tables are encrypted using [AWS KMS](https://docs.aws.amazon.com/kms/). All infrastructure and security controls are provisioned programmatically via **Terraform**.
