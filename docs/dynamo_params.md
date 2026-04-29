# DynamoDB Parameters

EpiMind's ETL pipelines are entirely **Configuration-Driven**. Instead of hardcoding paths, API endpoints, or database schemas into Lambda and Glue scripts, the execution environment fetches parameters at runtime from AWS DynamoDB.

This approach means onboarding a new epidemiological dataset (e.g., Zika instead of Dengue) requires zero code changes—just a new entry in DynamoDB.

---

## Configuration Tables

### 1. Ingestion Parameters (`ingestion_params`)
Used by AWS Lambda to fetch data from APIs.
- **API Endpoints:** URL to hit.
- **Pagination Strategy:** How to iterate over pages.
- **Target S3 Path:** Where to land the Bronze JSON/CSV file.

### 2. Transformation Parameters (`refined_params`)
Used by AWS Glue to process Bronze data into Silver Parquet files.
- **Schema Definitions:** Expected column types and casting rules.
- **Partitioning:** How to partition the data (e.g., by State and Date).
- **Target Path:** The Silver S3 bucket prefix.

### 3. Quality Parameters (`quality_params`)
Used by the `Quality` module.
- **Expectations:** JSON array defining rules like "column X cannot be null" or "column Y must be an integer between 0 and 100".

### 4. Notification Parameters (`notification_params`)
Controls the AWS SES alerting system.
- **Subscribers:** Emails to notify on success, warning, or failure.
- **Thresholds:** Logic to define when a warning becomes an error.

![DynamoDB Tables](img/dynamo_tables.png)
