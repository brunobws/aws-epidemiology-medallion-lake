# Modules

The data pipeline utilizes modularized Python code to keep functions DRY (Don't Repeat Yourself) and to standardize critical operations across different AWS resources (Lambda and Glue).

All shared modules are packaged and deployed to AWS automatically via GitHub Actions.

## 1. Logs Module (`Logs`)

The Logs module ensures that all execution runs—whether from a Lambda function triggering an API, or a Glue Job performing PySpark transformations—are recorded centrally in Athena.

- **Standardization:** Every run captures `job_name`, `status` (success, warning, error), `layer` (bronze, silver, gold), and `records_processed`.
- **Performance Profiling:** It calculates exact time spent in each step and stores it in a JSON `info` column.
- **Traceability:** Facilitates the "Observability" tab in the dashboard, enabling developers to pinpoint exactly where and when a failure occurred.

## 2. Quality Module (`Quality`)

The Quality module wraps **Great Expectations** to ensure data integrity as it moves from Bronze to Silver.

- **Automated Validation:** It validates null checks, data types, uniqueness, and custom regex rules.
- **Dynamic Rules:** The validation rules for a given table are read dynamically from DynamoDB, meaning no hardcoded rules in the PySpark script.
- **Logging:** Results are saved into a `quality_logs` Athena table.

## 3. AwsManager Module (`AwsManager`)

The AWS Manager simplifies interactions with the boto3 SDK.

- **Secrets & Parameters:** Retrieves configurations from DynamoDB seamlessly.
- **S3 Utilities:** Provides helper functions to read, write, and list objects within S3 buckets.
- **Alerting:** Wraps AWS SES (Simple Email Service) to dispatch HTML-formatted alert emails directly to stakeholders if a pipeline fails.
