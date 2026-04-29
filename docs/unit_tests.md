# Unit Tests

The pipeline includes a robust suite of **44 Pytest Unit Tests**. The objective of these tests is to guarantee that logic functions as expected before the code ever touches the AWS environment.

## Execution and Mocking

All tests are designed to be run **offline**. We utilize libraries like `moto` to mock AWS services (S3, DynamoDB, Athena).

This means a developer can validate transformations, data quality rules, and API pagination without needing internet access or incurring AWS costs.

### How to run

```bash
pytest tests/ -v
```

## CI/CD Integration

**Does GitHub Actions blocking a deployment count as a test?**
Yes! The integration of these tests in our **GitHub Actions** pipeline acts as a strict gateway. If a developer pushes a broken code or a logic regression, the `pytest` step will fail. The CI/CD pipeline immediately halts, meaning the broken code will **never** be deployed to AWS Lambda or Glue.

**Is it possible to implement Pytest for each pipeline (esteira)?**
Absolutely. We can create separate test suites for each layer (Bronze API Ingestion, Silver Transformations, Gold Aggregations).
- *Bronze:* Mock the external API response and assert the Lambda constructs the correct JSON output.
- *Silver:* Mock the input DataFrame and assert the PySpark job correctly standardizes columns and applies partitions.
- *Gold:* Validate the SQL aggregations on a localized DuckDB or Spark session.

This isolation is a highly recommended practice to scale data engineering teams safely.
