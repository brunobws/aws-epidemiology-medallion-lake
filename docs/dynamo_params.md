# DynamoDB Parameters

This document describes the DynamoDB tables used to configure the pipeline. The goal is to keep all operational parameters — bucket paths, schemas, notification rules, quality checks — out of the code and in a centralized, easy-to-change place.

[Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html) is a fully managed NoSQL key-value and document database. It was chosen here because it is serverless, has single-digit millisecond reads at any scale, and integrates natively with Lambda and Glue without any connection management overhead.

The source JSON files that represent each DynamoDB item live in [aws/dynamo_params/](../aws/dynamo_params/).

![DynamoDB tables overview](img/01_architecture/03_dynamodb_tables.png)

---

## ingestion_params

Source file: [aws/dynamo_params/ingestion_params.json](../aws/dynamo_params/ingestion_params.json)

Read by the `bronze_to_silver` Glue job to know where to find the raw file, how to parse it, what schema to apply, and where to write the result. This is what makes the job a generic processing engine — none of this is hardcoded in the script.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `trgt_tbl` | string | yes | Primary key. Identifies the target table (e.g. `breweries_tb_breweries`). |
| `ext` | string | yes | File extension of the source file (`json`, `csv`, `txt`). |
| `s3_bronze_path` | string | yes | Full S3 URI of the Bronze source folder. |
| `s3_silver_path` | string | yes | Full S3 URI of the Silver destination folder. |
| `silver_table` | string | yes | Athena/Glue catalog table to write to (e.g. `silver.tb_breweries`). |
| `table_schema` | string | yes | JSON-serialized dict mapping column names to their target Spark types. Used for schema casting during ingestion. |
| `partition_column` | string | yes | Column used to partition the Parquet output on S3. |
| `mode` | string | no | Write mode: `overwrite` (default) or `append`. |
| `has_bdq` | boolean | no | Whether to run data quality checks (from `quality_params`) after ingestion. |
| `source` | string | no | Source domain name, used for path and catalog resolution. |
| `header` | boolean | no | For CSV/TXT files: whether the first row is a header. Default `true`. |
| `encoding` | string | no | File encoding. Default `UTF-8`. |
| `sep` | string | no | Column separator for CSV/TXT files. |
| `explode_column` | string | no | If the JSON contains a nested array, this column is exploded into rows before processing. |
| `skip_header` | integer | no | Number of header rows to skip (for fixed-width or legacy files). |
| `skip_footer` | integer | no | Number of footer rows to skip. |
| `filter_column` | string | no | Column name to apply a filter on after reading. |
| `filter_value` | string | no | Value to match when filtering rows. |
| `positional_column` | list | no | Column layout definition for fixed-width (positional) files. |
| `iceberg_query` | string | no | Optional custom Iceberg SQL to run instead of the default overwrite write. |
| `lit_values` | object | no | Literal or row-derived columns to add to the DataFrame before writing (e.g. a date parsed from a raw file header row). |
| `options_params` | object | no | Additional Spark reader options passed directly to the DataFrame reader. |

---

## notification_params

Source file: [aws/dynamo_params/notification_params.json](../aws/dynamo_params/notification_params.json)

Controls which email addresses receive alerts and under what conditions. Used by Lambda, and both Glue jobs via the `utils` module. Each item is keyed by `trgt_tbl`, so notifications can be configured independently per table.

For more on how email sending works, see [modules.md](modules.md).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `trgt_tbl` | string | yes | Primary key. Identifies the target table this configuration applies to. |
| `critical` | boolean | yes | If `true`, a failure on this table escalates to the internal critical email list in addition to the configured addresses. |
| `email_on_failure` | string | yes | Email address to notify when the job fails. |
| `email_on_warning` | string | no | Email address to notify on warnings (e.g. data quality issues that did not halt the job). |
| `email_on_success` | string | no | Email address to notify on successful completion. |
| `email_on_ingestion` | boolean | no | If `true`, triggers the ingestion-layer notification logic (used for Lambda/Bronze jobs). |
| `email_on_refined` | boolean | no | If `true`, triggers the refined-layer notification logic (used for Silver→Gold jobs). |

---

## refined_params

Source file: [aws/dynamo_params/refined_params.json](../aws/dynamo_params/refined_params.json)

Read by the `silver_to_gold` Glue job. Tells it which layer to target and how to write the result. The SQL file path is resolved automatically from `target_table`, so the only decisions here are about write behavior.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `target_table` | string | yes | Primary key. Identifies the target table (e.g. `breweries_tb_ft_breweries_agg`). |
| `layer` | string | yes | Destination layer. Currently only `gold` is supported. Determines the Athena catalog and S3 warehouse path. |
| `mode` | string | yes | Write mode: `overwrite` replaces the existing table data on each run. `merge` delegates the write logic entirely to the SQL file (e.g. a `MERGE INTO` statement). |

---

## quality_params

Source file: [aws/dynamo_params/quality_params.json](../aws/dynamo_params/quality_params.json)

Defines the data quality rules applied by the `Quality` module after ingestion in the `bronze_to_silver` job. Rules are evaluated using [Great Expectations](https://docs.greatexpectations.io/docs/), a data validation library that runs checks on Spark or Pandas DataFrames and produces structured pass/fail results.

When `has_bdq: true` is set in `ingestion_params`, the job reads this table and runs each check against the ingested DataFrame before writing to Silver.

| Parameter | Type | Description |
|---|---|---|
| `trgt_tbl` | string | Primary key. Matches the `trgt_tbl` in `ingestion_params`. |
| `quality_params.not_null` | object | Defines columns that must not contain null values. `column` accepts a comma-separated list. |
| `quality_params.unique_vals` | object | Defines columns that must contain only unique (non-duplicate) values. `column` accepts a comma-separated list. |
| `quality_params.df_count_between` | object | Validates that the total row count of the DataFrame falls within `min` and `max` bounds. Catches empty or unexpectedly large loads. |
| `quality_params.value_match_regex` | object | Validates column values against regex patterns. `column` and `regex` are comma-separated lists in the same order — each column is paired with its corresponding pattern. |
| `quality_params.values_between` | object | Validates that numeric column values fall within a specified range. `column`, `min`, and `max` are comma-separated and aligned by index. Also reports the mean of unexpected values. |
| `quality_params.value_length_between` | object | Validates that string column values have lengths within a defined range. `column`, `min`, and `max` are comma-separated and aligned by index. |
| `quality_params.date_mask_equal` | object | Validates that date column values match their expected strftime format. `column` and `date_mask` are comma-separated and aligned by index. |
| `quality_params.values_to_be_in_set` | object | Validates that column values belong to an allowed set. Accepts `column` (comma-separated), `type` (`int`, `float`, `str`), and `set_values` (list of lists). |
| `quality_params.values_not_be_in_set` | object | Validates that column values do not contain any forbidden values. Same structure as `values_to_be_in_set`. |
| `quality_params.compare_count_df_with_db` | object | Compares the row count of the Athena DataFrame against a relational database query. Requires `ssm_name`, `technology`, and `db_query`. |
| `quality_params.compare_df_with_df_db` | object | Performs a full row-level comparison between the Athena DataFrame and a database result set. Requires `ssm_name`, `technology`, `db_query`, and `schema`. |
| `quality_params.general_metrics_athena_db` | object | Compares aggregate metrics (row counts, sums, min/max dates) between multiple Athena tables and their database counterparts. Requires `athena_tables`, `db_tables`, `ssm_name`, and `technology`. |
| `quality_params.stop_job` | boolean | **Controls job halt behavior on quality failure.** See note below. |

> [!IMPORTANT]
> **`stop_job`** determines what happens when a quality check fails:
> - `false` (default) — the job continues, data is written to Silver, and the execution is logged with status `warning`
> - `true` — the job halts immediately, an error is raised, and **no data is written**

For full details on how the Quality module works, how results are stored in Athena, and what happens on failure, see [modules.md](modules.md).