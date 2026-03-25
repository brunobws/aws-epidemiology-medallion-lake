####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Data quality validation class built on top of Great Expectations.
#   Supports Spark and Pandas DataFrames, executes configurable
#   quality checks (null validation, uniqueness, regex matching,
#   range checks, date format masks, and cross-system row/aggregate
#   comparisons between AWS Athena and relational databases).
#   Builds an HTML report per execution and dispatches email
#   notifications via AWS SES on success or failure.
#   Optionally halts the job pipeline if a critical check fails.
#
# Usage type:
#   Instantiate the Quality class within a Glue or Lambda job,
#   pass the DataFrame and quality_params configuration, then
#   call run_quality_checks() to execute all validations.
#
####################################################################

########## Imports ##########
import great_expectations as ge
from support import split_target_table, get_date_and_time
from utils import AwsManager
from logs import Logs
from decimal import Decimal

try:
    from pyspark_utils import Pyspark
    from pyspark.sql.types import DecimalType, StructType, StructField, StringType  # noqa: F401
    import pyspark.sql.functions as F
except ImportError:
    print("Functions that use Spark are not available.")

try:
    import pandas as pd
except ImportError:
    pass
########## End Imports ##########


########## Constants ##########
SENDER_EMAIL = "brun0ws@outlook.com"

# Unicode invisible/whitespace characters to strip from string columns
INVISIBLE_CHARS_REGEX = r"[\u200B\u00A0\u2000-\u200F\u2028\u2029\u3000\uFEFF\u200C\u200D]"

# Numeric zero representations used in cross-system comparison
ZERO_REPRESENTATIONS = {"0", "0.00", "0.0"}
########## End Constants ##########


########## Quality Class ##########
class Quality(AwsManager):
    """
    Data quality validation class built on top of Great Expectations.

    Supports Spark and Pandas DataFrames. Executes configurable quality
    checks, builds an HTML report, and sends email notifications on
    success or failure. Optionally raises an exception to halt the job.
    """

    def __init__(
        self,
        job_name: str,
        quality_params: dict,
        target_table: str,
        df,
        stop_job: bool = False,
        destination_on_failure: list = None,
        destination_on_success: list = None,
        spark=None,
        logger: Logs = None,
        env: str = "prd",
    ):
        """
        Initializes the Quality instance and configures logging, reporting,
        and the Great Expectations DataFrame wrapper.

        Args:
            job_name (str): Name of the Glue/Lambda job being validated.
            quality_params (dict): Dictionary mapping check names to their parameters.
            target_table (str): Fully qualified target table name (e.g. 'domain.table').
            df: Spark or Pandas DataFrame to be validated.
            stop_job (bool): Whether to raise an exception if any check fails.
            destination_on_failure (list): Email addresses to notify on failure.
            destination_on_success (list): Email addresses to notify on success.
            spark: Active SparkSession instance, if applicable.
            logger (Logs): External logger instance for the parent job.
            env (str): Deployment environment ('prd', 'dev', etc.).
        """
        super().__init__(
            job_name=job_name,
            destination=destination_on_failure,
            target_table=target_table,
        )

        self.spark = spark
        self.target_table = target_table
        self.job_name = job_name
        self.logger = logger
        self.df = df
        self.stop_job = stop_job
        self.destination_on_failure = destination_on_failure
        self.destination_on_success = destination_on_success
        self.quality_params = quality_params
        self.failed_expectation = False
        self.quality_test_results = []

        # Parse table name and source domain from the fully qualified table name
        self.table, self.source = split_target_table(self.target_table)

        # Initialize the dedicated quality logger
        self.quality_logger = Logs(
            job_name=job_name,
            target_table=self.target_table,
            env=env,
            layer="quality",
            table="quality_logs",
        )
        self.quality_logger.add_info(email_on_failure=destination_on_failure)
        self.quality_logger.add_info(email_on_success=destination_on_success)
        self.quality_logger.add_info(stop_job=stop_job)

        # Email subject template — filled in after checks complete
        self.subject = f"{{status}} - {self.table} - {job_name}"

        # HTML report template — rows appended as checks run
        self.report = f"""<html>
                        <body>
                        <h3 style="color: {{color}};">
                        <strong>[Data quality] - [{self.table}] - [{job_name}]</strong>
                        </h3>
                        <p><em>Execution details:</em></p>
                        <ul>
                        <li><strong>Table:</strong> {self.table}</li>
                        <li><strong>Domain:</strong> {self.source}</li>
                        <li><strong>Executed at:</strong> {get_date_and_time()}</li>
                        </ul>
                        <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
                        <tr>
                            <th>Column</th>
                            <th>Test Applied</th>
                            <th>Status</th>
                        </tr>
                        """

        # Wrap the DataFrame in a Great Expectations dataset
        if spark:
            if df:
                self.df_ge = self._convert_df_from_spark()
            self.pyspark_instance = Pyspark(
                job_name=job_name,
                spark=spark,
                destination=destination_on_failure,
                trgt_tbl=self.target_table,
            )
        elif isinstance(df, pd.DataFrame):
            self.df_ge = self._convert_df_from_pandas()

    ########## Report Helpers ##########

    def _generate_report_row(self, column: str = None, expectation: str = None) -> None:
        """
        Appends a result row to the HTML report based on the last expectation result.

        Args:
            column (str): Column name being tested, or None for dataset-level checks.
            expectation (str): Human-readable description of the test applied.

        Returns:
            None
        """
        # Determine the subject of the test: column name or full dataset
        subject_label = column if column else "Dataset"
        is_success = self.response["success"]

        print(f"Column: {subject_label} | Status: {is_success}")

        # Record structured result for the quality log
        log_record = {
            "column_tested": subject_label,
            "test_applied": expectation,
            "status": "success" if is_success else "failure",
        }
        self.quality_test_results.append(log_record)

        # Build the HTML table row with colour-coded status
        if is_success:
            status_cell = '<td style="color: green;">Success</td>'
        else:
            status_cell = '<td style="color: red;">Failure</td>'
            self.failed_expectation = True

        row_html = f"""<tr>
                        <td>{subject_label}</td>
                        <td>{expectation}</td>
                        {status_cell}
                    </tr>"""

        self.report += row_html

    ########## Expectation Methods ##########

    def not_null(self, params: dict) -> None:
        """
        Validates that specified columns contain no null values.

        Args:
            params (dict): Must contain 'column' — a comma-separated list of column names.

        Returns:
            None
        """
        column_names = params["column"].split(",")
        for col_name in column_names:
            self.response = self.df_ge.expect_column_values_to_not_be_null(col_name)
            self._generate_report_row(col_name, "not null values")

    def unique_vals(self, params: dict) -> None:
        """
        Validates that specified columns contain only unique values.

        Args:
            params (dict): Must contain 'column' — a comma-separated list of column names.

        Returns:
            None
        """
        column_names = params["column"].split(",")
        for col_name in column_names:
            self.response = self.df_ge.expect_column_values_to_be_unique(col_name)
            self._generate_report_row(col_name, "unique values")

    def date_mask_equal(self, params: dict) -> None:
        """
        Validates that date columns match their expected strftime format masks.

        Args:
            params (dict): Must contain 'column' (comma-separated) and
                           'date_mask' (comma-separated, aligned with columns).

        Returns:
            None
        """
        column_names = params["column"].split(",")
        date_masks = params["date_mask"].split(",")

        for col_name, mask in zip(column_names, date_masks):
            self.response = self.df_ge.expect_column_values_to_match_strftime_format(
                col_name, strftime_format=mask
            )
            self._generate_report_row(col_name, f"date mask matches {mask}")

    def value_length_between(self, params: dict) -> None:
        """
        Validates that string column values have lengths within a defined range.

        Args:
            params (dict): Must contain 'column', 'min', and 'max'
                           as comma-separated strings (aligned by index).

        Returns:
            None
        """
        column_names = params["column"].split(",")
        min_values = params["min"].split(",")
        max_values = params["max"].split(",")

        for col_name, min_val, max_val in zip(column_names, min_values, max_values):
            self.response = self.df_ge.expect_column_value_lengths_to_be_between(
                column=col_name,
                min_value=min_val,
                max_value=max_val,
            )
            self._generate_report_row(
                col_name, f"value length between {min_val} and {max_val}"
            )

    def df_count_between(self, params: dict) -> None:
        """
        Validates that the total row count of the DataFrame is within a defined range.

        Args:
            params (dict): Must contain 'min' and 'max' as integers (or castable strings).

        Returns:
            None
        """
        min_value = int(params["min"])
        max_value = int(params["max"])

        self.response = self.df_ge.expect_table_row_count_to_be_between(
            min_value=min_value,
            max_value=max_value,
        )
        self._generate_report_row(
            expectation=f"DataFrame row count between {min_value} and {max_value}"
        )

    def value_match_regex(self, params: dict) -> None:
        """
        Validates that column values match the expected regular expression patterns.

        Args:
            params (dict): Must contain 'column' and 'regex' as comma-separated strings
                           (aligned by index).

        Returns:
            None
        """
        column_names = params["column"].split(",")
        regexes = params["regex"].split(",")

        for col_name, regex in zip(column_names, regexes):
            self.response = self.df_ge.expect_column_values_to_match_regex(
                column=col_name, regex=regex
            )
            self._generate_report_row(col_name, f"values match regex {regex}")

    def values_between(self, params: dict) -> None:
        """
        Validates that numeric column values fall within a specified range.
        Also reports the mean of unexpected values when violations occur.

        Args:
            params (dict): Must contain 'column', 'min', and 'max'
                           as comma-separated strings (aligned by index).

        Returns:
            None
        """
        column_names = params["column"].split(",")
        min_values = params["min"].split(",")
        max_values = params["max"].split(",")

        for col_name, min_raw, max_raw in zip(column_names, min_values, max_values):
            minimum = float(min_raw)
            maximum = float(max_raw)

            self.response = self.df_ge.expect_column_values_to_be_between(
                column=col_name,
                min_value=minimum,
                max_value=maximum,
            )

            # Extract unexpected values to compute their mean for reporting
            unexpected_values = self.response["result"].get("partial_unexpected_list", None)
            mean_unexpected = "N/A"

            if unexpected_values:
                numeric_vals = [v for v in unexpected_values if isinstance(v, (int, float))]
                if numeric_vals:
                    mean_unexpected = str(sum(numeric_vals) / len(numeric_vals))

            print(f"Column '{col_name}' validated. Unexpected values: {unexpected_values}")

            self._generate_report_row(
                col_name,
                f"values between {minimum} and {maximum} | mean of unexpected: {mean_unexpected}",
            )

    def compare_count_df_with_db(self, params: dict) -> None:
        """
        Compares the row count of the instance DataFrame against a relational database query.

        Args:
            params (dict): Must contain:
                - 'ssm_name' (str): SSM secret key for database credentials.
                - 'technology' (str): Database technology ('oracle', 'sqlserver', etc.).
                - 'db_query' (str): SQL query to execute against the database.

        Returns:
            None
        """
        ssm_secret = super().ssm.get_ssm_secret(key=params.get("ssm_name"))

        # Fetch the database DataFrame only once; reuse across subsequent calls
        if not hasattr(self, "df_db"):
            self.df_db = self.pyspark_instance.run_query_jdbc(
                db_host=ssm_secret["host"],
                db_port=ssm_secret["port"],
                db_name=ssm_secret["dbname"],
                db_user=ssm_secret["user"],
                db_psswd=ssm_secret["password"],
                query=params.get("db_query"),
                technology=params.get("technology"),
            )

        count_athena = self.df.count()
        count_db = self.df_db.count()

        self.response = {"success": count_athena == count_db}

        self._generate_report_row(
            expectation=(
                f"Row count comparison — Athena: {count_athena} "
                f"vs DB ({ssm_secret['dbname']}): {count_db}"
            )
        )

    def compare_df_with_df_db(self, params: dict) -> None:
        """
        Performs a full row-level comparison between the instance DataFrame (Athena)
        and a relational database result set.

        Strips invisible Unicode characters from string columns before comparison,
        sorts both DataFrames by column name, and casts both to a unified schema.

        Args:
            params (dict): Must contain:
                - 'ssm_name' (str): SSM secret key for database credentials.
                - 'technology' (str): Database technology.
                - 'db_query' (str): SQL query for the database.
                - 'schema' (dict): Shared schema used to cast both DataFrames.

        Returns:
            None
        """
        ssm_secret = super().ssm.get_ssm_secret(key=params.get("ssm_name"))

        # Fetch the database DataFrame only once
        if not hasattr(self, "df_db"):
            self.df_db = self.pyspark_instance.run_query_jdbc(
                db_host=ssm_secret["host"],
                db_port=ssm_secret["port"],
                db_name=ssm_secret["dbname"],
                db_user=ssm_secret["user"],
                db_psswd=ssm_secret["password"],
                query=params.get("db_query"),
                technology=params.get("technology"),
            )

        # Strip leading/trailing whitespace and invisible Unicode characters from
        # all string columns in both DataFrames to avoid false differences
        for column in self.df_db.columns:
            if isinstance(self.df_db.schema[column].dataType, StringType):
                self.df = self.df.withColumn(
                    column, F.trim(F.rtrim(self.df[column]))
                )
                self.df = self.df.withColumn(
                    column,
                    F.trim(F.regexp_replace(self.df[column], INVISIBLE_CHARS_REGEX, "")),
                )
                self.df_db = self.df_db.withColumn(
                    column, F.trim(F.rtrim(self.df_db[column]))
                )
                self.df_db = self.df_db.withColumn(
                    column,
                    F.trim(F.regexp_replace(self.df_db[column], INVISIBLE_CHARS_REGEX, "")),
                )

        # Normalise both DataFrames: fill nulls, sort columns, cast to unified schema
        self.df_db = self.df_db.fillna("")
        self.df_db = self.df_db.select(sorted(self.df_db.columns))
        self.df_db = self.pyspark_instance.cast_df(df=self.df_db, schema=params["schema"])

        self.df = self.df.fillna("")
        self.df = self.df.select(sorted(self.df.columns))
        self.df = self.pyspark_instance.cast_df(df=self.df, schema=params["schema"])

        # Identify rows present in Athena but absent in the database
        diff_df = self.df.exceptAll(self.df_db)
        count_diff = diff_df.count()

        self.response = {"success": count_diff == 0}

        if count_diff != 0:
            print(f"Divergent rows found ({count_diff}):")
            diff_df.show()

        self._generate_report_row(
            expectation=(
                f"Row comparison — Athena vs DB ({ssm_secret['dbname']}): "
                f"{count_diff} divergent row(s)"
            )
        )

    def general_metrics_athena_db(self, params: dict) -> None:
        """
        Compares aggregate metrics (counts, sums, min/max dates) between multiple
        Athena tables and their database counterparts, table by table.

        Args:
            params (dict): Must contain:
                - 'athena_tables' (str): Comma-separated Athena table names.
                - 'db_tables' (str): Comma-separated database table names (aligned).
                - 'ssm_name' (str): SSM secret key for database credentials.
                - 'technology' (str): Database technology.

        Returns:
            None
        """
        ssm_secret = super().ssm.get_ssm_secret(key=params.get("ssm_name"))

        athena_tables = params["athena_tables"].split(",")
        db_tables = params["db_tables"].split(",")

        # Reset the report for this metric-focused validation
        self.report = ""

        for i, (athena_table, db_table) in enumerate(zip(athena_tables, db_tables)):
            table_short = athena_table.split(".")[1]
            database_short = athena_table.split(".")[0]

            # Build a per-table HTML header block
            self.report += f"""
                <li><strong>Table:</strong> {table_short}</li>
                <li><strong>Database:</strong> {database_short}</li>
                <li><strong>Executed at:</strong> {get_date_and_time()}</li>
                <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
                <tr>
                    <th>Column</th>
                    <th>Result</th>
                    <th>Status</th>
                </tr>
            """

            # Retrieve Athena schema to determine which aggregate functions apply
            describe_result = self.spark.sql(f"DESCRIBE {athena_table}")
            schema_athena = {
                row["col_name"]: row["data_type"]
                for row in describe_result.collect()
                if row["col_name"] != "# Partition Information"
                and "# col" not in row["col_name"]
            }

            # Start building the SELECT queries with a row count
            query_athena = "SELECT COUNT(*) AS row_count"
            query_db = "SELECT COUNT(*) AS row_count"

            # Append type-specific aggregate expressions for each column
            for column, data_type in schema_athena.items():
                if data_type == "date" and "ptt_col" not in column:
                    query_athena += (
                        f", MIN({column}) AS min_{column}"
                        f", MAX({column}) AS max_{column}"
                    )
                    query_db += (
                        f", MIN(CAST({column} AS DATE)) AS min_{column}"
                        f", MAX(CAST({column} AS DATE)) AS max_{column}"
                    )
                elif data_type in ("float", "decimal(10,2)"):
                    query_athena += f", ROUND(SUM({column}), 2) AS sum_{column}"
                    query_db += (
                        f", CAST(ROUND(SUM(CAST(COALESCE({column}, 0) AS BIGINT)), 2)"
                        f" AS DECIMAL(38, 2)) AS sum_{column}"
                    )
                elif data_type == "int":
                    query_athena += f", SUM({column}) AS sum_{column}"
                    query_db += (
                        f", SUM(CAST(COALESCE({column}, 0) AS BIGINT)) AS sum_{column}"
                    )
                elif data_type == "timestamp":
                    fmt = "yyyy-MM-dd HH:mm:ss"
                    query_athena += (
                        f", MIN(DATE_FORMAT({column}, '{fmt}')) AS min_{column}"
                        f", MAX(DATE_FORMAT({column}, '{fmt}')) AS max_{column}"
                    )
                    query_db += (
                        f", MIN(FORMAT({column}, '{fmt}')) AS min_{column}"
                        f", MAX(FORMAT({column}, '{fmt}')) AS max_{column}"
                    )

            query_athena += f" FROM {athena_table}"
            query_db += f" FROM {db_table}"

            print(f"DB query: {query_db}")

            # Execute queries on both systems
            df_athena = self.spark.sql(query_athena)
            df_database = self.pyspark_instance.run_query_jdbc(
                db_host=ssm_secret["host"],
                db_port=ssm_secret["port"],
                db_name=ssm_secret["dbname"],
                db_user=ssm_secret["user"],
                db_psswd=ssm_secret["password"],
                query=query_db,
                technology=params.get("technology"),
            )

            print(f"DB result: {df_database.show()}")

            # Collect single-row results for column-by-column comparison
            columns = df_athena.columns
            row_athena = df_athena.collect()[0]
            row_db = df_database.collect()[0]

            # Free memory immediately after collecting
            del df_database, df_athena

            for column in columns:
                val_athena = row_athena[column]
                val_db = row_db[column]

                str_athena = str(val_athena)
                str_db = str(val_db)

                # Treat None and numeric zero as equivalent across systems
                values_match = (
                    str_athena == str_db
                    or (val_athena is None and str_db in ZERO_REPRESENTATIONS)
                    or (val_db is None and str_athena in ZERO_REPRESENTATIONS)
                )

                self.response = {"success": values_match}
                self._generate_report_row(
                    column=column,
                    expectation=(
                        f"DB ({ssm_secret['dbname']}) {column}={row_db[column]}"
                        f" | Athena {column}={row_athena[column]}"
                    ),
                )

            self.report += "</table><br/>"

    def values_not_be_in_set(self, params: dict) -> None:
        """
        Validates that specified columns do not contain any of the forbidden values.

        Args:
            params (dict): Must contain:
                - 'column' (str): Comma-separated column names.
                - 'type' (str): Comma-separated value types ('int', 'float', 'str').
                - 'set_values' (list[list]): Matrix of forbidden values per column.

        Returns:
            None
        """
        column_names = params["column"].split(",")
        set_values = params["set_values"]
        types = params["type"].split(",")

        for i, col_name in enumerate(column_names):
            raw_values = set_values[i]
            target_type = types[i]

            # Cast values to the declared Python type
            if target_type == "float":
                cast_values = [float(v) for v in raw_values]
            elif target_type == "int":
                cast_values = [int(v) for v in raw_values]
            else:
                cast_values = raw_values

            self.response = self.df_ge.expect_column_values_to_not_be_in_set(
                col_name, cast_values
            )
            self._generate_report_row(
                col_name, f"column {col_name} must not contain values {cast_values}"
            )

    def values_to_be_in_set(self, params: dict) -> None:
        """
        Validates that specified columns only contain values from an allowed set.

        Values from DynamoDB arrive as Decimal objects and are converted to the
        declared Python type before the expectation is evaluated.

        Args:
            params (dict): Must contain:
                - 'column' (str): Comma-separated column names.
                - 'type' (str): Comma-separated value types ('int', 'float', 'str').
                - 'set_values' (list[list]): Matrix of allowed values per column
                  (may contain Decimal instances from DynamoDB).

        Returns:
            None
        """
        column_names = params["column"].split(",")
        set_values_raw = params["set_values"]  # e.g. [[Decimal('0')], [Decimal('1')]]
        types = params["type"].split(",")

        for i, col_name in enumerate(column_names):
            decimal_list = set_values_raw[i]
            target_type = types[i]

            # Convert Decimal values from DynamoDB to native Python numeric types
            if target_type == "int":
                converted_values = [int(d) for d in decimal_list]
            elif target_type == "float":
                converted_values = [float(d) for d in decimal_list]
            else:
                converted_values = decimal_list

            self.response = self.df_ge.expect_column_values_to_be_in_set(
                col_name, converted_values
            )
            self._generate_report_row(
                col_name,
                f"column {col_name} must only contain values {converted_values}",
            )

    ########## DataFrame Conversion ##########

    def _convert_df_from_pandas(self):
        """
        Wraps a Pandas DataFrame in a Great Expectations dataset.

        Returns:
            ge.dataset.PandasDataset: GE-wrapped Pandas DataFrame.
        """
        return ge.from_pandas(self.df)

    def _convert_df_from_spark(self):
        """
        Wraps a Spark DataFrame in a Great Expectations SparkDFDataset.

        Returns:
            ge.dataset.sparkdf_dataset.SparkDFDataset: GE-wrapped Spark DataFrame.
        """
        return ge.dataset.sparkdf_dataset.SparkDFDataset(self.df)

    ########## Orchestration ##########

    def _finalize_report_html(self) -> str:
        """
        Closes all open HTML tags in the report string.

        Returns:
            str: The finalized HTML report.
        """
        report = self.report
        if not report.strip().endswith("</table>"):
            report += "\n</table>\n</body>\n</html>"
        else:
            report += "\n</body>\n</html>"
        return report

    def _send_report_email(self, destination: list, status: str, color: str) -> None:
        """
        Renders and dispatches the HTML quality report via SES.

        Args:
            destination (list): List of recipient email addresses.
            status (str): Subject status label (e.g. 'Success', 'Failure').
            color (str): HTML colour for the report heading (e.g. 'green', 'orange').

        Returns:
            None
        """
        rendered_report = self._finalize_report_html().format(color=color)
        rendered_subject = self.subject.format(status=status)

        super().ses.send_email(
            sender=SENDER_EMAIL,
            destination=destination,
            message=rendered_report,
            subject=rendered_subject,
            mode="Html",
        )

    def run_quality_checks(self) -> None:
        """
        Executes all configured quality checks, dispatches notifications,
        and writes the quality log. Raises an exception if stop_job is True
        and any check has failed.

        Returns:
            None

        Raises:
            Exception: If stop_job is True and at least one expectation failed.
        """
        # Dynamically invoke each check method by name using eval
        for check_name, check_params in self.quality_params.items():
            call_expression = f"self.{check_name}({check_params})"
            print(f"Running quality check: {call_expression}")
            eval(call_expression)  # noqa: S307

        # Notify and optionally halt the job on failure
        if self.destination_on_failure and self.failed_expectation:
            self._send_report_email(
                destination=self.destination_on_failure,
                status="Failure",
                color="orange",
            )

            if self.logger:
                self.logger.warning("One or more quality checks failed.")

            if self.stop_job:
                self.quality_logger.add_info(quality_tests=self.quality_test_results)
                self.quality_logger.write_log()
                if self.logger:
                    self.logger.error("Job halted due to quality check failure.")
                raise Exception("One or more data quality checks failed.")

        # Notify on full success
        elif self.destination_on_success:
            self._send_report_email(
                destination=self.destination_on_success,
                status="Success",
                color="green",
            )

        # Update the parent logger with quality outcome
        if self.logger:
            self.logger.log["status"] = "warning" if self.failed_expectation else "success"
            self.logger.add_info(has_bdq=True)

        # Persist quality test results to the log table
        self.quality_logger.add_info(quality_tests=self.quality_test_results)
        self.quality_logger.write_log()

        print(self.report)

########## End Quality Class ##########