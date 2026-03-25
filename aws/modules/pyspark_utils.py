####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
#
# Description:
#   Provides a Pyspark utility class used across AWS Glue jobs.
#   Handles reading files from S3 (CSV, TXT, JSON), casting and
#   transforming Spark DataFrames, writing to Athena/Iceberg Silver
#   tables, executing JDBC queries against relational databases,
#   and writing output files back to S3.
#
# Usage type:
#   Instantiate Pyspark within a Glue job and use its methods to
#   read, transform, and write data across the data lake layers.
#
####################################################################

########## Imports ##########
from datetime import datetime, date, timedelta
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, StructType, StructField, StringType
from pyspark.sql.functions import col, lit, to_date, to_timestamp, trim, rtrim, translate, when
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
from py4j.protocol import Py4JJavaError

from utils import S3
from support import write_error_logs, split_target_table
########## End Imports ##########


########## Global Variables ##########

# Date/time references adjusted to UTC-3 (Brazil standard time)
_now_br = datetime.now() - timedelta(hours=3)

TODAY = date.today().strftime("%Y%m%d")
TIMESTAMP_NOW = _now_br.strftime("%Y_%m_%d_%H_%M_%S")
DATE_AND_TIME = _now_br.strftime("%Y-%m-%d %H:%M:%S")

########## End Global Variables ##########


########## Pyspark Class ##########

class Pyspark(S3):
    """
    Spark utility class for AWS Glue jobs.

    Wraps common Spark operations including S3 file reads, DataFrame
    casting, filtering, row skipping, JDBC connectivity, and writes
    to Athena/Iceberg tables.
    """

    def __init__(
        self,
        job_name: str,
        spark,
        env: str = "prd",
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        glueContext=None,
    ) -> None:
        """
        Initialises the Pyspark utility instance.

        Args:
            job_name (str): Name of the Glue job for logging purposes.
            spark: Active SparkSession instance.
            env (str): Deployment environment ('prd', 'dev', etc.).
            logger: Logger instance for structured job logging.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name (e.g. 'domain.table').
            glueContext: GlueContext instance, required for RDS JDBC writes.

        Returns:
            None
        """
        self.logger = logger
        super().__init__(job_name, self.logger)

        # Store a reference to the parent SES/S3 instance for error reporting
        self.super_ses = super()

        self.env = env
        self.spark = spark
        self.glueContext = glueContext
        self.destination = destination
        self.trgt_tbl = trgt_tbl

        # Tracks the final column order after casting (used for select at the end)
        self.column_order = []

        if trgt_tbl:
            self.table, self.source = split_target_table(trgt_tbl)

    ########## Private Helpers ##########

    def _apply_lit_value(self, lit_value: dict, df_aux=None, df=None) -> DataFrame:
        """
        Adds a literal or row-derived column to a DataFrame.

        Supports two modes:
          - 'row': extracts a substring from a specific raw file row and
            converts it to a date column.
          - default: adds a static literal date column using the configured
            value and format mask.

        Args:
            lit_value (dict): Configuration dict containing keys:
                'value', 'column_name', 'mask', 'dt_type'.
                If 'value' starts with 'row', also expects a comma-separated
                format: 'row,<line_number>,<start>:<end>'.
            df_aux (DataFrame): Raw auxiliary DataFrame used for row extraction.
            df (DataFrame): Target DataFrame to which the column is added.

        Returns:
            DataFrame: DataFrame with the new literal column appended.
        """
        # Row-extraction mode: pull a date substring from a specific raw row
        if "row" in lit_value["value"]:
            _, line_number, start_end = lit_value["value"].split(",")
            raw_row = df_aux.limit(1).collect()[int(line_number)][0]
            start, end = start_end.split(":")
            date_str = raw_row[int(start):int(end)]
            df = df.withColumn(
                lit_value["column_name"],
                to_date(lit(date_str), lit_value["mask"]),
            )
        else:
            # Static literal mode — currently supports date type only
            if lit_value["dt_type"].lower() == "date":
                df = df.withColumn(
                    lit_value["column_name"],
                    to_date(lit(lit_value["value"]), lit_value["mask"]),
                )


        self.column_order.append(lit_value["column_name"])
        return df

    def _build_jdbc_url(self, technology: str, db_host: str, db_port: str, db_name: str) -> str:
        """
        Constructs a JDBC connection URL for the specified database technology.

        Args:
            technology (str): Database technology ('sqlserver', 'oracle', 'postgresql').
            db_host (str): Database host address.
            db_port (str): Database port.
            db_name (str): Database or service name.

        Returns:
            str: Fully formatted JDBC URL.

        Raises:
            Exception: If the technology is not supported.
        """
        tech = technology.lower()

        if tech == "sqlserver":
            return f"jdbc:sqlserver://{db_host}:{db_port};databaseName={db_name}"
        elif tech == "oracle":
            return f"jdbc:oracle:thin:@//{db_host}:{db_port}/{db_name}"
        elif tech == "postgresql":
            return f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
        else:
            raise Exception(f"Unsupported database technology: '{technology}'.")

    def _skip_header_row(self, df: DataFrame) -> DataFrame:
        """
        Removes the first row of the DataFrame (used for files with metadata headers).

        Args:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with the first row removed.
        """
        try:
            first_row = df.first()
            first_row_df = self.spark.createDataFrame([first_row], df.schema)
            return df.exceptAll(first_row_df)
        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while skipping header row.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def _skip_footer_row(self, df: DataFrame) -> DataFrame:
        """
        Removes the last row of the DataFrame (used for files with metadata footers).

        Args:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with the last row removed.
        """
        try:
            last_row = df.tail(1)
            last_row_df = self.spark.createDataFrame(last_row, df.schema)
            return df.exceptAll(last_row_df)
        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while skipping footer row.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def _read_csv_file(self, s3_path: str, schema: dict, params: dict) -> DataFrame:
        """
        Internal method that executes the Spark CSV read with resolved parameters.

        When the file has a header or uses a newline separator, reads normally
        and optionally normalises column names against the schema. Otherwise,
        injects a StructType schema directly into the reader.

        Args:
            s3_path (str): S3 path to the file or partition folder.
            schema (dict): Expected column-to-type mapping.
            params (dict): Spark reader options (header, sep, encoding, etc.).

        Returns:
            DataFrame: Spark DataFrame loaded from S3.
        """
        try:
            if params["header"] == "true" or params["sep"] == "\n":
                # Remove internal param before passing to Spark
                normalize_schema = params.get("kwargs", {}).pop("normalize_schema", None)

                df = self.spark.read.options(**params).csv(s3_path)

                if str(normalize_schema).lower() == "true":
                    # Normalise file columns to uppercase and align with schema keys
                    df = df.toDF(*[c.upper() for c in df.columns])
                    expected_cols = [c.upper() for c in schema.keys()]

                    # Add NULL columns for any schema keys absent in the file
                    missing_cols = [c for c in expected_cols if c not in df.columns]
                    if missing_cols:
                        print(f"Columns missing in file, will be added as NULL: {missing_cols}")
                    for missing_col in missing_cols:
                        df = df.withColumn(missing_col, lit(None))

                    df = df.select(*expected_cols)
            else:
                # No header: inject schema so Spark assigns correct column names
                columns = list(schema.keys())
                struct_schema = StructType(
                    [StructField(c, StringType(), True) for c in columns]
                )
                df = self.spark.read.options(**params).csv(s3_path, schema=struct_schema)

            if self.logger:
                self.logger.time_execution_step(step_name="read_file")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while reading CSV file from S3.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

        return df

    def _unpackage_json_kwargs(self, kwargs: dict, encoding_default: str = "UTF-8") -> dict:
        """
        Maps user-friendly keyword arguments into Spark JSON reader option keys.

        All parameters default to Spark-safe values. See Spark documentation:
        https://spark.apache.org/docs/latest/sql-data-sources-json.html

        Args:
            kwargs (dict): User-provided options using snake_case names.
            encoding_default (str): Fallback encoding if not specified in kwargs.

        Returns:
            dict: Spark-compatible options dictionary ready for spark.read.options().
        """
        line_sep = kwargs.get("line_sep", None)

        json_params = {
            "primitivesAsString":               kwargs.get("primitive_as_string", "false"),
            "prefersDecimal":                   kwargs.get("prefers_decimal", "false"),
            "allowComments":                    kwargs.get("allow_comments", "false"),
            "allowUnquotedFieldNames":          kwargs.get("allow_unquoted_field_names", "false"),
            "allowSingleQuotes":                kwargs.get("allow_single_quotes", "true"),
            "allowNumericLeadingZeros":         kwargs.get("allow_numeric_leading_zeros", "false"),
            "allowBackslashEscapingAnyCharacter": kwargs.get(
                "allow_backslash_escaping_any_character", "false"
            ),
            "columnNameOfCorruptRecord":        kwargs.get(
                "column_name_of_corrupt_record", "_corrupt_record"
            ),
            "enableDateTimeParsingFallback":    kwargs.get(
                "enable_datetime_parsing_fallback", "true"
            ),
            "multiLine":                        kwargs.get("multiline", "true"),
            "allowUnquotedControlChars":        kwargs.get("allow_unquoted_control_chars", "false"),
            "encoding":                         kwargs.get("encoding", encoding_default),
            "dropFieldIfAllNull":               kwargs.get("drop_field_if_all_null", "false"),
            "locale":                           kwargs.get("locale", "en-US"),
            "allowNonNumericNumbers":           kwargs.get("allow_non_numeric_numbers", "true"),
            "useUnsafeRow":                     kwargs.get("use_unsafe_row", "true"),
        }

        # lineSep is optional — only add if explicitly provided
        if line_sep:
            json_params["lineSep"] = line_sep

        return json_params

    ########## DataFrame Transformation ##########

    def cast_df(
        self,
        df: DataFrame,
        schema: dict,
        ext: str = None,
        positional_column: list = None,
        lit_values: dict = None,
        partition_column: str = None,
    ) -> DataFrame:
        """
        Casts DataFrame columns to the types defined in the schema.

        For TXT files, reads a substring definition from S3 and applies it
        via eval to reshape the raw single-column DataFrame. For other formats,
        iterates over schema entries and casts each column, applying date/
        timestamp formatting, decimal precision, and double normalisation
        (comma-to-dot conversion) as needed. Trims whitespace from string
        columns unless they are positional.

        Args:
            df (DataFrame): Input Spark DataFrame to cast.
            schema (dict): Column-to-type mapping. Values may be:
                - str: simple type name ('string', 'int', 'double', etc.)
                - list[2]: [type, format] for date/timestamp casting
                - list[3]: [source_col, type, format] for renamed date/timestamp
            ext (str): File extension ('csv', 'txt', 'json').
            positional_column (list): Columns that must not be trimmed.
            lit_values (dict): Configuration for adding a literal/derived column.
            partition_column (str): Partition column name, appended last in order.

        Returns:
            DataFrame: Fully cast and ordered Spark DataFrame.
        """
        if positional_column is None:
            positional_column = []

        try:
            if ext == "txt":
                # TXT files arrive as a single column 'value' containing the raw line
                df = df.withColumnRenamed("_c0", "value")
                df_aux = df

                # Fetch the substring extraction script from S3 artifacts bucket
                bucket = f"bws-artifacts-sae1-{self.env}"
                table, source = split_target_table(self.trgt_tbl)
                key = f"support/{source}/{table}/substring_{table}.txt"
                response = super().get_s3_file(bucket=bucket, key=key)

                print(df.show())

                # The S3 script returns a Spark transformation expression — evaluate it
                df = eval(response)  # noqa: S307 — controlled internal usage

                if lit_values:
                    df = self._apply_lit_value(lit_value=lit_values, df_aux=df_aux, df=df)

                # Trim whitespace from all string columns except date ingestion and positional ones
                for column in df.columns:
                    col_type = df.schema[column].dataType
                    is_string = "StringType" in str(col_type)
                    is_excluded = column == "date_ingestion" or column in positional_column

                    if is_string and not is_excluded:
                        print(f"Trimming column: {column}")
                        df = df.withColumn(column, rtrim(trim(df[column])))

            else:
                # CSV / JSON: cast each column according to its schema definition
                for raw_column in df.columns:
                    column = raw_column.lower()
                    print(f"Casting column: {column}")

                    # Track column order, keeping partition column for the end
                    if column != partition_column:
                        self.column_order.append(column)

                    data_type = schema[column]

                    if isinstance(data_type, list):
                        # List format: [type, format] or [source_col, type, format]
                        if len(data_type) == 2:
                            cast_type, fmt = data_type
                            source_col = column
                        elif len(data_type) == 3:
                            source_col, cast_type, fmt = data_type

                        if cast_type == "timestamp":
                            df = df.withColumn(
                                column,
                                to_timestamp(col(source_col), fmt).cast("timestamp"),
                            )
                        elif cast_type == "date":
                            df = df.withColumn(column, to_date(col(source_col), fmt))

                    elif isinstance(data_type, str) and data_type.startswith("decimal("):
                        # Parse precision and scale from 'decimal(p,s)' notation
                        inner = data_type.split("(")[1].rstrip(")")
                        precision, scale = inner.split(",")
                        df = df.withColumn(
                            column,
                            col(column).cast(DecimalType(int(precision), int(scale))),
                        )

                    elif data_type == "double":
                        # Normalise European number format: remove thousand dots, swap decimal comma
                        df = df.withColumn(
                            column,
                            when(
                                col(column).contains(","),
                                translate(translate(col(column), ".", ""), ",", "."),
                            ).otherwise(col(column)),
                        )
                        df = df.withColumn(column, col(column).cast("double"))

                    else:
                        df = df.withColumn(column, col(column).cast(data_type))

                        # Trim string columns unless they are positional fixed-width
                        if data_type.lower() == "string" and column not in positional_column:
                            df = df.withColumn(
                                column, rtrim(trim(df[column])).cast(data_type)
                            )

            # Append the literal/derived column after all schema columns
            if lit_values:
                df = self._apply_lit_value(lit_value=lit_values, df=df)

            # Append partition column last and reorder the DataFrame accordingly
            if (
                partition_column
                and (lit_values or column.lower() != partition_column)
                and partition_column not in self.column_order
            ):
                self.column_order.append(partition_column)
                df = df.select(*self.column_order)

            if self.logger:
                self.logger.time_execution_step(step_name="cast_dataframe_columns")

            print("cast_df completed successfully.")
            return df

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while casting DataFrame columns.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    ########## Read Methods ##########

    def read_csv_file_from_s3(
        self,
        s3_path: str,
        ext: str,
        header: bool,
        sep: str = None,
        encoding: str = "UTF-8",
        schema: dict = None,
        **kwargs,
    ) -> DataFrame:
        """
        Reads a CSV or TXT file from S3 into a Spark DataFrame.

        Validates that a separator is provided for CSV files and that a schema
        is available when the file has no header. Delegates the actual read
        to the internal _read_csv_file method.

        Args:
            s3_path (str): Full S3 path to the file or partition folder.
            ext (str): File extension ('csv' or 'txt').
            header (bool): Whether the file contains a header row.
            sep (str): Column delimiter (required for CSV).
            encoding (str): File encoding, default 'UTF-8'.
            schema (dict): Column schema; required when header is False.
            **kwargs: Additional Spark reader options forwarded to _read_csv_file.

        Returns:
            DataFrame: Spark DataFrame loaded from S3.

        Raises:
            Exception: If CSV is provided without a separator.
        """
        if not header and not schema:
            if self.logger:
                self.logger.error(
                    "Error reading file",
                    '{"error_message":"Schema is required when header=False."}',
                )
            raise Exception("No header provided and no schema passed.")

        if not sep and ext == "csv":
            if self.logger:
                self.logger.error(
                    "Error reading file",
                    '{"error_message":"Separator is required for CSV files."}',
                )
            raise Exception("CSV file provided without a separator.")

        # Default TXT separator to newline (single-column raw line mode)
        if not sep:
            sep = "\n"

        params = {
            "header": "true" if header else "false",
            "sep": sep,
            "encoding": encoding,
        }
        params.update(kwargs)

        return self._read_csv_file(s3_path=s3_path, schema=schema, params=params)

    def read_json_file_from_s3(
        self,
        s3_path: str,
        encoding: str = "UTF-8",
        explode_column: str = None,
        **kwargs,
    ) -> DataFrame:
        """
        Reads a JSON file from S3 into a Spark DataFrame.

        Optionally explodes a nested array column into individual rows.

        Args:
            s3_path (str): Full S3 path to the file or partition folder.
            encoding (str): File encoding, default 'UTF-8'.
            explode_column (str): Column containing an array to explode, if any.
            **kwargs: Additional Spark JSON reader options (snake_case).

        Returns:
            DataFrame: Spark DataFrame loaded from S3.
        """
        spark_options = self._unpackage_json_kwargs(kwargs=kwargs, encoding_default=encoding)

        try:
            df = self.spark.read.format("json").options(**spark_options).load(s3_path)

            if explode_column:
                df = self.explode_df(df, explode_column)

            if self.logger:
                self.logger.time_execution_step(step_name="read_file")

            return df

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while reading JSON file from S3.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    ########## DataFrame Operations ##########

    def explode_df(self, df: DataFrame, column: str) -> DataFrame:
        """
        Explodes a nested array column into individual top-level rows.

        Args:
            df (DataFrame): Input Spark DataFrame containing the array column.
            column (str): Name of the column to explode.

        Returns:
            DataFrame: Flattened DataFrame with array elements as rows.
        """
        try:
            return df.select(F.explode(column).alias(column)).select(f"{column}.*")
        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error while exploding column '{column}'.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def filter_df(
        self,
        df: DataFrame,
        filter_column: str,
        filter_value: str,
        is_re: bool,
    ) -> DataFrame:
        """
        Filters a DataFrame by a column value or regex pattern.

        Args:
            df (DataFrame): Input Spark DataFrame.
            filter_column (str): Column to apply the filter on.
            filter_value (str): Value or regex pattern to filter by.
            is_re (bool): If True, applies a regex filter (rlike). If False,
                          applies an equality filter.

        Returns:
            DataFrame: Filtered Spark DataFrame.
        """
        try:
            if is_re:
                df = df.filter(df[filter_column].rlike(filter_value))
            else:
                df = df.filter(col(filter_column) == filter_value)

            if self.logger:
                self.logger.time_execution_step(step_name="filter_df")

            return df

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while filtering DataFrame.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def skip_rows(
        self,
        df: DataFrame,
        skip_footer: str = None,
        skip_header: str = None,
    ) -> DataFrame:
        """
        Removes header and/or footer rows from a DataFrame.

        Used for fixed-width or non-standard files that contain metadata rows
        at the start or end that should not be ingested.

        Args:
            df (DataFrame): Input Spark DataFrame.
            skip_header (str): Pass any truthy string to remove the first row.
            skip_footer (str): Pass any truthy string to remove the last row.

        Returns:
            DataFrame: DataFrame with the specified rows removed.
        """
        if skip_header:
            df = self._skip_header_row(df)

        if skip_footer:
            df = self._skip_footer_row(df)

        if self.logger:
            self.logger.time_execution_step(step_name="skip_rows")

        return df

    ########## Write Methods ##########

    def write(
        self,
        df: DataFrame,
        bucket: str,
        file_type: str,
        header: str = "True",
        sep: str = None,
        compression: str = "gzip",
    ) -> None:
        """
        Writes a Spark DataFrame to S3 in the specified file format.

        The output path is automatically constructed using the target table
        name and today's date as a partition.

        Args:
            df (DataFrame): Spark DataFrame to write.
            bucket (str): Destination S3 bucket name.
            file_type (str): Output format ('csv' or 'parquet').
            header (str): Whether to include a header row ('True'/'False').
            sep (str): Column delimiter for CSV output.
            compression (str): Compression codec for CSV output (default 'gzip').

        Returns:
            None
        """
        try:
            table_name, source = split_target_table(self.trgt_tbl)
            target_path = f"s3://{bucket}/{source}/{table_name}/{TODAY}/"

            if file_type.lower() == "csv":
                df.write.csv(target_path, header=header, sep=sep, compression=compression)
            elif file_type.lower() == "parquet":
                df.write.parquet(target_path)

            count = df.count()
            self.logger.add_info(count=str(count))

            if self.logger:
                self.logger.time_execution_step(step_name="writing_dataframe")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while writing DataFrame to S3.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def insert_into_at_tbl(
        self,
        df: DataFrame,
        athena_tbl: str,
        mode: str,
        query_iceberg: str = None,
    ) -> None:
        """
        Writes a Spark DataFrame into an Athena or Iceberg table.

        Supports three modes:
          - 'append': inserts rows without removing existing data.
          - 'overwrite': replaces all existing data.
          - 'iceberg': executes a custom Iceberg merge/upsert SQL statement.

        For append/overwrite modes, if the S3 path does not exist yet, it is
        created automatically before retrying the write.

        Args:
            df (DataFrame): Spark DataFrame to insert.
            athena_tbl (str): Fully qualified Athena table name (e.g. 'db.table').
            mode (str): Write mode — 'append', 'overwrite', or 'iceberg'.
            query_iceberg (str): Iceberg SQL statement to execute when mode='iceberg'.
                                 The DataFrame is registered as 'source_table'.

        Returns:
            None

        Raises:
            Exception: If an unsupported mode is provided.
        """
        print(f"Inserting into Athena table: {athena_tbl} | mode: {mode}")

        # Reorder DataFrame columns to exactly match the target table definition
        table_columns = [c.name for c in self.spark.table(athena_tbl).schema]
        df = df.select(*table_columns)

        if mode in ("append", "overwrite"):
            try:
                df.write.format("parquet").mode(mode).insertInto(athena_tbl)

            except (AnalysisException, Py4JJavaError) as e:
                error_str = str(e)

                # If the S3 path was never created, initialise it and retry
                if "Path does not exist" in error_str or "PATH_NOT_FOUND" in error_str:
                    print("S3 path does not exist — creating it before retrying.")
                    bucket = f"bws-dl-silver-sae1-{self.env}"
                    key = f"{self.source}/{self.table}/"
                    super().put_s3_file(bucket=bucket, key=key, body=b"")
                    df.write.format("parquet").mode(mode).insertInto(athena_tbl)
                else:
                    write_error_logs(
                        logger=self.logger,
                        error_msg="Error while inserting DataFrame into Athena table.",
                        e=e,
                        destination=self.destination,
                        super=self.super_ses,
                        target_tbl=self.trgt_tbl,
                    )

            except Exception as e:
                write_error_logs(
                    logger=self.logger,
                    error_msg="Error while inserting DataFrame into Athena table.",
                    e=e,
                    destination=self.destination,
                    super=self.super_ses,
                    target_tbl=self.trgt_tbl,
                )

        elif mode == "iceberg":
            try:
                # Register DataFrame as a temp view so the Iceberg SQL can reference it
                df.createOrReplaceTempView("source_table")
                self.spark.sql(query_iceberg)

            except Exception as e:
                write_error_logs(
                    logger=self.logger,
                    error_msg="Error while executing Iceberg insert/merge.",
                    e=e,
                    destination=self.destination,
                    super=self.super_ses,
                    target_tbl=self.trgt_tbl,
                )

        else:
            raise Exception(
                f"Unsupported write mode: '{mode}'. Accepted values: 'append', 'overwrite', 'iceberg'."
            )

        if self.logger:
            self.logger.time_execution_step(step_name="insert_table")

    ########## JDBC Methods ##########

    def run_query_jdbc(
        self,
        db_host: str,
        db_port: str,
        db_name: str,
        db_user: str,
        db_psswd: str,
        query: str,
        technology: str,
    ) -> DataFrame:
        """
        Connects to a relational database via JDBC and executes a SQL query.

        Supported technologies: SQL Server, Oracle, PostgreSQL.

        Args:
            db_host (str): Database host address.
            db_port (str): Database port.
            db_name (str): Database or service name.
            db_user (str): Database username.
            db_psswd (str): Database password.
            query (str): SQL query to execute.
            technology (str): Database technology ('sqlserver', 'oracle', 'postgresql').

        Returns:
            DataFrame: Spark DataFrame with the query results.
        """
        try:
            jdbc_url = self._build_jdbc_url(technology, db_host, db_port, db_name)

            print("Running JDBC query...")

            df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("user", db_user)
                .option("password", db_psswd)
                .option("query", query)
                .load()
            )

            print("JDBC query executed successfully.")

            if self.logger:
                self.logger.time_execution_step(step_name="run_query_into_db")

            return df

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while executing JDBC query.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def insert_df_to_db(
        self,
        df: DataFrame,
        host: str,
        port: str,
        dbname: str,
        user: str,
        pswd: str,
        mode: str,
        technology: str,
    ) -> None:
        """
        Writes a Spark DataFrame to a relational database table via JDBC.

        Currently supports Oracle. The target table is determined by
        the instance's trgt_tbl attribute.

        Args:
            df (DataFrame): Spark DataFrame to write.
            host (str): Database host address.
            port (str): Database port.
            dbname (str): Database or service name.
            user (str): Database username.
            pswd (str): Database password.
            mode (str): Write mode ('overwrite' or 'append').
            technology (str): Database technology ('oracle').

        Returns:
            None
        """
        try:
            if technology == "oracle":
                url = f"jdbc:oracle:thin:@//{host}:{port}/{dbname}"
                properties = {
                    "user": user,
                    "password": pswd,
                    "driver": "oracle.jdbc.driver.OracleDriver",
                }

            df.write.jdbc(url=url, table=self.trgt_tbl, mode=mode, properties=properties)

            if self.logger:
                self.logger.time_execution_step(step_name="insert_df_into_db")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while inserting DataFrame into database via JDBC.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def insert_df_to_db_rds(
        self,
        df: DataFrame,
        connection_name: str,
        target_table: str,
        mode: str = "append",
    ) -> None:
        """
        Writes a Spark DataFrame to an RDS table using a Glue JDBC connection.

        Extracts connection credentials from the GlueContext and writes
        using the Spark JDBC writer. When mode is 'overwrite', the target
        table is truncated before insertion.

        Args:
            df (DataFrame): Spark DataFrame to write.
            connection_name (str): Name of the Glue connection to use.
            target_table (str): Destination table name in the database.
            mode (str): Write mode ('append' or 'overwrite').

        Returns:
            None

        Raises:
            Exception: Propagates any write failure after logging.
        """
        try:
            jdbc_conf = self.glueContext.extract_jdbc_conf(connection_name)
            url = jdbc_conf.get("fullUrl") or jdbc_conf.get("url")

            write_options = {
                "url": url,
                "dbtable": target_table,
                "user": jdbc_conf["user"],
                "password": jdbc_conf["password"],
                "batchsize": "10000",
            }

            # Truncate the destination table before inserting in overwrite mode
            if mode == "overwrite":
                write_options["truncate"] = "true"

            df.write.format("jdbc").options(**write_options).mode(mode).save()

            if self.logger:
                self.logger.time_execution_step(step_name="insert_df_to_db_rds")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while writing DataFrame to RDS via Glue JDBC connection.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )
            raise


########## End Pyspark Class ##########