####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
#
# Description:
#   Provides the Logs class for structured job execution logging
#   across AWS Glue jobs and Lambda functions. Each execution
#   generates a unique ID via SHA-256, tracks step-level timing,
#   records status (success, warning, error), and writes the final
#   log record as a Parquet file to S3. After writing, triggers an
#   Athena MSCK REPAIR TABLE to make the new partition queryable.
#
# Usage type:
#   Instantiate once per job execution and use add_info(), warning(),
#   error(), and write_log() to build and persist the log record.
#   Example: logger = Logs(job_name, target_table, layer, env)
#
####################################################################

########## Imports ##########
import hashlib
import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta
########## End Imports ##########


########## Logs Class ##########

class Logs:
    """
    Structured execution logger for AWS Glue jobs and Lambda functions.

    Creates a unique execution record per job run, tracks step-level
    timing, captures warnings and errors, and persists the final log
    as a Parquet file partitioned by execution date in S3. After
    writing, repairs the Athena partition so the record is immediately
    queryable.
    """

    def __init__(
        self,
        job_name: str,
        target_table: str,
        layer: str,
        env: str = "prd",
        table: str = "execution_logs",
        technology: str = "glue",
    ) -> None:
        """
        Initialises the Logs instance and builds the base log record.

        Args:
            job_name (str): Name of the Glue job or Lambda function.
            target_table (str): Fully qualified target table name
                                (e.g. 'breweries_tb_breweries').
            layer (str): Processing layer the job belongs to
                         ('bronze', 'silver', 'gold', 'quality').
            env (str): Deployment environment ('prd', 'dev', etc.).
            table (str): Athena/S3 log table name (default 'execution_logs').
            technology (str): Writer technology — 'glue' uses pandas.to_parquet,
                              any other value uses awswrangler.

        Returns:
            None
        """
        self.job_name = job_name
        self.target_table = target_table
        self.layer = layer
        self.technology = technology
        self.env = env
        self.athena_table = table

        # Parse source domain and table name from the qualified identifier
        self._split_target_table()

        # Capture the execution start time (UTC-3) used for ID generation and timing
        self.start_execution = self._get_current_timestamp()
        self.execution_id = self._create_execution_id()

        self.athena_client = boto3.client("athena")

        # S3 output path partitioned by execution date for Athena compatibility
        self.path_write = (
            f"s3://bws-dl-logs-sae1-{env}/{table}"
            f"/dt_ref={self.start_execution.strftime('%Y-%m-%d')}"
            f"/{self.execution_id}.parquet"
        )

        # Accumulates free-form key-value pairs added via add_info()
        self.info = {}

        # Base log record — updated throughout the job lifecycle
        self.log = {
            "start_execution":    self.start_execution,
            "end_execution":      self.start_execution,
            "source":             self.source,
            "table_name":         self.table_name,
            "job_name":           self.job_name,
            "layer":              self.layer,
            "status":             "success",
            "error":              None,
            "error_description":  None,
            "warning_description": None,
            "has_bdq":            False,
            "critical_table":     False,
            "file_name":          None,
            "count":              None,
            "info":               {},
        }

    ########## Private Helpers ##########

    def _get_current_timestamp(self) -> datetime:
        """
        Returns the current datetime adjusted to UTC-3 (Brazil standard time).

        Returns:
            datetime: Current local datetime.
        """
        return datetime.today() - timedelta(hours=3)

    def _create_execution_id(self) -> str:
        """
        Generates a unique execution identifier using SHA-256.

        Combines job name, target table, and start timestamp to ensure
        uniqueness across concurrent and sequential executions.

        Returns:
            str: 64-character hexadecimal SHA-256 hash string.
        """
        unique_string = f"{self.job_name}-{self.target_table}-{self.start_execution}"
        return hashlib.sha256(unique_string.encode()).hexdigest()

    def _split_target_table(self) -> None:
        """
        Parses the target table identifier into source domain and table name.

        Convention: '<source>_<table_name>' — the source is the first
        underscore-delimited segment; the table name is everything after it.
        Falls back to None/full-string if parsing fails.

        Returns:
            None
        """
        try:
            parts = self.target_table.split("_")
            self.source = parts[0]
            self.table_name = "_".join(parts[1:])
        except Exception:
            # Degrade gracefully if the table name does not follow the convention
            self.source = None
            self.table_name = self.target_table

    ########## Public Methods ##########

    def add_info(self, **kwargs) -> None:
        """
        Adds arbitrary key-value metadata to the log record.

        Keys that match top-level log fields are written directly to self.log.
        All other keys are accumulated in self.info and serialised at write time.

        Args:
            **kwargs: Arbitrary keyword arguments representing log fields
                      or free-form metadata (e.g. count=123, file_name='data.csv').

        Returns:
            None

        Example:
            logger.add_info(count=1500, file_name="breweries_2026.csv")
        """
        for key, value in kwargs.items():
            if key in self.log:
                self.log[key] = value
            else:
                self.info[key] = value

    def time_execution_step(self, step_name: str) -> None:
        """
        Records the elapsed time since the last timing checkpoint.

        Should be called at the end of each logical step. Resets the
        internal start reference so the next call measures only the
        subsequent step's duration.

        Args:
            step_name (str): Label for the step whose duration is being recorded.

        Returns:
            None
        """
        current_timestamp = self._get_current_timestamp()
        elapsed = current_timestamp - self.start_execution

        # Reset the reference point for the next step's measurement
        self.start_execution = current_timestamp

        elapsed_seconds = round(elapsed.total_seconds(), 2)
        self.info[step_name] = str(elapsed_seconds)

    def warning(self, warning_msg: str) -> None:
        """
        Records a warning condition without writing the log to S3.

        Sets the job status to 'warning' and stores the message.
        The log is persisted only when write_log() is called explicitly.

        Args:
            warning_msg (str): Human-readable description of the warning.

        Returns:
            None
        """
        self.log["status"] = "warning"
        self.log["warning_description"] = warning_msg

    def error(self, error_msg: str, error_desc: str = None) -> None:
        """
        Records an error condition and immediately persists the log to S3.

        Sets the job status to 'error', stores the message and optional
        structured description, then calls write_log() automatically so
        the failure is captured even if the job is about to raise an exception.

        Args:
            error_msg (str): Human-readable description of the error.
            error_desc (str): Optional structured error detail, typically
                              the output of summarize_exception().

        Returns:
            None
        """
        self.log["status"] = "error"
        self.log["error"] = error_msg
        self.log["error_description"] = error_desc

        # Write immediately so the error is persisted before the job halts
        self.write_log()

    def write_log(self) -> None:
        """
        Finalises and writes the log record as a Parquet file to S3.

        Stamps the end_execution timestamp, serialises the info dict,
        builds a single-row DataFrame, and writes it to the configured
        S3 path. After writing, triggers an Athena MSCK REPAIR TABLE
        to register the new partition for querying.

        Returns:
            None
        """
        self.log["end_execution"] = self._get_current_timestamp()
        self.log["info"] = str(self.info)

        df = pd.DataFrame([self.log])

        print("##################################################")
        print(f"Log written to: {self.path_write}")
        print(f"Log record: {self.log}")
        print(f"Info column: {self.info}")
        print("##################################################")

        # Choose the writer based on the execution technology
        if self.technology == "glue":
            # Native pandas writer — available in the Glue Spark environment
            df.to_parquet(self.path_write)
        else:
            # AWS Data Wrangler for non-Glue environments (e.g. Lambda, local)
            wr.s3.to_parquet(df=df, path=self.path_write, dataset=False)

        # Repair the Athena table partition so the new record is immediately queryable
        self.athena_client.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE logs.{self.athena_table}",
            QueryExecutionContext={"Database": "logs"},
            ResultConfiguration={
                "OutputLocation": f"s3://bws-dl-logs-sae1-{self.env}/athena/query_results/"
            },
        )

########## End Logs Class ##########