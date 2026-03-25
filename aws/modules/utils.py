####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
#
# Description:
#   Provides reusable AWS service wrapper classes used across
#   Glue jobs and Lambda functions. Includes clients for SES
#   (email notifications), S3 (file operations), DynamoDB
#   (parameter and record management), SSM (secrets), Athena
#   (query execution via boto3 and PyAthena), SNS (topic
#   messaging), SQS (queue messaging), Bedrock (AI model
#   invocation), and Pandas (DataFrame utilities).
#   The AwsManager class acts as a lazy-initialised facade,
#   providing a single entry point to all services.
#
# Usage type:
#   Instantiate AwsManager once per job and access services via
#   its properties (e.g. manager.s3.get_s3_file(bucket, key)).
#   Individual service classes can also be instantiated directly.
#
####################################################################

########## Imports ##########
import boto3
import json
import time
from decimal import Decimal
from datetime import datetime, date, timedelta

# Optional dependencies — not available in all environments
try:
    import pandas as pd
except ImportError:
    pass

try:
    from support import write_error_logs, get_date_and_time, split_target_table
except ImportError:
    pass

try:
    from pyathena import connect
except ImportError:
    pass
########## End Imports ##########


########## Global Variables ##########

# Date/time references adjusted to UTC-3 (Brazil standard time)
_now_br = datetime.now() - timedelta(hours=3)

TODAY = date.today().strftime("%Y%m%d")
TIMESTAMP_NOW = _now_br.strftime("%Y_%m_%d_%H_%M_%S")

# Sender address used across all SES notifications
SENDER_EMAIL = "brun0ws@outlook.com"

# Internal escalation email for critical table failures
CRITICAL_EMAIL = "brun0ws@outlook.com"

########## End Global Variables ##########


########## Ses Class ##########

class Ses:
    """
    AWS SES wrapper for sending HTML and plain-text notification emails.

    Provides helper methods for standardised failure, warning, and
    success email templates used across Glue jobs and Lambda functions.
    """

    def __init__(self, job_name: str, logger=None, env: str = "prd") -> None:
        """
        Initialises the SES client and email templates.

        Args:
            job_name (str): Name of the job for use in email subjects and bodies.
            logger: Optional logger instance for structured step timing.
            env (str): Deployment environment ('prd', 'dev', etc.).

        Returns:
            None
        """
        self.ses_client = boto3.client("ses", region_name="sa-east-1")
        self.logger = logger
        self.env = env
        self.job_name = job_name

        # Base HTML template for all notification emails
        self.main_template = """<html>
        <body>
        <h3 style="color: {color};"> <strong>[{type_error}] - {table_name} - {job_name} </strong></h3>
        <p>Execution details:</p>
        <ul>
            <li><strong>Table: </strong>{table_name}</li>
            <li><strong>Domain: </strong>{source}</li>
            <li><strong>Executed at: </strong>{date_and_time}</li>
            <li><strong>Description: </strong>{description}</li>
            <li><strong>Environment: </strong>{env}</li>
        </ul>
        <p>Regards,</p>
        <p><strong>Data Engineering - Brewery Data Lake</strong></p>
        </body>
        </html>"""

        # Subject template — filled in by each notification method
        self.subject = "{type_error} - {table_name} - {job_name}"

    def send_email(
        self,
        sender: str,
        destination: list,
        message: str,
        subject: str = "",
        mode: str = "Text",
    ) -> None:
        """
        Sends an email via AWS SES.

        The sender address must be verified in SES. The mode parameter
        controls whether the body is interpreted as plain text or HTML.

        Args:
            sender (str): Verified SES sender email address.
            destination (list): List of recipient email addresses.
            message (str): Email body content.
            subject (str): Email subject line.
            mode (str): Body format — 'Text' for plain text, 'Html' for HTML.

        Returns:
            None
        """
        try:
            self.ses_client.send_email(
                Destination={"ToAddresses": destination},
                Message={
                    "Body": {mode: {"Charset": "UTF-8", "Data": message}},
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                },
                Source=sender,
            )

            if self.logger:
                self.logger.time_execution_step(step_name="send_notification_email")

        except Exception as e:
            write_error_logs(self.logger, "Error while sending email via SES.", e)

    def send_email_on_failure(
        self, target_table: str, description: str, destination: list
    ) -> None:
        """
        Sends a standardised ERROR notification email.

        Args:
            target_table (str): Fully qualified table name (e.g. 'domain.table').
            description (str): Human-readable description of the failure.
            destination (list): List of recipient email addresses.

        Returns:
            None
        """
        table_name, source = split_target_table(target_table)

        subject = self.subject.format(
            type_error="ERROR",
            table_name=table_name,
            job_name=self.job_name,
        )
        message = self.main_template.format(
            color="red",
            table_name=table_name,
            type_error="ERROR",
            job_name=self.job_name,
            date_and_time=get_date_and_time(),
            source=source,
            description=description,
            env=self.env,
        )

        self.send_email(
            sender=SENDER_EMAIL,
            destination=destination,
            message=message,
            subject=subject,
            mode="Html",
        )

    def send_email_on_warning(
        self, target_table: str, description: str, destination: list
    ) -> None:
        """
        Sends a standardised WARNING notification email.

        Args:
            target_table (str): Fully qualified table name (e.g. 'domain.table').
            description (str): Human-readable description of the warning condition.
            destination (list): List of recipient email addresses.

        Returns:
            None
        """
        table_name, source = split_target_table(target_table)

        subject = self.subject.format(
            type_error="WARNING",
            table_name=table_name,
            job_name=self.job_name,
        )
        message = self.main_template.format(
            color="orange",
            table_name=table_name,
            type_error="WARNING",
            job_name=self.job_name,
            date_and_time=get_date_and_time(),
            source=source,
            description=description,
            env=self.env,
        )

        self.send_email(
            sender=SENDER_EMAIL,
            destination=destination,
            message=message,
            subject=subject,
            mode="Html",
        )

    def send_email_on_success(
        self,
        target_table: str,
        destination: list,
        description: str = "Table executed successfully!",
    ) -> None:
        """
        Sends a standardised SUCCESS notification email.

        Args:
            target_table (str): Fully qualified table name (e.g. 'domain.table').
            destination (list): List of recipient email addresses.
            description (str): Optional success message body text.

        Returns:
            None
        """
        table_name, source = split_target_table(target_table)

        subject = self.subject.format(
            type_error="SUCCESS",
            table_name=table_name,
            job_name=self.job_name,
        )
        message = self.main_template.format(
            color="green",
            table_name=table_name,
            type_error="SUCCESS",
            job_name=self.job_name,
            date_and_time=get_date_and_time(),
            source=source,
            description=description,
            env=self.env,
        )

        self.send_email(
            sender=SENDER_EMAIL,
            destination=destination,
            message=message,
            subject=subject,
            mode="Html",
        )

########## End Ses Class ##########


########## S3 Class ##########

class S3(Ses):
    """
    AWS S3 wrapper for common file operations.

    Provides methods to read, write, copy, delete, and list objects
    within S3 buckets.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the S3 resource and client.

        Args:
            job_name (str): Job name used for error reporting.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.destination = destination
        self.trgt_tbl = trgt_tbl
        self.env = env
        self.logger = logger
        self.s3 = boto3.resource("s3")
        self.s3_client = boto3.client("s3")

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def get_s3_file(self, bucket: str, key: str) -> str:
        """
        Reads a file from S3 and returns its content as a UTF-8 string.

        Args:
            bucket (str): Name of the S3 bucket.
            key (str): Object key (path within the bucket).

        Returns:
            str: File content decoded as UTF-8.
        """
        try:
            response = self.s3.Object(bucket, key)
            s3_file = response.get()["Body"].read().decode("utf-8")

            if self.logger:
                self.logger.time_execution_step(step_name="get_s3_file_content")

            return s3_file

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"File not found in S3: s3://{bucket}/{key}",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def put_s3_file(self, bucket: str, key: str, body: bytes) -> dict:
        """
        Writes a binary object to a specified S3 path.

        Args:
            bucket (str): Destination S3 bucket name.
            key (str): Object key (path within the bucket).
            body (bytes): Binary content to write.

        Returns:
            dict: AWS S3 PutObject response metadata.
        """
        try:
            response = self.s3_client.put_object(Bucket=bucket, Key=key, Body=body)
            return response

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error creating S3 object at s3://{bucket}/{key}",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        trgt_bucket: str,
        trgt_key: str,
    ) -> None:
        """
        Copies an S3 object from one path to another.

        Args:
            src_bucket (str): Source bucket name.
            src_key (str): Source object key.
            trgt_bucket (str): Destination bucket name.
            trgt_key (str): Destination object key.

        Returns:
            None
        """
        try:
            copy_source = {"Bucket": src_bucket, "Key": src_key}
            self.s3.meta.client.copy(copy_source, trgt_bucket, trgt_key)

            if self.logger:
                self.logger.time_execution_step(step_name="copy_s3_object")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=(
                    f"Error copying s3://{src_bucket}/{src_key} "
                    f"to s3://{trgt_bucket}/{trgt_key}"
                ),
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def delete_object(self, bucket: str, key: str) -> None:
        """
        Deletes a specific object from S3.

        Args:
            bucket (str): Bucket containing the object.
            key (str): Object key to delete.

        Returns:
            None
        """
        try:
            self.s3.Object(bucket, key).delete()

            if self.logger:
                self.logger.time_execution_step(step_name="delete_s3_object")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error deleting S3 object at s3://{bucket}/{key}",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def list_objects(self, bucket: str, key: str) -> list:
        """
        Lists all objects under a given S3 prefix.

        Args:
            bucket (str): Bucket to list objects from.
            key (str): Prefix (folder path) to filter results.

        Returns:
            list: List of object metadata dicts, or an empty list if none found.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

            if self.logger:
                self.logger.time_execution_step(step_name="list_s3_objects")

            return response.get("Contents", [])

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error listing S3 objects at s3://{bucket}/{key}",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End S3 Class ##########


########## Athena Class ##########

class Athena(Ses):
    """
    AWS Athena wrapper using boto3 for executing SQL queries.

    Starts query executions asynchronously, polls until completion,
    and returns paginated results as columns and rows.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the Athena boto3 client.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.destination = destination
        self.trgt_tbl = trgt_tbl
        self.logger = logger
        self.env = env

        self.athena_client = boto3.client("athena", region_name="sa-east-1")

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def _available_query(self, execution_id: str) -> None:
        """
        Polls Athena until the given query execution reaches a terminal state.

        Raises an exception if the query fails or is cancelled.

        Args:
            execution_id (str): Athena query execution ID to monitor.

        Returns:
            None

        Raises:
            Exception: If the query state becomes FAILED or CANCELLED.
        """
        state = "RUNNING"

        while state in ("RUNNING", "QUEUED"):
            result = self.athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )
            state = result["QueryExecution"]["Status"]["State"]

            if state in ("FAILED", "CANCELLED"):
                write_error_logs(
                    logger=self.logger,
                    error_msg="Athena query failed or was cancelled.",
                    destination=self.destination,
                    super=self.super_ses,
                    target_tbl=self.trgt_tbl,
                )
                raise Exception("Athena query execution failed.")

            # Avoid tight-loop polling — wait one second between status checks
            time.sleep(1)

    def _get_results(self, execution_id: str) -> tuple:
        """
        Retrieves paginated results from a completed Athena query execution.

        Args:
            execution_id (str): Athena query execution ID.

        Returns:
            tuple: A (columns, data_rows) tuple where columns is a list of
                   column name strings and data_rows is a list of row value lists.
                   Returns ([], []) if no results are found.
        """
        paginator = self.athena_client.get_paginator("get_query_results")
        iterator = paginator.paginate(QueryExecutionId=execution_id)

        rows = []
        column_info = None

        for page in iterator:
            result_set = page.get("ResultSet", None)

            if not result_set:
                continue

            # Capture column metadata from the first page only
            if column_info is None:
                column_info = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]

            for row in result_set.get("Rows", []):
                rows.append([d.get("VarCharValue") for d in row["Data"]])

        if not column_info:
            if self.logger:
                self.logger.add_info("execute_athena_query")
            return [], []

        columns = [c["Name"] for c in column_info]
        # Skip the first row — Athena includes the header row in results
        data_rows = rows[1:]

        if self.logger:
            self.logger.add_info("execute_athena_query")

        return columns, data_rows

    def run_query_athena(self, query: str) -> tuple:
        """
        Executes a SQL query in Athena and returns the results.

        Starts the query execution, waits for it to complete, then
        fetches and returns all paginated results.

        Args:
            query (str): SQL query string to execute in Athena.

        Returns:
            tuple: A (columns, data_rows) tuple — columns is a list of
                   column name strings; data_rows is a list of row value lists.
        """
        output_bucket = f"s3://bws-dl-logs-sae1-{self.env}/athena/query_results/"

        response = self.athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": output_bucket},
        )

        print("Athena query submitted.")

        execution_id = response["QueryExecutionId"]

        self._available_query(execution_id=execution_id)

        print("Athena query completed successfully.")

        columns, data = self._get_results(execution_id=execution_id)

        return columns, data

########## End Athena Class ##########


########## Pyathena Class ##########

class Pyathena(Ses):
    """
    Athena wrapper using the PyAthena library for cursor-based query execution.

    Useful for fetching results directly into Pandas DataFrames.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the PyAthena wrapper.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.destination = destination
        self.trgt_tbl = trgt_tbl
        self.logger = logger
        self.env = env

        print(f"Pyathena env: {self.env}")

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def _connect_pyathena(self):
        """
        Opens a PyAthena cursor connected to the environment's S3 staging directory.

        Returns:
            cursor: PyAthena cursor ready for query execution.
        """
        staging_dir = f"s3://bws-dl-logs-sae1-{self.env}/athena/query_results/"
        print(f"PyAthena staging directory: {staging_dir}")

        cursor = connect(s3_staging_dir=staging_dir).cursor()
        return cursor

    def run_query_athena(self, query: str):
        """
        Executes a SQL query in Athena using PyAthena and returns the raw cursor result.

        Args:
            query (str): SQL query string to execute.

        Returns:
            cursor: PyAthena cursor containing the query results.
        """
        try:
            print("Connecting to Athena via PyAthena...")
            cursor = self._connect_pyathena()

            print("Running query in Athena...")
            response_athena = cursor.execute(query)

            if self.logger:
                self.logger.time_execution_step(step_name="execute_athena_query")

            return response_athena

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error while executing Athena query via PyAthena.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def convert_results_to_df(self, response_athena) -> "pd.DataFrame":
        """
        Converts a PyAthena cursor result into a Pandas DataFrame.

        Args:
            response_athena: PyAthena cursor object returned by run_query_athena.

        Returns:
            pd.DataFrame: DataFrame containing the query result rows and columns.
        """
        try:
            columns = [col[0] for col in response_athena.description]
            results = response_athena.fetchall()

            print("Converting Athena results to DataFrame...")
            df = pd.DataFrame(results, columns=columns)

            if self.logger:
                self.logger.time_execution_step(step_name="convert_query_to_dataframe")

            return df

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error converting Athena results to Pandas DataFrame.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End Pyathena Class ##########


########## Dynamo Class ##########

class Dynamo(Ses):
    """
    AWS DynamoDB wrapper for reading and writing configuration records.

    Used across Glue jobs to retrieve ingestion parameters, quality
    configurations, and notification settings stored in DynamoDB tables.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination=None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the DynamoDB resource.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.logger = logger
        self.destination = destination
        self.env = env
        self.trgt_tbl = trgt_tbl
        self.dynamodb = boto3.resource("dynamodb")

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def get_dynamo_table(self, dynamo_table: str) -> list:
        """
        Scans an entire DynamoDB table and returns all items.

        Args:
            dynamo_table (str): Name of the DynamoDB table to scan.

        Returns:
            list: List of all item dictionaries in the table.
        """
        try:
            table = self.dynamodb.Table(dynamo_table)
            response = table.scan()
            records = response["Items"]

            if self.logger:
                self.logger.time_execution_step(step_name="read_dynamo_table")

            return records

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error scanning DynamoDB table '{dynamo_table}'.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def get_dynamo_records(
        self, dynamo_table: str, id_value: str, id_column: str
    ) -> dict:
        """
        Retrieves a single item from DynamoDB by its primary key.

        Args:
            dynamo_table (str): Name of the DynamoDB table.
            id_value (str): Value of the primary key to look up.
            id_column (str): Name of the primary key attribute.

        Returns:
            dict: Item dictionary, or an empty dict if not found.
        """
        try:
            table = self.dynamodb.Table(dynamo_table)
            response = table.get_item(Key={id_column: id_value})
            records = response.get("Item", {})

            if self.logger:
                self.logger.time_execution_step(step_name="get_dynamo_item")

            return records

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error retrieving item from DynamoDB table '{dynamo_table}'.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def put_dynamo_record(self, dynamo_table: str, records: dict) -> None:
        """
        Inserts or replaces a single item in a DynamoDB table.

        Args:
            dynamo_table (str): Name of the DynamoDB table.
            records (dict): Item to write. Must include the table's primary key.

        Returns:
            None
        """
        try:
            table = self.dynamodb.Table(dynamo_table)
            table.put_item(Item=records)

            if self.logger:
                self.logger.time_execution_step(step_name="put_dynamo_item")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error writing item to DynamoDB table '{dynamo_table}'.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

    def get_email_notif(self, dynamo_notif_params: dict, layer: str) -> tuple:
        """
        Extracts and returns the email notification lists for a given job layer.

        Reads the 'email_on_<layer>' flag from the notification params record.
        If enabled, parses comma-separated email lists for failure, warning,
        and success. Appends the internal escalation address for critical tables.

        Args:
            dynamo_notif_params (dict): DynamoDB item from the 'notification_params' table.
            layer (str): Job layer key (e.g. 'ingestion', 'capture', 'refined').

        Returns:
            tuple: Three lists — (email_on_failure, email_on_warning, email_on_success).
                   All three are empty lists if notifications are disabled for the layer.
        """
        try:
            email_on_layer_key = f"email_on_{layer}"

            if not dynamo_notif_params.get(email_on_layer_key):
                print("Email notifications disabled for this layer.")
                return [], [], []

            print("Email notifications enabled — parsing addresses.")

            # Parse comma-separated email strings; default to empty list if missing
            def _parse_emails(key: str) -> list:
                raw = dynamo_notif_params.get(key, "")
                return raw.split(",") if raw else []

            email_on_failure = _parse_emails("email_on_failure")
            email_on_warning = _parse_emails("email_on_warning")
            email_on_success = _parse_emails("email_on_success")

            # Escalate critical table failures to the internal team address
            if dynamo_notif_params.get("critical") is True:
                print("Critical table — appending escalation address.")
                email_on_failure.append(CRITICAL_EMAIL)
                email_on_warning.append(CRITICAL_EMAIL)

            return email_on_failure, email_on_warning, email_on_success

        except Exception as e:
            print(f"Error parsing email notifications: {e}")

########## End Dynamo Class ##########


########## Ssm Class ##########

class Ssm(Ses):
    """
    AWS SSM Parameter Store wrapper for retrieving encrypted secrets.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the SSM client.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.ssm_client = boto3.client("ssm")
        self.env = env
        self.logger = logger
        self.destination = destination
        self.trgt_tbl = trgt_tbl

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def get_ssm_secret(self, key: str, json: bool = True):
        """
        Retrieves a decrypted parameter from AWS SSM Parameter Store.

        Args:
            key (str): Full SSM parameter name/path.
            json (bool): If True, parses the value as JSON and returns a dict.
                         If False, returns the raw string value.

        Returns:
            dict | str: Parameter value as a dictionary (if json=True)
                        or as a plain string.
        """
        try:
            response = self.ssm_client.get_parameter(Name=key, WithDecryption=True)

            if json:
                # Parameter values stored as JSON strings (e.g. DB credentials)
                ssm_secret = globals()["json"].loads(response["Parameter"]["Value"])  # noqa: S307
            else:
                ssm_secret = response["Parameter"]["Value"]

            if self.logger:
                self.logger.time_execution_step(step_name="get_ssm_parameter")

            return ssm_secret

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg=f"Error retrieving SSM secret for key '{key}'.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End Ssm Class ##########


########## Sns Class ##########

class Sns(Ses):
    """
    AWS SNS wrapper for publishing messages to topics and target ARNs.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the SNS client.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.logger = logger
        self.env = env
        self.sns_client = boto3.client("sns")
        self.destination = destination
        self.job_name = job_name
        self.trgt_tbl = trgt_tbl

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def publish_message(self, arn: str, message: str) -> None:
        """
        Publishes a message to an SNS topic or target ARN.

        Args:
            arn (str): ARN of the SNS topic or endpoint to publish to.
            message (str): Message content to publish.

        Returns:
            None
        """
        try:
            self.sns_client.publish(TargetArn=arn, Message=message)

            if self.logger:
                self.logger.time_execution_step(step_name="publish_sns_message")

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error publishing SNS message.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End Sns Class ##########


########## Sqs Class ##########

class Sqs(Ses):
    """
    AWS SQS wrapper for sending messages to FIFO queues.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the SQS client.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.logger = logger
        self.destination = destination
        self.env = env
        self.trgt_tbl = trgt_tbl
        self.job_name = job_name
        self.sqs_client = boto3.client("sqs")

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    def put_message_queue(self, msg: str, queue_url: str, key: str) -> dict:
        """
        Sends a message to an SQS FIFO queue.

        Uses a fixed MessageGroupId to route messages within the FIFO group,
        and the provided key as the deduplication ID to prevent duplicate
        deliveries within the deduplication window.

        Args:
            msg (str): Message body to send.
            queue_url (str): URL of the SQS FIFO queue.
            key (str): Unique key used as MessageDeduplicationId.

        Returns:
            dict: AWS SQS SendMessage response metadata.
        """
        try:
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=msg,
                MessageGroupId="triggerDag",
                MessageDeduplicationId=key,
            )

            if self.logger:
                self.logger.time_execution_step(step_name="send_sqs_message")

            return response

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error sending message to SQS queue.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End Sqs Class ##########


########## Pandas Class ##########

class Pandas(Ses):
    """
    Pandas utility class for DataFrame casting and CSV reading operations.

    Wraps common transformation patterns used when processing data
    outside of a Spark context.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the Pandas utility wrapper.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.logger = logger
        self.env = env
        self.destination = destination
        self.trgt_tbl = trgt_tbl

        super().__init__(job_name, self.logger, env=self.env)
        self.super_ses = super()

    @staticmethod
    def convert_to_dec(x) -> Decimal:
        """
        Converts a value to a Decimal with two decimal places.

        Returns None for NaN or empty string values to preserve
        nullable semantics in DynamoDB and database writes.

        Args:
            x: Input value to convert (numeric, string, or NaN).

        Returns:
            Decimal | None: Decimal representation rounded to 2 places,
                            or None if the input is null/empty.
        """
        if pd.isna(x) or x == "":
            return None
        return Decimal(f"{float(x):.2f}")

    def read_csv(self, path: str, delimiter: str = ";", header=None) -> "pd.DataFrame":
        """
        Reads a CSV file from a local or S3 path into a Pandas DataFrame.

        Args:
            path (str): File path or S3 URI of the CSV file.
            delimiter (str): Column delimiter character (default ';').
            header: Row number to use as column names, or None for no header.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        return pd.read_csv(path, sep=delimiter, header=header)

    def cast_df(self, df: "pd.DataFrame", schema: dict) -> "pd.DataFrame":
        """
        Casts Pandas DataFrame columns to the types defined in the schema.

        Supports simple types ('string', 'int', 'double', 'decimal(...)')
        and list-based date/timestamp formats with an optional source column
        rename ([type, mask] or [source_col, type, mask]).

        Args:
            df (pd.DataFrame): Input Pandas DataFrame.
            schema (dict): Column-to-type mapping. Values may be:
                - str: simple type name ('string', 'int', 'double', 'decimal(...)')
                - list[2]: [type, mask] for date/timestamp conversion
                - list[3]: [source_col, type, mask] for renamed date/timestamp

        Returns:
            pd.DataFrame: DataFrame with all columns cast to their target types.
        """
        try:
            for column, dt_type in schema.items():
                print(f"Casting column '{column}' to type '{dt_type}'")

                if isinstance(dt_type, list):
                    # List format carries the format mask alongside the type
                    if len(dt_type) == 2:
                        source_col = column
                        dt_type_date, mask = dt_type
                    elif len(dt_type) == 3:
                        source_col, dt_type_date, mask = dt_type

                    if dt_type_date == "timestamp":
                        df[column] = pd.to_datetime(df[source_col], format=mask).dt.to_pydatetime()
                    elif dt_type_date == "date":
                        df[column] = pd.to_datetime(df[source_col], format=mask).dt.date

                elif dt_type == "string":
                    df[column] = df[column].astype(str)

                elif dt_type == "int":
                    # Int64 (nullable) to preserve NaN semantics
                    df[column] = df[column].astype("Int64")

                elif "decimal" in dt_type:
                    df[column] = df[column].apply(self.convert_to_dec)

                elif dt_type == "double":
                    df[column] = df[column].astype(float)

            if self.logger:
                self.logger.time_execution_step(step_name="cast_dataframe_columns")

            return df

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error casting Pandas DataFrame columns.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End Pandas Class ##########


########## Bedrock Class ##########

class Bedrock(Ses):
    """
    AWS Bedrock wrapper for invoking foundation AI models.

    Supports sending user and system prompts and returning the
    model's text response.
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        trgt_tbl: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the Bedrock Runtime client.

        Args:
            job_name (str): Job name used for logging.
            logger: Optional logger instance.
            destination (list): Email addresses for failure notifications.
            trgt_tbl (str): Fully qualified target table name.
            env (str): Deployment environment.

        Returns:
            None
        """
        self.logger = logger
        self.destination = destination
        self.trgt_tbl = trgt_tbl

        super().__init__(job_name, self.logger, env=env)
        self.super_ses = super()

        self.bedrock_runtime = boto3.client("bedrock-runtime")

    def run_prompt(
        self,
        model_id: str,
        prompt: str,
        system_prompt: str = None,
        max_tokens: int = 4096,
        temperature: float = 0.5,
    ) -> str:
        """
        Invokes a Bedrock foundation model with a user prompt and returns the response.

        Optionally prepends a system prompt to set context or persona.
        Temperature controls response creativity — lower values produce more
        deterministic outputs.

        Args:
            model_id (str): Bedrock model identifier
                            (e.g. 'openai.gpt-oss-120b-1:0').
            prompt (str): User message to send to the model.
            system_prompt (str): Optional system-level context or instructions.
            max_tokens (int): Maximum number of tokens in the response (default 4096).
            temperature (float): Sampling temperature between 0 and 1 (default 0.5).

        Returns:
            str: Model response text.
        """
        try:
            messages = []

            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})

            messages.append({"role": "user", "content": prompt})

            request_body = {
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            }

            response = self.bedrock_runtime.invoke_model(
                body=json.dumps(request_body),
                modelId=model_id,
            )

            response_body = json.loads(response.get("body").read())
            result = response_body["choices"][0]["message"]["content"]

            if self.logger:
                self.logger.time_execution_step(step_name="invoke_ai_model")
                self.logger.add_info(user_prompt=prompt, ia_answer=result)

            return result

        except Exception as e:
            write_error_logs(
                logger=self.logger,
                error_msg="Error invoking Bedrock AI model.",
                e=e,
                destination=self.destination,
                super=self.super_ses,
                target_tbl=self.trgt_tbl,
            )

########## End Bedrock Class ##########


########## AwsManager Class ##########

class AwsManager:
    """
    Lazy-initialised facade providing a single entry point to all AWS service classes.

    Each service is instantiated on first access via its property and cached
    for the lifetime of the manager instance. This avoids unnecessary boto3
    client creation when only a subset of services is needed.

    Usage:
        manager = AwsManager(job_name="my_job", logger=logger,
                             destination=emails, target_table="domain.table")
        file_content = manager.s3.get_s3_file(bucket, key)
        params = manager.dynamo.get_dynamo_records("ingestion_params", trgt_tbl, "trgt_tbl")
    """

    def __init__(
        self,
        job_name: str,
        logger=None,
        destination: list = None,
        target_table: str = None,
        env: str = "prd",
    ) -> None:
        """
        Initialises the AwsManager with shared configuration for all services.

        Args:
            job_name (str): Glue/Lambda job name passed to all service instances.
            logger: Optional logger instance shared across services.
            destination (list): Email addresses for failure notifications.
            target_table (str): Fully qualified target table name.
            env (str): Deployment environment ('prd', 'dev', etc.).

        Returns:
            None
        """
        self.job_name = job_name
        self.logger = logger
        self.destination = destination
        self.trgt_tbl = target_table
        self.env = env

        # All service instances are lazily initialised on first property access
        self._dynamo = None
        self._s3 = None
        self._ses = None
        self._pyathena = None
        self._ssm = None
        self._sns = None
        self._sqs = None
        self._pandas = None
        self._bedrock = None
        self._athena = None

        print(f"AwsManager initialised | env: {self.env}")

    @property
    def s3(self) -> S3:
        """
        Lazily initialises and returns the S3 service instance.

        Returns:
            S3: S3 wrapper instance.
        """
        if self._s3 is None:
            self._s3 = S3(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._s3

    @property
    def dynamo(self) -> Dynamo:
        """
        Lazily initialises and returns the DynamoDB service instance.

        Returns:
            Dynamo: DynamoDB wrapper instance.
        """
        if self._dynamo is None:
            self._dynamo = Dynamo(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._dynamo

    @property
    def ses(self) -> Ses:
        """
        Lazily initialises and returns the SES service instance.

        Returns:
            Ses: SES email wrapper instance.
        """
        if self._ses is None:
            self._ses = Ses(
                job_name=self.job_name,
                logger=self.logger,
                env=self.env,
            )
        return self._ses

    @property
    def pyathena(self) -> Pyathena:
        """
        Lazily initialises and returns the PyAthena service instance.

        Returns:
            Pyathena: PyAthena cursor-based query wrapper instance.
        """
        if self._pyathena is None:
            self._pyathena = Pyathena(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._pyathena

    @property
    def ssm(self) -> Ssm:
        """
        Lazily initialises and returns the SSM service instance.

        Returns:
            Ssm: SSM Parameter Store wrapper instance.
        """
        if self._ssm is None:
            self._ssm = Ssm(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._ssm

    @property
    def sns(self) -> Sns:
        """
        Lazily initialises and returns the SNS service instance.

        Returns:
            Sns: SNS topic messaging wrapper instance.
        """
        if self._sns is None:
            self._sns = Sns(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._sns

    @property
    def sqs(self) -> Sqs:
        """
        Lazily initialises and returns the SQS service instance.

        Returns:
            Sqs: SQS queue messaging wrapper instance.
        """
        if self._sqs is None:
            self._sqs = Sqs(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._sqs

    @property
    def pandas(self) -> Pandas:
        """
        Lazily initialises and returns the Pandas utility instance.

        Returns:
            Pandas: Pandas DataFrame utility wrapper instance.
        """
        if self._pandas is None:
            self._pandas = Pandas(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._pandas

    @property
    def bedrock(self) -> Bedrock:
        """
        Lazily initialises and returns the Bedrock AI service instance.

        Returns:
            Bedrock: Bedrock foundation model wrapper instance.
        """
        if self._bedrock is None:
            self._bedrock = Bedrock(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._bedrock

    @property
    def athena(self) -> Athena:
        """
        Lazily initialises and returns the Athena boto3 query instance.

        Returns:
            Athena: Athena boto3 query wrapper instance.
        """
        if self._athena is None:
            self._athena = Athena(
                job_name=self.job_name,
                logger=self.logger,
                destination=self.destination,
                trgt_tbl=self.trgt_tbl,
                env=self.env,
            )
        return self._athena

########## End AwsManager Class ##########