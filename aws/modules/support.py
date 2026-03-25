####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
#
# Description:
#   Provides shared utility functions used across Glue jobs and
#   Lambda functions. Includes exception summarisation (handling
#   both Python and PySpark/JVM errors), date/time helpers,
#   target table name parsing, structured error logging with
#   optional SES email notification, and parameter evaluation
#   (converting DynamoDB string values to native Python types).
#
# Usage type:
#   Import individual functions as needed across job scripts.
#   Example: table_name, source = split_target_table(target_table)
#
####################################################################

########## Imports ##########
import traceback
from datetime import datetime, timedelta

try:
    from py4j.protocol import Py4JJavaError
except ImportError:
    # PySpark is not available in all environments — degrade gracefully
    Py4JJavaError = None
########## End Imports ##########


########## Helper Functions ##########

def summarize_exception(e: Exception) -> str:
    """
    Inspects an exception and returns a structured summary string.

    Handles both pure Python exceptions and PySpark/JVM errors
    (Py4JJavaError) transparently. Returns an empty string for
    empty-file sentinel exceptions that should not be logged.

    Args:
        e (Exception): The exception to inspect.

    Returns:
        str: String representation of a structured error summary dict,
             or an empty string if the exception should be suppressed.
    """
    if not e or str(e).lower() == "empty_file":
        return ""

    # Distinguish between Spark/JVM errors and pure Python errors
    if Py4JJavaError and isinstance(e, Py4JJavaError):
        # For Spark errors, extract the root cause from the JVM exception
        real_exception = e.java_exception
        context = "Error originated in the JVM (Spark/Java)"
    else:
        real_exception = e
        context = "Error originated in the Python interpreter"

    # Extract the last frame of the traceback for file/line context
    tb_info = traceback.extract_tb(e.__traceback__)[-1]

    error_summary = {
        "error_type":    type(real_exception).__name__,
        "error_message": str(real_exception),
        "line_number":   tb_info.lineno,
        "context":       context,
    }

    return str(error_summary)


def get_date_and_time() -> str:
    """
    Returns the current date and time adjusted to UTC-3 (Brazil standard time).

    Returns:
        str: Formatted datetime string in 'YYYY-MM-DD HH:MM:SS'.
    """
    date_now = datetime.now() - timedelta(hours=3)
    return date_now.strftime("%Y-%m-%d %H:%M:%S")


def split_target_table(target_table: str) -> tuple:
    """
    Parses a fully qualified table identifier into its table name and source domain.

    The convention is '<source>_<table_name>', where the source is the first
    underscore-delimited segment and the table name is everything after it.

    Args:
        target_table (str): Fully qualified table identifier
                            (e.g. 'breweries_tb_breweries').

    Returns:
        tuple: A (table_name, source) tuple, e.g. ('tb_breweries', 'breweries').
    """
    parts = target_table.split("_")
    table_name = "_".join(parts[1:])
    source = parts[0]
    return table_name, source


def write_error_logs(
    logger,
    error_msg: str = None,
    e: Exception = None,
    destination: list = None,
    super=None,
    target_tbl: str = None,
) -> None:
    """
    Logs a structured error, optionally sends a failure notification email,
    and raises an exception to halt the current job execution.

    Skips the email notification for empty-file sentinel errors
    (where str(e) == 'empty_file') to avoid spurious alerts.

    Args:
        logger: Job logger instance for structured error recording.
        error_msg (str): Human-readable description of the error.
        e (Exception): The original exception that triggered the failure.
        destination (list): Email addresses to notify on failure.
        super: Parent SES instance exposing send_email_on_failure().
        target_tbl (str): Fully qualified target table name for the email body.

    Returns:
        None

    Raises:
        Exception: Always raised with the combined error message and cause.
    """
    # Send failure email unless this is an empty-file sentinel error
    if destination and str(e) != "empty_file":
        super.send_email_on_failure(
            target_table=target_tbl,
            description=error_msg,
            destination=destination,
        )

    # Log the structured error if a logger is available
    if logger:
        error_description = summarize_exception(e=e)
        logger.error(error_msg=error_msg, error_desc=error_description)

    print(f"{error_msg}: returned with error {e}")
    raise Exception(f"{error_msg}: returned with error {e}")


def eval_values(
    value,
    target_tbl: str = None,
    logger=None,
    manager=None,
    destination: list = None,
):
    """
    Evaluates a string parameter value into its native Python type.

    Converts DynamoDB-stored string representations ('true'/'false',
    dicts, lists, etc.) into proper Python objects using eval().
    Non-string values are returned unchanged.

    Args:
        value: The value to evaluate. If not a string, returned as-is.
        target_tbl (str): Fully qualified target table name for error emails.
        logger: Job logger instance for error recording.
        manager: AwsManager instance used to send failure notification emails.
        destination (list): Email addresses to notify on parsing failure.

    Returns:
        The evaluated native Python value, or the original value unchanged
        if it is not a string or does not require conversion.

    Raises:
        Exception: Propagates a parsing failure after logging and notification.
    """
    try:
        if value and isinstance(value, str):
            # Handle boolean string representations explicitly before eval()
            if value.lower() == "false":
                return False
            elif value.lower() == "true":
                return True

            # Safely evaluate remaining string types (dicts, lists, numbers, etc.)
            return eval(value)  # noqa: S307 — controlled internal usage

        return value

    except Exception as e:
        error_summary = summarize_exception(e=e)

        if logger:
            logger.error("Parsing error", error_summary)

        if manager and destination:
            manager.ses.send_email_on_failure(
                target_table=target_tbl,
                description="Error while processing parameter value.",
                destination=destination,
            )

        raise Exception(f"Parsing error while evaluating value '{value}': {e}")

########## End Helper Functions ##########