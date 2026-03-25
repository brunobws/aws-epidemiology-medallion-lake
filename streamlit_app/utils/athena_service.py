####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Fixed AWS Athena service with proper query execution flow.
#   Handles connection, query execution, result retrieval, and error handling.
#   Critical fix: Properly waits for query completion before fetching results.
#
#   Key Features:
#   - Proper async query execution flow
#   - Status polling with timeout
#   - Paginated result retrieval
#   - Health checks and error handling
####################################################################

########### imports ################
import boto3
import pandas as pd
from typing import Optional, Dict, Any
from time import sleep
from datetime import datetime
from utils.logger import get_logger
from utils.config import (
    AWS_REGION, ATHENA_DATABASE, ATHENA_LOGS_DATABASE,
    ATHENA_S3_OUTPUT, ATHENA_QUERY_TIMEOUT_SECONDS
)
###################################

logger = get_logger(__name__)


class AthenaService:
    """
    Service for executing queries against AWS Athena.

    Implements proper async query execution with:
    - Query submission (start_query_execution)
    - Status polling (wait for SUCCEEDED)
    - Result fetching (get_query_results with pagination)
    - Comprehensive error handling and logging
    """

    def __init__(
        self,
        database: str = ATHENA_DATABASE,
        logs_database: str = ATHENA_LOGS_DATABASE,
        s3_output_location: str = ATHENA_S3_OUTPUT,
        region: str = AWS_REGION,
        timeout_seconds: int = ATHENA_QUERY_TIMEOUT_SECONDS,
    ):
        """
        Initialize Athena service.

        Args:
            database: Primary database name
            logs_database: Logs database name
            s3_output_location: S3 path for query results
            region: AWS region
            timeout_seconds: Query timeout in seconds
        """
        self.database = database
        self.logs_database = logs_database
        self.s3_output_location = s3_output_location
        self.region = region
        self.timeout_seconds = timeout_seconds
        self.client = boto3.client("athena", region_name=region)
        
        logger.info(f"Athena service initialized: database={database}, region={region}")

    def _submit_query(
        self,
        query: str,
        database: str,
        execution_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Submit a query to Athena for execution.

        Args:
            query: SQL query string
            database: Database to execute against
            execution_context: Optional additional execution context

        Returns:
            Query execution ID

        Raises:
            Exception: If query submission fails
        """
        context = execution_context or {}
        context["Database"] = database

        try:
            logger.info(f"Submitting query to database={database}")
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext=context,
                ResultConfiguration={"OutputLocation": self.s3_output_location}
            )
            
            execution_id = response["QueryExecutionId"]
            logger.info(f"Query submitted: execution_id={execution_id}")
            return execution_id

        except Exception as e:
            logger.error(f"Query submission failed: {str(e)}")
            raise

    def _wait_for_query_completion(self, execution_id: str) -> Dict[str, Any]:
        """
        Poll Athena until query completes (SUCCEEDED, FAILED, or CANCELLED).

        This is THE critical fix for the bug: properly waiting before fetching results.

        Args:
            execution_id: Query execution ID

        Returns:
            Query execution details

        Raises:
            TimeoutError: If query doesn't complete within timeout
            RuntimeError: If query fails or is cancelled
        """
        start_time = datetime.now()
        poll_count = 0

        while True:
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if elapsed > self.timeout_seconds:
                logger.error(f"Query timeout after {elapsed}s")
                raise TimeoutError(
                    f"Query did not complete within {self.timeout_seconds} seconds"
                )

            try:
                response = self.client.get_query_execution(QueryExecutionId=execution_id)
                query_execution = response["QueryExecution"]
                state = query_execution["Status"]["State"]
                poll_count += 1

                logger.info(f"Query status poll {poll_count}: state={state}, elapsed={elapsed:.1f}s")

                if state == "SUCCEEDED":
                    logger.info(f"Query succeeded after {elapsed:.1f}s and {poll_count} polls")
                    return query_execution

                elif state == "FAILED":
                    reason = query_execution["Status"].get("StateChangeReason", "Unknown error")
                    logger.error(f"Query failed: {reason}")
                    raise RuntimeError(f"Query failed: {reason}")

                elif state == "CANCELLED":
                    logger.error("Query was cancelled")
                    raise RuntimeError("Query was cancelled")

                # Still running, wait before next poll
                sleep(1)

            except Exception as e:
                if isinstance(e, (TimeoutError, RuntimeError)):
                    raise
                logger.error(f"Error checking query status: {str(e)}")
                raise

    def _fetch_results(self, execution_id: str) -> pd.DataFrame:
        """
        Fetch results for a completed query using pagination.

        Args:
            execution_id: Query execution ID (must be SUCCEEDED)

        Returns:
            DataFrame with query results

        Raises:
            Exception: If result fetching fails
        """
        try:
            logger.info(f"Fetching results for execution_id={execution_id}")
            
            # Use paginator for large result sets
            paginator = self.client.get_paginator("get_query_results")
            page_iterator = paginator.paginate(QueryExecutionId=execution_id)

            all_rows = []
            column_names = None
            page_count = 0

            for page in page_iterator:
                page_count += 1
                rows = page.get("ResultSet", {}).get("Rows", [])
                
                logger.info(f"Processing page {page_count} with {len(rows)} rows")

                for row in rows:
                    data_cells = row.get("Data", [])
                    row_data = []

                    for cell in data_cells:
                        # Extract value from cell
                        if "VarCharValue" in cell:
                            row_data.append(cell["VarCharValue"])
                        else:
                            row_data.append(None)

                    # First row is column headers
                    if column_names is None:
                        column_names = row_data
                        logger.info(f"Found {len(column_names)} columns")
                    else:
                        all_rows.append(row_data)

            if not all_rows:
                logger.warning("Query returned no data rows")
                return pd.DataFrame()

            # Create DataFrame
            df = pd.DataFrame(all_rows, columns=column_names)
            logger.info(f"Successfully fetched {len(df)} rows × {len(df.columns)} columns")
            
            return df

        except Exception as e:
            logger.error(f"Error fetching results: {str(e)}")
            raise

    def execute_query(self, query: str, database: Optional[str] = None) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.

        This is the main public method that orchestrates the entire flow:
        1. Submit query
        2. Wait for completion
        3. Fetch results
        4. Return DataFrame

        Args:
            query: SQL query to execute
            database: Optional database (defaults to self.database)

        Returns:
            DataFrame with query results

        Raises:
            TimeoutError: If query takes too long
            RuntimeError: If query fails
            Exception: For other errors
        """
        database = database or self.database
        
        try:
            # Step 1: Submit query
            execution_id = self._submit_query(query, database)
            
            # Step 2: Wait for completion (THIS WAS THE BUG FIX)
            self._wait_for_query_completion(execution_id)
            
            # Step 3: Fetch results
            df = self._fetch_results(execution_id)
            
            return df

        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise

    def query_gold(self, query: str) -> pd.DataFrame:
        """Execute query against gold database."""
        return self.execute_query(query, database=self.database)

    def query_logs(self, query: str) -> pd.DataFrame:
        """Execute query against logs database."""
        return self.execute_query(query, database=self.logs_database)

    def health_check(self) -> bool:
        """
        Check if Athena service is accessible.

        Returns:
            True if accessible, False otherwise
        """
        try:
            self.client.list_query_executions(MaxResults=1)
            logger.info("Athena health check passed")
            return True
        except Exception as e:
            logger.error(f"Athena health check failed: {str(e)}")
            return False
