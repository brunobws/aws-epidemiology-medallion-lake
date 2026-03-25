####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Parser service for extracting and normalizing data from JSON-like fields.
#   Particularly handles parsing the "info" column from execution_logs which contains
#   Data Quality test results in JSON format.
#
#   Key Features:
#   - JSON parsing with fallbacks
#   - Data normalization
#   - Error handling and logging
####################################################################

########### imports ################
import json
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from utils.logger import get_logger
###################################

logger = get_logger(__name__)


class ParserService:
    """Service for parsing and normalizing data from various sources."""

    @staticmethod
    def parse_json_field(value: Any, default: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Safely parse JSON-like string to dictionary.

        Args:
            value: Value to parse (string or dict)
            default: Default value if parsing fails

        Returns:
            Parsed dictionary or default
        """
        if value is None:
            return default or {}

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                logger.debug(f"Failed to parse JSON: {value[:100]}")
                return default or {}

        return default or {}

    @staticmethod
    def normalize_dq_info(info_json: str) -> Dict[str, Any]:
        """
        Normalize Data Quality test results from JSON info field.

        Extracts:
        - column_tested: Column being tested
        - test_applied: Type of test (e.g., nullness, uniqueness)
        - status: PASSED, FAILED, WARNING
        - execution_timestamp: When test ran

        Args:
            info_json: JSON string from execution_logs.info column

        Returns:
            Normalized dictionary with DQ test details
        """
        data = ParserService.parse_json_field(info_json)

        result = {
            "column_tested": data.get("column", data.get("column_name", "unknown")),
            "test_applied": data.get("test_type", data.get("test", "unknown")),
            "status": data.get("status", data.get("result", "unknown")).upper(),
            "execution_timestamp": data.get("timestamp", data.get("executed_at", None)),
            "value": data.get("value", None),
            "expected": data.get("expected", None),
            "threshold": data.get("threshold", None),
            "raw": data,
        }

        return result

    @staticmethod
    def extract_dq_tests_from_logs(
        df: pd.DataFrame,
        info_column: str = "info",
    ) -> pd.DataFrame:
        """
        Extract and normalize DQ test results from logs dataframe.

        Creates a new dataframe with parsed DQ information alongside original log data.

        Args:
            df: Execution logs dataframe
            info_column: Column name containing JSON info

        Returns:
            DataFrame with parsed DQ data
        """
        if df.empty or info_column not in df.columns:
            logger.warning(f"Cannot extract DQ tests: empty dataframe or missing '{info_column}'")
            return pd.DataFrame()

        try:
            # Parse info column
            df["dq_info"] = df[info_column].apply(
                lambda x: ParserService.normalize_dq_info(x)
            )

            # Expand parsed info into separate columns
            dq_expanded = pd.DataFrame(
                df["dq_info"].tolist(),
                index=df.index
            )

            # Combine with original data
            result = pd.concat([df, dq_expanded], axis=1)
            logger.info(f"Extracted DQ info from {len(df)} rows")

            return result

        except Exception as e:
            logger.error(f"Error extracting DQ tests: {str(e)}")
            return df

    @staticmethod
    def extract_execution_duration(
        df: pd.DataFrame,
        start_col: str = "start_execution",
        end_col: str = "end_execution",
        output_col: str = "duration_seconds"
    ) -> pd.DataFrame:
        """
        Calculate execution duration from start and end timestamps.

        Args:
            df: Input dataframe
            start_col: Column name for start time
            end_col: Column name for end time
            output_col: Name for output duration column

        Returns:
            DataFrame with duration column added
        """
        if start_col not in df.columns or end_col not in df.columns:
            logger.warning(f"Cannot calculate duration: missing {start_col} or {end_col}")
            return df

        try:
            # Convert to datetime if needed
            start = pd.to_datetime(df[start_col], errors="coerce")
            end = pd.to_datetime(df[end_col], errors="coerce")

            # Calculate duration in seconds
            df[output_col] = (end - start).dt.total_seconds()

            logger.info(f"Calculated execution duration for {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Error calculating duration: {str(e)}")
            return df

    @staticmethod
    def count_by_status(df: pd.DataFrame, status_col: str = "status") -> Dict[str, int]:
        """
        Count executions by status.

        Args:
            df: Input dataframe
            status_col: Status column name

        Returns:
            Dictionary with status counts
        """
        if status_col not in df.columns:
            return {}

        return df[status_col].value_counts().to_dict()

    @staticmethod
    def identify_critical_failures(
        df: pd.DataFrame,
        critical_col: str = "critical_table",
        status_col: str = "status",
        critical_status: str = "FAILED"
    ) -> pd.DataFrame:
        """
        Identify failures in critical tables.

        Args:
            df: Execution logs dataframe
            critical_col: Column indicating if table is critical
            status_col: Status column name
            critical_status: Status value to filter

        Returns:
            DataFrame with critical failures only
        """
        critical_failures = df[
            (df[critical_col] == True) & (df[status_col] == critical_status)
        ]

        logger.info(f"Found {len(critical_failures)} critical failures")
        return critical_failures

    @staticmethod
    def top_failures(
        df: pd.DataFrame,
        status_col: str = "status",
        group_col: str = "column_tested",
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Get top columns/tests with most failures.

        Args:
            df: Dataframe with test results
            status_col: Status column name
            group_col: Column to group by (column_tested, test_applied, etc.)
            limit: Number of top items to return

        Returns:
            DataFrame with failure counts per group
        """
        if status_col not in df.columns or group_col not in df.columns:
            return pd.DataFrame()

        # Find failures (status == "failure" or "FAILED")
        failure_mask = df[status_col].astype(str).str.lower().isin(["failure", "failed"])
        failures = df[failure_mask]

        if failures.empty:
            return pd.DataFrame()

        # Count failures per group
        result = failures.groupby(group_col).size().reset_index(name='failure_count')
        return result.nlargest(limit, 'failure_count')
