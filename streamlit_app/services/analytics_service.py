####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Analytics service for data aggregations and calculations.
#   Provides business logic for metrics, KPIs, and derived analytics.
#
#   Key Features:
#   - KPI calculations
#   - Data aggregations
#   - Derived metrics
#   - Performance analysis
####################################################################

########### imports ################
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
from utils.logger import get_logger
###################################

logger = get_logger(__name__)


class AnalyticsService:
    """Service for analytical calculations and aggregations."""

    @staticmethod
    def calculate_kpis(df: pd.DataFrame, metric_configs: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Calculate KPIs based on configuration.

        Args:
            df: Input dataframe
            metric_configs: Dictionary with metric definitions

        Returns:
            Dictionary with calculated KPI values
        """
        kpis = {}

        for metric_name, config in metric_configs.items():
            try:
                column = config.get("column")
                operation = config.get("operation", "count")
                label = config.get("label", metric_name)

                if operation == "count":
                    kpis[label] = len(df)
                elif operation == "sum":
                    kpis[label] = df[column].sum() if column in df.columns else 0
                elif operation == "avg":
                    kpis[label] = df[column].mean() if column in df.columns else 0
                elif operation == "max":
                    kpis[label] = df[column].max() if column in df.columns else 0
                elif operation == "min":
                    kpis[label] = df[column].min() if column in df.columns else 0
                elif operation == "unique":
                    kpis[label] = df[column].nunique() if column in df.columns else 0
                else:
                    kpis[label] = None

            except Exception as e:
                logger.warning(f"Error calculating KPI {metric_name}: {str(e)}")
                kpis[label] = None

        return kpis

    @staticmethod
    def success_rate(df: pd.DataFrame, status_col: str = "status", success_value: str = "SUCCEEDED") -> float:
        """
        Calculate success rate as percentage.

        Args:
            df: Input dataframe
            status_col: Status column name
            success_value: Value indicating success

        Returns:
            Success rate as percentage (0-100)
        """
        if df.empty or status_col not in df.columns:
            return 0.0

        successful = len(df[df[status_col] == success_value])
        total = len(df)

        return (successful / total * 100) if total > 0 else 0.0

    @staticmethod
    def group_by_aggregation(
        df: pd.DataFrame,
        group_cols: List[str],
        agg_col: str,
        agg_func: str = "count",
        sort: bool = True,
        ascending: bool = False,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Group data and apply aggregation.

        Args:
            df: Input dataframe
            group_cols: Columns to group by
            agg_col: Column to aggregate
            agg_func: Aggregation function (count, sum, mean, max, min)
            sort: Whether to sort results
            ascending: Sort order
            limit: Limit number of results

        Returns:
            Aggregated dataframe
        """
        if df.empty or not all(col in df.columns for col in group_cols):
            return pd.DataFrame()

        try:
            # Handle count case (no agg_col needed)
            if agg_func == "count":
                result = df.groupby(group_cols, as_index=False).size()
                result.columns = group_cols + ["count"]
            else:
                if agg_col not in df.columns:
                    logger.warning(f"Column '{agg_col}' not found for aggregation")
                    return pd.DataFrame()

                agg_methods = {
                    "sum": "sum",
                    "mean": "mean",
                    "avg": "mean",
                    "max": "max",
                    "min": "min",
                }

                result = df.groupby(group_cols, as_index=False)[agg_col].agg(
                    agg_methods.get(agg_func, "sum")
                )
                result.columns = group_cols + [agg_func]

            # Sort if requested
            if sort:
                sort_col = result.columns[-1]
                result = result.sort_values(sort_col, ascending=ascending)

            # Limit if requested
            if limit:
                result = result.head(limit)

            logger.info(f"Aggregated {len(df)} rows into {len(result)} groups")
            return result

        except Exception as e:
            logger.error(f"Error in aggregation: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def time_series_aggregation(
        df: pd.DataFrame,
        date_col: str,
        agg_col: str,
        agg_func: str = "count",
        freq: str = "D"
    ) -> pd.DataFrame:
        """
        Create time series aggregation by day, week, or month.

        Args:
            df: Input dataframe
            date_col: Date/timestamp column name
            agg_col: Column to aggregate
            agg_func: Aggregation function
            freq: Frequency ('D' for day, 'W' for week, 'M' for month)

        Returns:
            Time series dataframe with date index
        """
        if df.empty or date_col not in df.columns:
            return pd.DataFrame()

        try:
            # Convert to datetime
            df_copy = df.copy()
            df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors="coerce")

            # Handle count case
            if agg_func == "count":
                result = df_copy.groupby(pd.Grouper(key=date_col, freq=freq)).size()
                result = result.reset_index()
                result.columns = [date_col, "count"]
            else:
                if agg_col not in df_copy.columns:
                    logger.warning(f"Column '{agg_col}' not found")
                    return pd.DataFrame()

                agg_methods = {
                    "sum": "sum",
                    "mean": "mean",
                    "avg": "mean",
                    "max": "max",
                    "min": "min",
                }

                result = df_copy.groupby(pd.Grouper(key=date_col, freq=freq))[agg_col].agg(
                    agg_methods.get(agg_func, "sum")
                )
                result = result.reset_index()
                result.columns = [date_col, agg_func]

            logger.info(f"Created time series with {len(result)} periods")
            return result

        except Exception as e:
            logger.error(f"Error in time series aggregation: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def percentile_calculation(
        df: pd.DataFrame,
        column: str,
        percentiles: List[float] = [0.25, 0.5, 0.75, 0.95]
    ) -> Dict[str, float]:
        """
        Calculate percentiles for a numeric column.

        Args:
            df: Input dataframe
            column: Column name
            percentiles: List of percentiles (0-1)

        Returns:
            Dictionary with percentile values
        """
        if df.empty or column not in df.columns:
            return {}

        try:
            result = {}
            for p in percentiles:
                result[f"p{int(p*100)}"] = df[column].quantile(p)

            return result

        except Exception as e:
            logger.error(f"Error calculating percentiles: {str(e)}")
            return {}

    @staticmethod
    def top_failures(
        df: pd.DataFrame,
        status_col: str = "status",
        group_col: Optional[str] = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Identify top failing items.

        Args:
            df: Input dataframe
            status_col: Status column name
            group_col: Column to group failures by
            limit: Top N results

        Returns:
            DataFrame with failure counts
        """
        if df.empty:
            return pd.DataFrame()

        try:
            failures = df[df[status_col] != "SUCCEEDED"]

            if failures.empty:
                return pd.DataFrame()

            if group_col and group_col in df.columns:
                result = failures.groupby(group_col, as_index=False).size()
                result.columns = [group_col, "failure_count"]
                result = result.sort_values("failure_count", ascending=False).head(limit)
            else:
                result = pd.DataFrame({
                    "failure_count": [len(failures)]
                })

            logger.info(f"Identified {len(result)} failure groups")
            return result

        except Exception as e:
            logger.error(f"Error identifying failures: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    def filter_by_date_range(
        df: pd.DataFrame,
        date_col: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Filter dataframe by date range.

        Args:
            df: Input dataframe
            date_col: Date column name
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Filtered dataframe
        """
        if df.empty or date_col not in df.columns:
            return df

        try:
            df_copy = df.copy()
            df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors="coerce")

            if start_date:
                df_copy = df_copy[df_copy[date_col] >= start_date]

            if end_date:
                df_copy = df_copy[df_copy[date_col] <= end_date]

            logger.info(f"Filtered to {len(df_copy)} rows by date range")
            return df_copy

        except Exception as e:
            logger.error(f"Error filtering by date: {str(e)}")
            return df
