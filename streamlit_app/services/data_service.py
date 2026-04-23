####################################################################
# Author: Bruno William da Silva
# Date: 04/14/2026
#
# Description:
#   Data service for common database query operations.
#   Consolidates shared logic used across multiple analysis modules.
#   Reduces code duplication and improves maintainability.
####################################################################

########### imports ################
from utils.cache_manager import cached_query
from services.athena_service import AthenaService
from config import CACHE_TTL
###################################


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_available_years(
    _athena_service: AthenaService,
    disease: str,
    table_name: str,
    year_column: str = "nr_ano_epi",
) -> list:
    """
    Fetch available years from a specified table for a given disease.
    
    Args:
        _athena_service: Initialized AthenaService instance
        disease: Disease name (e.g., 'dengue', 'chikungunya', 'zika')
        table_name: Table name to query (e.g., tb_ft_alerta_semanal)
        year_column: Column name for year (default: nr_ano_epi)
    
    Returns:
        List of available years sorted in descending order, or [2026] on error
    
    Example:
        years = fetch_available_years(
            athena_service, 
            "dengue", 
            TABLE_ALERTS_WEEKLY,
            "nr_ano_epi"
        )
    """
    query = f"""
    SELECT DISTINCT {year_column}
    FROM {table_name}
    WHERE ds_doenca = '{disease}'
    ORDER BY {year_column} DESC
    """
    
    try:
        df = _athena_service.query_gold(query)
        if df.empty:
            return [2026]
        return sorted(df[year_column].astype(int).tolist(), reverse=True)
    except Exception:
        return [2026]
