####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Cache management utilities for the Data Platform Dashboard.
#   Handles Streamlit session state caching with TTL support for
#   query results and expensive operations.
####################################################################

########### imports ################
import streamlit as st
from typing import Callable
from config import CACHE_TTL, ENABLE_CACHE
###################################


def cached_query(ttl_seconds: int = CACHE_TTL):
    """
    Decorator for caching query results with TTL support.
    Uses Streamlit's native cache_data which persists across page reloads.

    Args:
        ttl_seconds: Time to live in seconds

    Returns:
        Callable: Decorated function
    """
    def decorator(func: Callable) -> Callable:
        if not ENABLE_CACHE:
            return func
        
        # Use Streamlit's native cache_data with TTL
        cached_func = st.cache_data(ttl=ttl_seconds)(func)
        return cached_func
    return decorator


def clear_all_caches():
    """Clear all cached values (works with st.cache_data)."""
    st.cache_data.clear()
