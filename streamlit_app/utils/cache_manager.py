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
from datetime import datetime, timedelta
from typing import Any, Callable
from functools import wraps
from utils.config import CACHE_TTL, ENABLE_CACHE
###################################


def get_cache_key(*args, **kwargs) -> str:
    """
    Generate a cache key from function arguments.

    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        str: Cache key string
    """
    key_parts = [str(arg) for arg in args]
    key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
    return "|".join(key_parts)


def cached_query(ttl_seconds: int = CACHE_TTL):
    """
    Decorator for caching query results with TTL support.

    Args:
        ttl_seconds: Time to live in seconds

    Returns:
        Callable: Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if not ENABLE_CACHE:
                return func(*args, **kwargs)
            
            cache_key = f"cache_{func.__name__}_{get_cache_key(*args, **kwargs)}"
            
            if cache_key in st.session_state:
                cached_data = st.session_state[cache_key]
                if cached_data["expires_at"] > datetime.now():
                    return cached_data["value"]
            
            result = func(*args, **kwargs)
            st.session_state[cache_key] = {
                "value": result,
                "expires_at": datetime.now() + timedelta(seconds=ttl_seconds)
            }
            
            return result
        
        return wrapper
    return decorator


def clear_all_caches():
    """Clear all cached values from session state."""
    keys_to_delete = [k for k in st.session_state.keys() if k.startswith("cache_")]
    for key in keys_to_delete:
        del st.session_state[key]
