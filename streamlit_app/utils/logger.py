####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Centralized logging configuration and utilities for the
#   Data Platform Dashboard. Provides standardized logger setup
#   across all application modules.
####################################################################

########### imports ################
import logging
from utils.config import LOG_LEVEL, LOG_FORMAT
###################################


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with standardized configuration.

    Args:
        name: Logger name (typically __name__)

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(LOG_LEVEL)
    
    return logger
