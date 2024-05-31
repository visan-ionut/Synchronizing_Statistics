"""
This module configures a logger for the application with a RotatingFileHandler.
It allows logging messages to be written to a file, with automatic log rotation
based on the file size, ensuring that log files do not grow indefinitely.

The logger setup function provided can be used to initialize a logger with
custom configuration, including the log file name, log level, maximum log file
size, and the number of backup files to retain.
"""
import logging
from logging.handlers import RotatingFileHandler
import time

def logger_setup(name='webserver_logger', log_file='webserver.log',
level=logging.INFO, max_bytes=1e6, backup_count=5):
    """
    Sets up a logger with rotating file handler.

    Parameters:
    - name (str): Name of the logger.
    - log_file (str): File name for the log file.
    - level (int): Logging level, e.g., logging.INFO.
    - max_bytes (int): Maximum size of a log file before it is rolled over.
    - backup_count (int): Number of backup files to keep.

    Returns:
    - logger: Configured logger object.
    """

    # Get or create a logger
    logger = logging.getLogger(name)
    # Set the log level
    logger.setLevel(level)

    # Check if the logger already has a RotatingFileHandler to avoid duplicate handlers
    if not any(isinstance(handler, RotatingFileHandler) for handler in logger.handlers):
        # Define the format for the log messages
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

        # Create a rotating file handler
        handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        handler.setFormatter(formatter)

        # Ensure timestamps in UTC
        logging.Formatter.converter = time.gmtime

        # Add the handler to the logger
        logger.addHandler(handler)
    else:
        pass

    return logger
