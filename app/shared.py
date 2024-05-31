"""
This module defines global shared resources for the application, including
a dictionary to store job information and a lock to ensure thread-safe operations
on this dictionary. The `jobs` dictionary acts as an in-memory database, storing
job IDs as keys and job details as values. The `jobs_lock` is used to synchronize
access to the `jobs` dictionary across multiple threads, preventing data corruption
and ensuring data consistency.
"""

from threading import Lock

# Dictionary to store job information. This will act as an in-memory database
# where job IDs are the keys and the job details are the values.
jobs = {}
# Lock to ensure thread-safe operations on the `jobs` dictionary.
jobs_lock = Lock()
