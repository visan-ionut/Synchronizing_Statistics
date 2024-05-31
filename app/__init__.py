"""
This module initializes the Flask web application, including setting up a
ThreadPool for task processing and a DataIngestor instance for managing
data related to nutrition, activity, and obesity in the USA.
"""
import threading
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

# Initialize the Flask application
webserver = Flask(__name__)
# Initialize a ThreadPool instance and assign it to the Flask application.
# This allows the ThreadPool to be accessed globally across the application,
# enabling task processing in a multi-threaded environment.
webserver.tasks_runner = ThreadPool()

# webserver.task_runner.start()

# Initialize a DataIngestor instance with the path to the dataset.
# The DataIngestor is responsible for reading and processing the CSV data.
# This instance is also attached to the Flask application for global access,
# making it available for route handlers that need to query or manipulate the data.
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

# Initialize a job counter starting from 1.
# This counter will be used to assign a unique ID to each job that is submitted to the application.
# The ID allows tracking and retrieving the status or result of the job at a later time.
webserver.job_counter = 1

from app import routes # pylint: disable=wrong-import-position
