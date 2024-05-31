"""
This module defines the endpoints for a Flask web application, including 
routes for job management and data processing requests.
"""
import os
import json
from flask import request, jsonify
from app import webserver
from .shared import jobs, jobs_lock
from .logger_config import logger_setup

# Initialize logger from logger setup function
logger = logger_setup()

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """
    Endpoint for gracefully shutting down the server.
    """
    logger.info("Received request (GET) for graceful shutdown.")

    webserver.tasks_runner.shutdown()
    return jsonify({'message': 'Shutting down...'}), 200

@webserver.route('/api/jobs', methods=['GET'])
def list_jobs():
    """
    Endpoint to list all jobs based on the presence and size of result
    files in the /results directory.
    """
    logger.info("Received request to list all jobs.")

    # Determine the path to the directory where results are stored
    results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'results'))
    # Initialize a list to hold the status of each job
    jobs_status = []

    try:
        # Iterate over each file in the results directory
        for filename in os.listdir(results_dir):
            # Check if the file is a JSON file, indicating a job result
            if filename.endswith(".json"):
                 # Construct the full path to the file
                file_path = os.path.join(results_dir, filename)
                # Extract the job ID from the filename (removing the '.json' extension)
                job_id = filename[:-5]

                # Determine the job's status based on the file size
                if os.path.getsize(file_path) > 0:
                    status = "done"
                else:
                    status = "running"
                # Append the job's status to the list
                jobs_status.append({"job_id": job_id, "status": status})

        logger.info("Jobs status response prepared with %s entries.", len(jobs_status))

        # Prepare and return the response with the status of all jobs
        response = {
            "status": "success",
            "data": jobs_status
        }
        return jsonify(response), 200

    except (OSError, IOError) as e:
        logger.error("Failed to list jobs due to an error: %s.", e)
        return jsonify({"status": "error", "reason": "Unable to list jobs at this time"}), 500

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    """
    Endpoint to get the number of remaining jobs in the queue.
    """
    logger.info("Received request (GET) to get the number of remaining jobs.")

    num_remaining_jobs = webserver.tasks_runner.get_num_jobs()

    logger.info("Number of remaining jobs: %s.", num_remaining_jobs)

    return jsonify({"num_jobs": num_remaining_jobs})

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """
    Endpoint to get the results of a specific job by its ID.
    """
    logger.info("Received request (GET) for job results - JobID: %s.", job_id)

    # Define the directory where job results are stored
    results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'results'))
    # Construct the path to the specific job's results file
    file_path = os.path.join(results_dir, f"{job_id}.json")

    # Check if the job ID exists in the system
    job = jobs.get(job_id)

    if job is None:
        logger.info("Invalid job ID: %s.", job_id)

        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        }), 404

    try:
        # Check if the job's results file exists and is not empty
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # Open and read the job's results file
            with open(file_path, 'r', encoding='utf-8') as file:
                content = json.load(file)
                logger.info("Job %s is done.", job_id)

                return jsonify({
                    "status": "done",
                    "data": content
                }), 200

        else:
            # If the results file does not exist or is empty, indicate that the job is still running
            logger.info("Job %s is still running or the result file is not yet available.", job_id)

            return jsonify({
                "status": "running"
            }), 200

    except json.JSONDecodeError:
        # Handle cases where the results file is invalid or corrupt
        logger.info("Job %s has an invalid or corrupt result file.", job_id)

        return jsonify({
            "status": "error",
            "reason": "Invalid or corrupt result file"
        }), 500

    except (OSError, IOError) as e:
        # Handle any unexpected errors during the process
        logger.error("Unexpected error occurred for Job %s: %s", job_id, e)

        return jsonify({
            "status": "error",
            "reason": "Unexpected error occurred"
        }), 500

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    Endpoint to post/add a job for states_mean_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for states_mean with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'states_mean'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    Endpoint to post/add a job for state_mean_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for state_mean with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'state_mean'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    Endpoint to post/add a job for best5_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for best5 with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'best5'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    Endpoint to post/add a job for worst5_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for worst5 with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'worst5'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    Endpoint to post/add a job for global_mean_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for global_mean with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'global_mean'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    Endpoint to post/add a job for diff_from_mean_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for diff_from_mean with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'diff_from_mean'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    Endpoint to post/add a job for state_diff_from_mean_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for state_diff_from_mean with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'state_diff_from_mean'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    Endpoint to post/add a job for mean_by_category_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for mean_by_category with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'mean_by_category'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    Endpoint to post/add a job for state_mean_by_category_requests.
    Specific details in data_ingestor.
    """
    # Get request data
    data = request.json
    logger.info("-------------------")
    logger.info("Received request (POST) for state_mean_by_category with data: %s", data)

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1
    logger.info("Registering job with ID: %s", job_id)

    # Complete dictionary
    with jobs_lock:
        jobs[job_id] = {'type': 'state_mean_by_category'}

    # Add job in queue
    webserver.tasks_runner.add_job(job_id, data)

    # Return associated job_id
    return jsonify({'job_id': job_id}), 200

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    """
    Home page route that displays a welcome message and a list of all defined routes 
    and their HTTP methods available in the webserver. Each route is displayed within 
    an HTML paragraph tag.
    
    Returns:
        A string of HTML content displaying the welcome message and available routes.
    """
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n" # pylint: disable=f-string-without-interpolation

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>" # pylint: disable=consider-using-join

    msg += paragraphs
    return msg

def get_defined_routes():
    """
    Gathers and formats all the routes defined in the webserver, including their HTTP 
    methods, into a list of strings.
    
    Returns:
        A list of strings, each representing a defined route and its methods in the 
        format "Endpoint: '{route}' Methods: '{methods}'".
    """
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
