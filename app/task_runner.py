"""
This module defines the ThreadPool and TaskRunner classes for managing and
processing asynchronous tasks in a multi-threaded environment. The ThreadPool
class creates a pool of worker threads (TaskRunners) that can execute tasks
concurrently. Tasks are represented as jobs that are placed in a queue and
are processed by the TaskRunners.

The TaskRunner class extends the Thread class and is responsible for continuously
processing jobs from the queue. It executes the appropriate processing function
based on the job type and saves the results to a file upon completion.

This module also integrates with the DataIngestor class to process various types
of data analysis tasks related to health-related data from a CSV file.
"""
import time
import os
import multiprocessing
import json
from queue import Queue, Empty
from threading import Thread
from .shared import jobs
from .data_ingestor import DataIngestor
from .logger_config import logger_setup

# Initialize logger from logger setup function
logger = logger_setup()

class ThreadPool:
    """
    A ThreadPool class that manages a pool of worker threads for processing tasks.
    """
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task

        # Create a queue for jobs that will be processed by the worker threads
        self.jobs_queue = Queue()
        # Determine the number of worker threads: either from an environment variable or the
        # system's CPU count
        num_threads = int(os.getenv('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))
        # Initialize the DataIngestor instance that will be shared among all workers
        data_ingestor_instance = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
        # Create worker threads (TaskRunners) and start them
        self.workers = [TaskRunner(self.jobs_queue, data_ingestor_instance) for _ in
        range(num_threads)]
        # Control flag to allow or disallow new jobs
        self.accept_jobs = True

        logger.info("Initializing ThreadPool with %s threads.", num_threads)

        for worker in self.workers:
            worker.start()

    def add_job(self, job_id, data):
        """
        Adds a new job to the queue if accepting new jobs.
        """
        if self.accept_jobs:
            self.jobs_queue.put((job_id, data))

            logger.info("Job %s added to the queue.", job_id)
        else:
            logger.info("Shutdown in progress. Not accepting new jobs.")

    def shutdown(self):
        """
        Stops accepting new jobs and waits for all current jobs to finish.
        """
        logger.info("Initiating shutdown of ThreadPool.")

        self.accept_jobs = False
        for worker in self.workers:
            worker.join()

        logger.info("ThreadPool has been successfully shut down.")

    def get_num_jobs(self):
        """
        Returns the number of jobs currently in the queue.
        """
        return self.jobs_queue.qsize()

class TaskRunner(Thread):
    """
    A worker thread that processes tasks from the jobs queue.
    """
    def __init__(self, jobs_queue, data_ingestor):
        super().__init__()
        self.jobs_queue = jobs_queue
        self.data_ingestor = data_ingestor
        self.daemon = True
        self.results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'results'))

        os.makedirs(self.results_dir, exist_ok=True)

    def save_result_to_file(self, job_id, result):
        """
        Saves the job result to a JSON file in the results directory.
        """
        # Construct the full path where the result file will be saved, using the job ID to name
        # the file.
        file_path = os.path.join(self.results_dir, f"{job_id}.json")
        try:
            # Attempt to open the file in write mode. If the file doesn't exist, it will be created.
            with open(file_path, 'w', encoding='utf-8') as json_file:
                # Serialize the 'result' dictionary to a JSON formatted string and write it into
                # the file.
                json.dump(result, json_file)
            logger.info("Result for job %s saved to %s.", job_id, file_path)
        except (OSError, IOError) as e:
            logger.error("Failed to save result for job %s to file. Error: %s", job_id, e)

    def run(self):
        """
        Continuously processes jobs from the queue until it is empty.
        """
        while True:
            # Record start time of the job processing
            job_start_time = time.time()
            try:
                # Attempt to get a job from the queue
                job_id, job_data = self.jobs_queue.get(timeout=120)
                logger.info("Starting to process job {job_id} with data {job_data}")

            except Empty:
                # If the queue is empty for 120 seconds, stop the worker
                logger.info("Queue is empty, TaskRunner is stopping.")
                break

            # Processing logic for different job types
            job_details = jobs.get(job_id)
            if not job_details:
                logger.error("Job %s not found in the job list.", job_id)

                self.jobs_queue.task_done()
                continue

            # Get the informations for processing the result
            job_type = job_details['type']
            question = job_data.get('question')
            state_name = job_data.get('state')

            try:
                result = None
                # Process the result for each job_type and store in result
                # More details for the job_type function in data_ingestor
                if job_type == 'states_mean':
                    result = self.data_ingestor.calculate_state_means_and_sort(question)
                elif job_type == 'state_mean':
                    result = self.data_ingestor.calculate_mean_for_state(question, state_name)
                elif job_type == 'best5':
                    result = self.data_ingestor.calculate_best5_for_question(question)
                elif job_type == 'worst5':
                    result = self.data_ingestor.calculate_worst5_for_question(question)
                elif job_type == 'global_mean':
                    result = self.data_ingestor.calculate_global_mean_for_question(question)
                elif job_type == 'diff_from_mean':
                    result = self.data_ingestor.calculate_diff_from_mean(question)
                elif job_type == 'state_diff_from_mean':
                    result = self.data_ingestor.calculate_state_diff_from_global_mean(question,
                    state_name)
                elif job_type == 'mean_by_category':
                    result = self.data_ingestor.calculate_mean_by_category(question)
                elif job_type == 'state_mean_by_category':
                    result = self.data_ingestor.calculate_state_mean_by_category(question,
                    state_name)

                if result is not None:
                    # Save the result in a file from results
                    self.save_result_to_file(job_id, result)
                    # Record and log processing time
                    job_end_time = time.time()
                    time_taken = job_end_time - job_start_time

                    logger.info("Successfully processed job %s in %.2f seconds.",
                    job_id, time_taken)
                else:
                    logger.error("Failed to process job %s due to an unknown job type: %s",
                    job_id, job_type)
            except (OSError, IOError) as e:
                logger.error("An error occurred while processing job %s: %s", job_id, e)
            finally:
                # Mark the job as done
                self.jobs_queue.task_done()
