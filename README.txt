The project entails developing a multi-threaded server in Python to manage
requests using data from a CSV file, which contains statistics on nutrition,
physical activity, and obesity rates in the United States from 2011 to 2022.
The server will compute statistical analyses based on this data, handling
multiple requests simultaneously.

Explanation for the Chosen Solution

The general approach of this implementation focuses on the asynchronous
and concurrent processing of statistics based on public health data.
The system uses Flask to handle HTTP requests and a custom ThreadPool to
execute background calculations. This design allows the server to quickly
respond to requests while intensive processing tasks are efficiently managed
by separate threads.

Strengths:
- Concurrency: Using a ThreadPool to process tasks in parallel.
- Modularity: The code is structured into modules with clear
responsibilities (e.g., DataIngestor for data manipulation).

Weaknesses:
- Complexity: Managing concurrency and shared states increases the
complexity of the code.

Utility:
- I consider the topic extremely useful, providing applied experience
in handling real data and developing an asynchronous web server.

Efficiency:
- The implementation is efficient, but there is room for optimizations,
such as improving exception handling and memory management.

Implementation
Coverage Statement:
- All functionalities required in the homework statement are fully
implemented.

Observations:
- I created a dictionary shared between routes and task_runner to retain
some necessary information for processing (job_id and the type of
information category). This later helps me to answer questions, to
sort them correctly, and to distribute information about the existence
of job_ids.
- I also used the Lock() function to secure the writing of shared data.
- The homework could be done without using the shared dictionary, but I
wanted to practice the concepts of sharing and securing information.
- After processing a result for the type of job_id, with the help of
calculation functions defined in data_ingestor, I create a file named
"job_id.json" and add the result to it.
- Since the file is created after the result is found, we can reduce
complexity and execution times, but it is necessary, when checking in
get_results/job_id, to allow time for processing the information and
creating the file (for the 'running' status).
- I added log messages where necessary to have as much information as
possible when we want to check the execution in relation to the flask server.
- For unittests, I created a .csv with 150 lines taken from the larger
set to verify the correctness of the results accurately.

Resources Used
- https://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-i-hello-world
- https://docs.python.org/3/library/unittest.html
- https://docs.python.org/3/library/logging.html
- https://ocw.cs.pub.ro/courses/asc/laboratoare/02
- https://ocw.cs.pub.ro/courses/asc/laboratoare/03

Git
- https://gitlab.cs.pub.ro/asc/asc-public.git
