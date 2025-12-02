DISTRIBUTED TEXT ANALYSIS SYSTEM ON AWS
=======================================
Students: 
1. [Your Name] (ID: [Your ID])
2. [Partner Name] (ID: [Partner ID])

--- SUBMISSION DETAILS ---
AMI ID Used:        ami-00e95a9222311e8ed (Ubuntu 24.04 LTS / Compatible)
Manager Type:       t3.medium
Worker Type:        t3.medium
Run Statistics:
   - Input File:    [Name of file, e.g., input1.txt]
   - Value of n:    [e.g., 2] (Files per worker ratio)
   - Total Time:    [e.g., 5 minutes, 30 seconds]
   - Number of Workers Launched: [e.g., 8]

--- HOW TO RUN ---
1. Build the project using Maven to create the artifacts (shades/fat jars).
2. Ensure your AWS credentials are configured in `~/.aws/credentials` or environment variables.
3. Run the Local Application from the terminal:

   java -jar text-analysis-local.jar inputFileName outputFileName n [terminate]

   Example:
   java -jar text-analysis-local.jar input.txt output.html 2 terminate

   * inputFileName: Path to the local file containing URLs.
   * outputFileName: Desired name for the result HTML.
   * n: Number of files per worker.
   * terminate (optional): If present, the Manager terminates after the job.

--- SYSTEM ARCHITECTURE ---
Our system is composed of three main components communicating via SQS and S3:

1. Local Application:
   - Uploads the input file to S3.
   - Checks for an active Manager. If not found, it launches a new EC2 instance (t3.medium) with a User Data script that downloads the Manager JAR from S3 and starts it.
   - Sends a "NEW_JOB" message to the ManagerQueue.
   - Listens on a private LocalAppQueue for the final "SUMMARY" message.

2. The Manager:
   - Polls the ManagerQueue for jobs.
   - Downloads the input file, parses it into individual tasks (URL + Analysis Type).
   - Calculates the required number of workers based on `n` (Tasks / n) and scales up EC2 instances (up to a limit of 17 workers to avoid AWS bans).
   - Sends tasks to the WorkerQueue.
   - Listens on the WorkerResultsQueue. It uses a `ConcurrentHashMap` to track pending tasks.
   - If a worker crashes or the message is lost (timeout), the Manager detects the missing task ID and reports it as an error in the final HTML, ensuring persistence.
   - Aggregates results into an HTML file, uploads it to S3, and notifies the Local App.

3. The Workers:
   - Poll the WorkerQueue for tasks.
   - Download the text from the given URL.
   - Run the Stanford CoreNLP pipeline (POS, Constituency, or Dependency).
   - Upload the resulting text analysis to S3.
   - Send a "RESULT" or "ERROR" message to the WorkerResultsQueue.
   - Catches exceptions (e.g., parsing errors) and reports them without crashing.

--- CONSIDERATIONS ---

1. SCALABILITY:
   - The Manager creates workers dynamically based on the workload.
   - We use `t3.medium` instances to ensure sufficient RAM (4GB) for the Stanford Parser, preventing `OutOfMemoryErrors` that occurred with micro instances.
   - The Manager uses an ExecutorService to handle multiple incoming messages in parallel (though the assignment flow effectively processes one job at a time per manager logic, the structure supports concurrency).

2. PERSISTENCE & FAULT TOLERANCE:
   - If a Worker node dies, the message visibility timeout in SQS would eventually allow another worker to pick it up (AWS SQS behavior).
   - The Manager implements a logical timeout (40 minutes). If a result isn't received by then, it marks the specific task as "Crashed/Timed Out" in the final output rather than hanging indefinitely.
   - If the Local App crashes, the Manager and Workers continue running. The Manager will store the result in S3 regardless.

3. SECURITY:
   - No credentials are hardcoded in the source code.
   - We use `DefaultCredentialsProvider`, which securely fetches credentials from the instance metadata (IAM Roles) when running on EC2, or local config when running locally.
   - IAM Roles are used for EC2 instances ("LabInstanceProfile").

4. THREADS:
   - Manager uses a ThreadPool to process SQS messages to ensure it doesn't block while waiting for S3 I/O.
   - Workers are single-threaded per instance to maximize the CPU/RAM available for the heavy NLP operation on that specific node.

5. TERMINATION:
   - If the `terminate` argument is provided, the Local App sends a TERMINATE message.
   - The Manager waits for all active jobs to finish (using a Semaphore) before terminating all Worker instances and finally itself.