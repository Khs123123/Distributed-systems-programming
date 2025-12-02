DISTRIBUTED TEXT ANALYSIS SYSTEM ON AWS
======================================

Student:
  Khaled Saleh ID: 326801628
  Mostafa Taha ID: 326524675

Course: Distributed Systems Programming – Text Analysis in the Cloud



1. ENVIRONMENT / AWS DETAILS

Region:              us-east-1 (N. Virginia)

AMI used:
  - AMI ID:          ami-00e95a9222311e8ed
  - Description:     Public course AMI used for both Manager and Worker
                     instances (Amazon Linux 2 compatible).

Instance types:
  - Manager type:    t3.medium (Selected to prevent manager process freezing)
  - Worker type:     t3.medium (Selected to ensure 4GB RAM for Stanford Parser)
  - Max workers:     17 (Hard limit enforced by Manager to avoid AWS blocks)

S3 bucket:
  - Name:            khaled-text-analysis-bucket-v2
  - Structure:
      inputs/      – input files uploaded by Localapplication
      results/     – result text files created by Workers
      summaries/   – HTML summary files created by Manager
      jars/        – remote fat jar for Manager/Workers

SQS queues:
  - Manager queue:       https://sqs.us-east-1.amazonaws.com/070930741423/MANAGER_QUEUE
  - Localapp queue:      https://sqs.us-east-1.amazonaws.com/070930741423/localapp-queue
  - Worker queue:        https://sqs.us-east-1.amazonaws.com/070930741423/worker-queue
  - Worker results:      https://sqs.us-east-1.amazonaws.com/070930741423/worker-results-queue
2. HOW TO BUILD

From the `demo` directory of the project:

1. Build both JARs with Maven:

   mvn clean package

   This creates two fat jars under `demo/target`:
   - text-analysis-1.0-SNAPSHOT-local.jar   (Local Application)
   - text-analysis-1.0-SNAPSHOT-remote.jar  (Manager & Worker)

2. Upload the **remote** jar to S3 (Required before running):

   aws s3 cp target/text-analysis-1.0-SNAPSHOT-remote.jar \
     s3://khaled-text-analysis-bucket-v2/jars/text-analysis-1.0-SNAPSHOT-remote.jar

3. INPUT FORMAT

The local input file (for example `input.txt`) is a plain text file.
Each line is:

   <ANALYSIS_TYPE> <URL>

where:
   - ANALYSIS_TYPE ∈ { POS, CONSTITUENCY, DEPENDENCY }
   - URL is the URL of a text file (for example, Project Gutenberg links)

Example of three books with all three analyses:

   POS https://www.gutenberg.org/files/1659/1659-0.txt
   CONSTITUENCY https://www.gutenberg.org/files/1659/1659-0.txt
   DEPENDENCY https://www.gutenberg.org/files/1659/1659-0.txt
   POS https://www.gutenberg.org/files/1661/1661-0.txt
   CONSTITUENCY https://www.gutenberg.org/files/1661/1661-0.txt
   DEPENDENCY https://www.gutenberg.org/files/1661/1661-0.txt
   POS https://www.gutenberg.org/files/1660/1660-0.txt
   CONSTITUENCY https://www.gutenberg.org/files/1660/1660-0.txt
   DEPENDENCY https://www.gutenberg.org/files/1660/1660-0.txt


4. HOW TO RUN (LOCALAPPLICATION)

The application is run via command line.

Command syntax:
   java -jar <jar-path> <input-file> <output-file> <n> [terminate]

The command used for our final submission test:

   java -jar target/text-analysis-1.0-SNAPSHOT-local.jar input-final.txt output-final.html 1 terminate

Arguments explanation:
  - input-final.txt   : The file containing the 9 heavy tasks.
  - output-final.html : The file where the HTML summary was saved.
  - 1                 : The value of 'n' (Tasks per Worker ratio).
                        Since we had 9 tasks and n=1, the system launched 9 Workers.
  - terminate         : Instructs the Manager to shut down all resources upon completion.


5. HIGH-LEVEL SYSTEM FLOW

5.1 Localapplication
--------------------
1. Checks for active Manager. If missing, launches a new EC2 (t3.medium) with a user-data script that downloads the code from S3.
2. Uploads the input file to S3 (`inputs/`).
3. Sends a "NEW_JOB" message to the Manager Queue via SQS.
4. Waits/Polls the LocalApp Queue for the "SUMMARY" completion message.
5. Downloads the final HTML from S3 (`summaries/`) and terminates the system if requested.

5.2 Manager
-----------
1. Polls Manager Queue. Upon "NEW_JOB":
2. Downloads input file, parses lines into Tasks.
3. Calculates required workers: Workers = ceil(Tasks / n).
   * In our run: ceil(9 tasks / 1) = 9 Workers launched.
4. Launches EC2 Workers (t3.medium) using user-data scripts.
5. Sends tasks to Worker Queue.
6. Collects results from Results Queue.
   * Maintains a ConcurrentHashMap of pending tasks.
   * If a task times out (40 mins), it marks it as "Crashed/Timed Out".
7. Generates HTML summary, uploads to S3, and notifies Local App.

5.3 Worker
----------
1. Bootstraps via user-data script (installs Java, downloads JAR from S3).
2. Polls Worker Queue.
3. Downloads text URL.
4. Runs Stanford CoreNLP (POS/Constituency/Dependency).
   * Protection: We set `parse.maxlen = 40` to prevent memory overflows on massive sentences.
   * Protection: We catch `Throwable` (not just Exception) to handle OutOfMemory errors gracefully.
5. Uploads result text to S3 (`results/`).
6. Sends "RESULT" (or "ERROR") message to Results Queue.
7. Deletes the processed message from SQS to prevent "poison pill" loops.


6. RUN STATISTICS (EXAMPLE RUN)

We performed a full stress test using `input-final.txt` containing 9 tasks (including heavy Dependency parsing).

  - Input File:       input-final.txt
  - Value of n:       1 (1 file per worker ratio)
  - Workers Launched: 9 (Calculated as 9 tasks / 1)
  - Total Time:       ~11 minutes
  - Output File:      output-final.html

Result: All 9 tasks were processed. Heavy Dependency tasks completed successfully without crashing the system due to our memory safeguards (t3.medium + maxlen setting).


7. NOTES / DESIGN DECISIONS

1. SCALABILITY:
   - The Manager creates workers dynamically based on the workload formula (Tasks/n).
   - We used `t3.medium` instances to ensure sufficient RAM (4GB) for the Stanford Parser, preventing `OutOfMemoryErrors` that occurred with micro instances.
   - The Manager uses an ExecutorService to handle multiple incoming messages in parallel.

2. PERSISTENCE & FAULT TOLERANCE:
   - **Worker Recovery:** If a Worker node dies (e.g., OOM), our code catches the `Throwable`, sends an Error message, and deletes the bad message from SQS. This prevents the system from hanging.
   - **SQS Visibility:** If a node dies silently (hardware failure), SQS Visibility Timeout (1 hour) ensures the message becomes visible to another worker eventually.
   - **Manager Timeout:** The Manager implements a logical timeout (40 minutes). If a result isn't received by then, it marks the specific task as "Crashed/Timed Out" in the final output rather than hanging indefinitely.

3. SECURITY:
   - No credentials are hardcoded.
   - We use `DefaultCredentialsProvider`, which securely fetches credentials from the instance metadata (IAM Roles: LabInstanceProfile) when running on EC2.

4. THREADS:
   - Manager uses a ThreadPool to process SQS messages to ensure it doesn't block while waiting for S3 I/O.
   - Workers are single-threaded per instance to maximize the CPU/RAM available for the heavy NLP operation on that specific node.

5. TERMINATION:
   - The `terminate` argument triggers a graceful shutdown.
   - The Manager waits for all active jobs to finish (using a Semaphore) before terminating all Worker instances and finally itself.