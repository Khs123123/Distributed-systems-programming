package com.example;

import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.*;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {

    private static final Region AWS_REGION = Region.US_EAST_1;
    private static final String S3_BUCKET = "khaled-text-analysis-bucket-v3";

    private static final String MANAGER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/597918329386/MANAGER_QUEUE";
    private static final String WORKER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/597918329386/worker-queue";
    private static final String WORKER_RESULTS_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/597918329386/worker-results-queue";

    private static final int MAX_WORKERS = 17; // AWS Instance limit safeguard 

    private static final int THREADS = Math.min(
            10, Math.max(2, Runtime.getRuntime().availableProcessors() * 2)
    );
    private final ExecutorService executor = Executors.newFixedThreadPool(THREADS); // For concurrent job handling

    private static final Gson GSON = new Gson();
    

    private final AtomicInteger activeJobs = new AtomicInteger(0); // Tracks running jobs from LocalApps
    private final Semaphore terminationLock = new Semaphore(0); // Barrier for graceful shutdown
    private volatile boolean terminationRequested = false;

    //  EC2 Metadata Helpers 
    private static String currentAmiId = null;

    // Retrieves AMI ID from instance metadata (used to launch Workers with the same image)
    private static String getCurrentInstanceAmiId() {
        if (currentAmiId != null) return currentAmiId;
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/ami-id");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                currentAmiId = reader.readLine().trim();
                System.out.println("[Manager] Detected AMI ID: " + currentAmiId);
                return currentAmiId;
            }
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to read AMI ID from metadata", e);
        }
    }
    
    // Retrieves Instance ID from instance metadata (used for self-termination)
    private static String getCurrentInstanceId() {
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                return reader.readLine().trim();
            }
        } catch (Exception e) {
            System.err.println("[Manager] WARNING: Failed to read Instance ID. Not on EC2?");
            return null;
        }
    }
    
    // Terminates the Manager's own EC2 instance
    private void terminateManagerSelf() {
        String instanceId = getCurrentInstanceId();
        if (instanceId == null) {
            System.err.println("[EC2] Cannot self-terminate: Instance ID not found.");
            return;
        }

        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[EC2] Terminating self: " + instanceId);
            ec2.terminateInstances(TerminateInstancesRequest.builder()
                        .instanceIds(instanceId)
                        .build());
        } catch (Exception e) {
            System.err.println("[Manager] Failed to self-terminate: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // JSON message classes 
    private static class BaseMessage { String type; }

    private static class JobMessage {
        String type; 
        String bucket;
        String inputKey;
        int n;
        String callbackQueueUrl;
    }

    private static class WorkerTaskMessage {
        String type = "TASK"; 
        String analysisType; 
        String url;
    }

    private static class ResultMessage {
        String type; 
        String analysisType;
        String url;
        String s3Key;
    }

    private static class ErrorMessage {
        String type; 
        String originalMessage;
        String error;
    }

    private static class SummaryRow {
        int index;
        String analysisType; 
        String url;
        String s3Key;
        boolean success;
        String error;
    }

    private static class SummaryMessage {
        String type = "SUMMARY";
        String bucket;
        String summaryKey;
    }

    public static void main(String[] args) {
        System.out.println("=== Manager started ===");
        Manager m = new Manager();
        m.runLoop();
    }

    // Main polling loop for the Manager
    private void runLoop() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            while (!terminationRequested) {
                // Poll MANAGER_QUEUE for new jobs or termination requests
                ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                        .queueUrl(MANAGER_QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20) // Long polling
                        .visibilityTimeout(3600) 
                        .build();

                List<Message> msgs = sqs.receiveMessage(req).messages();
                if (msgs.isEmpty()) continue;

                // Submit message handling to the thread pool (concurrent processing)
                for (Message msg : msgs) {
                    executor.submit(() -> handleManagerMessage(sqs, msg));
                }
            }
            
            // Wait for all active jobs to complete before final termination
            if (terminationRequested) {
                System.out.println("[Manager] Termination requested. Waiting for all jobs to finish...");
                try {
                    terminationLock.acquire(); 
                    System.out.println("[Manager] Termination lock released. Proceeding to shutdown.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        } catch (Exception e) {
            System.err.println("[Manager] Fatal error in main loop: " + e.getMessage());
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    // Distinguishes between NEW_JOB and TERMINATE messages
    private void handleManagerMessage(SqsClient sqs, Message msg) {
        try {
            BaseMessage base = GSON.fromJson(msg.body(), BaseMessage.class);
            
            if (base == null || base.type == null) {
                deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                return;
            }

            switch (base.type) {
                case "NEW_JOB":
                    if (terminationRequested) {
                        System.out.println("[Manager] Termination requested. Ignoring new job.");
                        deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                        return;
                    }
                    JobMessage job = GSON.fromJson(msg.body(), JobMessage.class);
                    System.out.println("[Manager] NEW_JOB received.");
                    deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                    handleNewJob(job);
                    break;

                case "TERMINATE":
                    System.out.println("[Manager] TERMINATE request received.");
                    terminationRequested = true; 
                    
                    // If no active jobs, proceed immediately. Otherwise, wait via terminationLock.
                    if (activeJobs.get() == 0) {
                        terminateAllWorkers();
                        deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                        terminateManagerSelf(); 
                        System.exit(0);
                    } else {
                        // If jobs are active, allow runLoop to acquire the lock when done.
                        System.out.println("[Manager] Waiting for " + activeJobs.get() + " active job(s)...");
                    }
                    break;

                default:
                    deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
            }

        } catch (Exception e) {
            System.err.println("[Manager] Error handling manager message: " + e.getMessage());
            e.printStackTrace();
            deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
        }
    }

    // Handles the full lifecycle of a new job request
    private void handleNewJob(JobMessage job) {
        
        activeJobs.incrementAndGet();

        try {
            // 1) Download input file from S3
            Path tempFile = Files.createTempFile("manager_input_", ".txt");
            downloadFromS3(job.bucket, job.inputKey, tempFile);

            List<String> allLines = Files.readAllLines(tempFile, StandardCharsets.UTF_8);
            List<WorkerTaskMessage> tasks = new ArrayList<>();
            
            // Map to track pending tasks (Key: "Type::URL") for crash detection
            Map<String, WorkerTaskMessage> pendingTasks = new ConcurrentHashMap<>();

            for (String line : allLines) {
                String s = line.trim();
                if (s.isEmpty()) continue;
                
                String[] parts = s.split("\\s+", 2);
                
                if (parts.length >= 2) {
                    WorkerTaskMessage task = new WorkerTaskMessage();
                    task.analysisType = parts[0].trim().toUpperCase(); 
                    task.url = parts[1].trim();
                    
                    tasks.add(task);
                    
                    // Add to pending map for tracking
                    String key = task.analysisType + "::" + task.url;
                    pendingTasks.put(key, task);
                }
            }

            int totalTasks = tasks.size();

            if (totalTasks == 0) return;

            // 2) Ensure enough workers (scaling logic)
            int neededWorkers = Math.max(1, (int) Math.ceil((double) totalTasks / job.n));
            ensureWorkerCount(neededWorkers);

            // 3) Send tasks to workers
            sendTasksToWorkers(tasks); 

            // 4) Collect results (Blocks until all results or timeout)
            List<SummaryRow> summary = collectResults(totalTasks, pendingTasks);

            // 5) Build summary HTML
            String localSummaryFile = "/tmp/summary_" + System.currentTimeMillis() + ".html";
            writeSummaryHTML(summary, localSummaryFile);

            // 6) Upload summary to S3
            String summaryKey = "summaries/" + UUID.randomUUID() + "_summary.html";
            uploadToS3(S3_BUCKET, summaryKey, localSummaryFile);

            // 7) Notify Localapplication
            sendSummaryMessage(job.callbackQueueUrl, summaryKey);

            Files.deleteIfExists(tempFile);

        } catch (Exception e) {
            System.err.println("[Manager] JOB ERROR: " + e.getMessage());
            e.printStackTrace();
        } finally {
            activeJobs.decrementAndGet(); 
            // Release termination lock if termination was requested and this was the last active job
            if (terminationRequested && activeJobs.get() == 0) {
                terminationLock.release();
            }
        }
    }
    
    // Collects results from the WORKER_RESULTS_QUEUE
    private List<SummaryRow> collectResults(int expected, Map<String, WorkerTaskMessage> pendingTasks) {
        List<SummaryRow> rows = new ArrayList<>();

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            long start = System.currentTimeMillis();
            long timeoutMs = Duration.ofMinutes(40).toMillis(); // 40-minute timeout for job completion

            while (rows.size() < expected &&
                        System.currentTimeMillis() - start < timeoutMs) {

                ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                        .queueUrl(WORKER_RESULTS_QUEUE_URL)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(20)
                        .visibilityTimeout(3600) 
                        .build();

                List<Message> msgs = sqs.receiveMessage(req).messages();
                if (msgs.isEmpty()) continue;

                for (Message m : msgs) {
                    try {
                        BaseMessage base = GSON.fromJson(m.body(), BaseMessage.class);
                        SummaryRow row = new SummaryRow();
                        row.index = rows.size();
                        String taskKey = null;

                        // Process RESULT message
                        if (base != null && "RESULT".equals(base.type)) {
                            ResultMessage r = GSON.fromJson(m.body(), ResultMessage.class);
                            row.analysisType = r.analysisType;
                            row.url = r.url;
                            row.s3Key = r.s3Key;
                            row.success = true;
                            row.error = "";
                            taskKey = r.analysisType + "::" + r.url;

                        // Process ERROR message (worker reported a failure)
                        } else if (base != null && "ERROR".equals(base.type)) {
                            ErrorMessage e = GSON.fromJson(m.body(), ErrorMessage.class);
                            try {
                                WorkerTaskMessage originalTask = GSON.fromJson(e.originalMessage, WorkerTaskMessage.class);
                                row.analysisType = originalTask.analysisType; 
                                row.url = originalTask.url;
                                taskKey = row.analysisType + "::" + row.url;
                            } catch (Exception parseEx) {
                                row.analysisType = "UNKNOWN";
                                row.url = "PARSE_ERROR"; 
                            }
                            row.s3Key = "";
                            row.success = false; 
                            row.error = e.error; 
                        }

                        // Remove from pending map if result/error received
                        if (taskKey != null) {
                            pendingTasks.remove(taskKey);
                        }
                        rows.add(row);
                    } finally {
                        deleteMessageQuiet(sqs, WORKER_RESULTS_QUEUE_URL, m);
                    }
                }
            }
            
            // Check for tasks that timed out (unreported crashes)
            if (!pendingTasks.isEmpty()) {
                System.err.println("[Manager] Timeout reached! " + pendingTasks.size() + " tasks did not return.");
                for (WorkerTaskMessage missing : pendingTasks.values()) {
                    SummaryRow crashedRow = new SummaryRow();
                    crashedRow.analysisType = missing.analysisType;
                    crashedRow.url = missing.url;
                    crashedRow.success = false;
                    crashedRow.s3Key = "";
                    crashedRow.error = "Worker crashed or timed out (No response received)";
                    rows.add(crashedRow);
                }
            } else {
                System.out.println("[Manager] All results collected successfully.");
            }
            
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to collect results", e);
        }
        return rows;
    }
    
    // Downloads file from S3 to a temporary path
    private void downloadFromS3(String bucket, String key, Path outFile) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            GetObjectRequest get = GetObjectRequest.builder()
                .bucket(bucket).key(key).build();

            try (ResponseInputStream<GetObjectResponse> in = s3.getObject(get);
                 FileOutputStream fos = new FileOutputStream(outFile.toFile())) {
                byte[] buf = new byte[8192];
                int r;
                while ((r = in.read(buf)) != -1) {
                    fos.write(buf, 0, r);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to download from S3", e);
        }
    }

    // Sends all individual tasks to the Worker SQS queue
    private void sendTasksToWorkers(List<WorkerTaskMessage> tasks) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            for (WorkerTaskMessage task : tasks) {
                String body = GSON.toJson(task);
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(WORKER_QUEUE_URL)
                        .messageBody(body)
                        .build());
            }
            System.out.println("[Manager] Sent " + tasks.size() + " TASK messages.");
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to send tasks", e);
        }
    }

    // Generates the final HTML summary file
    private void writeSummaryHTML(List<SummaryRow> rows, String outFile) {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get(outFile), StandardCharsets.UTF_8))) {

            pw.println("<html><head><title>Text Analysis Summary</title></head><body>");

            for (SummaryRow r : rows) {
                if (r.success && r.s3Key != null && !r.s3Key.isEmpty()) {
                    String s3Url = "https://" + S3_BUCKET + ".s3.amazonaws.com/" + r.s3Key;
                    pw.println("<p>" + r.analysisType + ": " + r.url + " " + s3Url + "</p>");
                } else {
                    String type = (r.analysisType != null) ? r.analysisType : "UNKNOWN";
                    pw.println("<p>" + type + ": " + r.url + " " + r.error + "</p>");
                }
            }
            pw.println("</body></html>");
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to write summary HTML", e);
        }
    }

    // Uploads the generated summary HTML to S3
    private void uploadToS3(String bucket, String key, String localPath) {
        try (S3Client s3 = S3Client.builder().region(AWS_REGION).credentialsProvider(DefaultCredentialsProvider.create()).build()) {
            PutObjectRequest put = PutObjectRequest.builder()
                        .bucket(bucket).key(key).build();
            s3.putObject(put, Paths.get(localPath));
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to upload summary", e);
        }
    }

    // Notifies the LocalApplication that the job is complete
    private void sendSummaryMessage(String callbackQueueUrl, String summaryKey) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            SummaryMessage msg = new SummaryMessage();
            msg.bucket = S3_BUCKET;
            msg.summaryKey = summaryKey;
            String body = GSON.toJson(msg);
            sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(callbackQueueUrl).messageBody(body).build());
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to send summary message", e);
        }
    }

    // Scaling logic: launches new Workers if needed
    private void ensureWorkerCount(int needed) {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            DescribeInstancesResponse resp = ec2.describeInstances();
            // Count current running/pending workers tagged "Type=Worker"
            long current = resp.reservations().stream()
                        .flatMap(r -> r.instances().stream())
                        .filter(i -> i.state().name().equals(InstanceStateName.RUNNING)
                                || i.state().name().equals(InstanceStateName.PENDING))
                        .filter(i -> i.tags().stream().anyMatch(
                                t -> t.key().equals("Type") && t.value().equals("Worker")))
                        .count();

            System.out.println("[EC2] Current workers: " + current + ", needed: " + needed);
            // Calculate how many to launch, respecting MAX_WORKERS limit
            int toLaunch = Math.min(MAX_WORKERS - (int) current, Math.max(0, needed - (int) current));
            if (toLaunch <= 0) return;

            System.out.println("[EC2] Launching " + toLaunch + " workers");
            
            // User data script for Worker instance bootstrapping
            String userDataScript =
                    "#!/bin/bash\n" +
                    "exec > /var/log/user-data.log 2>&1\n" +
                    "echo '=== USER-DATA START ==='\n" +
                    "if command -v apt-get &> /dev/null; then\n" +
                    "  apt-get update -y\n" +
                    "  apt-get install -y default-jre awscli\n" +
                    "  USER_HOME=\"/home/ubuntu\"\n" +
                    "elif command -v yum &> /dev/null; then\n" +
                    "  yum update -y\n" +
                    "  yum install -y java-1.8.0-openjdk awscli\n" +
                    "  USER_HOME=\"/home/ec2-user\"\n" +
                    "else\n" +
                    "  exit 1\n" +
                    "fi\n" +
                    "mkdir -p $USER_HOME/app\n" +
                    "cd $USER_HOME/app\n" +
                    "aws s3 cp s3://" + S3_BUCKET + "/jars/text-analysis-1.0-SNAPSHOT-remote.jar worker.jar\n" +
                    "nohup java -cp worker.jar com.example.Worker > worker.log 2>&1 &\n";

            String userDataBase64 = Base64.getEncoder().encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

            ec2.runInstances(RunInstancesRequest.builder()
                        .imageId(getCurrentInstanceAmiId())
                        .instanceType(InstanceType.T3_MEDIUM)
                        .minCount(1).maxCount(toLaunch)
                        .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                        .userData(userDataBase64)
                        .tagSpecifications(TagSpecification.builder()
                                .resourceType(ResourceType.INSTANCE)
                                .tags(Tag.builder().key("Type").value("Worker").build())
                                .build())
                        .build());
        } catch (Exception e) {
            System.err.println("[Manager] Failed to launch workers: " + e.getMessage());
        }
    }

    // Terminates all active Worker instances (used during Manager termination)
    private void terminateAllWorkers() {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            List<String> toKill = new ArrayList<>();
            DescribeInstancesResponse resp = ec2.describeInstances();
            for (Reservation r : resp.reservations()) {
                for (Instance i : r.instances()) {
                    if ((i.state().name().equals(InstanceStateName.RUNNING)
                            || i.state().name().equals(InstanceStateName.PENDING))
                            && i.tags().stream().anyMatch(
                            t -> t.key().equals("Type") && t.value().equals("Worker"))) {
                        toKill.add(i.instanceId());
                    }
                }
            }

            if (!toKill.isEmpty()) {
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(toKill).build());
                System.out.println("[EC2] Terminated workers: " + toKill);
            }
        } catch (Exception e) {
            System.err.println("[Manager] Failed to terminate workers: " + e.getMessage());
        }
    }

    // Deletes an SQS message silently (used for general cleanup)
    private void deleteMessageQuiet(SqsClient sqs, String queueUrl, Message msg) {
        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl).receiptHandle(msg.receiptHandle()).build());
        } catch (Exception ignored) {}
    }
}