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
import java.util.concurrent.atomic.AtomicInteger; // Required for thread-safe counting

public class Manager {

    // ===============================
    // AWS CONFIGURATION
    // ===============================
    private static final Region AWS_REGION = Region.US_EAST_1;
    private static final String S3_BUCKET = "khaled-text-analysis-bucket-v2";

    private static final String MANAGER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/070930741423/MANAGER_QUEUE";
    private static final String WORKER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/070930741423/worker-queue";
    private static final String WORKER_RESULTS_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/070930741423/worker-results-queue";

    private static final int MAX_WORKERS = 17;

    private static final int THREADS = Math.min(
            10, Math.max(2, Runtime.getRuntime().availableProcessors() * 2)
    );
    private final ExecutorService executor = Executors.newFixedThreadPool(THREADS);

    private static final Gson GSON = new Gson();
    
    // ===============================
    // CONCURRENCY CONTROL ADDITIONS
    // ===============================
    private final AtomicInteger activeJobs = new AtomicInteger(0);
    private final Semaphore terminationLock = new Semaphore(0); 
    private volatile boolean terminationRequested = false; // [cite: 62]

    // === AMI helper: use Manager's own AMI for Worker instances ===
    private static String currentAmiId = null;

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
    
    // ===============================
    // GET CURRENT INSTANCE ID (FOR SELF-TERMINATION)
    // ===============================
    private static String getCurrentInstanceId() {
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                String instanceId = reader.readLine().trim();
                return instanceId;
            }
        } catch (Exception e) {
            System.err.println("[Manager] WARNING: Failed to read Instance ID from metadata. Not on EC2?");
            return null;
        }
    }
    
    // ===============================
    // TERMINATE MANAGER (SELF)
    // ===============================
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


    // ---------- JSON message classes ----------
    private static class BaseMessage {
        String type;
    }

    private static class JobMessage {
        String type; // "NEW_JOB"
        String bucket;
        String inputKey;
        int n;
        String callbackQueueUrl;
    }

    // this must match Worker.WorkerTaskMessage
    private static class WorkerTaskMessage {
        String type = "TASK"; 
        String analysisType; // "POS", "CONSTITUENCY", "DEPENDENCY", "ALL"
        String url;
    }

    // this must match Worker.ResultMessage
    private static class ResultMessage {
        String type;  // "RESULT"
        String analysisType;
        String url;
        String s3Key;
    }

    // this must match Worker.ErrorMessage
    private static class ErrorMessage {
        String type;  // "ERROR"
        String originalMessage;
        String error;
    }

    // summary row for HTML
    private static class SummaryRow {
        int index;
        String analysisType; // <--- ADDED THIS to store the type
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

    // ===============================
    // MAIN
    // ===============================
    public static void main(String[] args) {
        System.out.println("=== Manager started (JSON, URL-based) ===");
        Manager m = new Manager();
        m.runLoop();
    }

    private void runLoop() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            // CHANGE: Loop condition now checks terminationRequested flag [cite: 62]
            while (!terminationRequested) {
                ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                        .queueUrl(MANAGER_QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .visibilityTimeout(3600)
                        .build();

                List<Message> msgs = sqs.receiveMessage(req).messages();
                if (msgs.isEmpty()) continue;

                for (Message msg : msgs) {
                    executor.submit(() -> handleManagerMessage(sqs, msg));
                }
            }
            
            // NEW BLOCK: Wait for TERMINATE to release the lock if it was requested
            if (terminationRequested) {
                System.out.println("[Manager] Main loop stopped. Waiting for all jobs to finish...");
                try {
                    // This blocks until terminate case handler releases the lock
                    terminationLock.acquire(); 
                    System.out.println("[Manager] Termination lock released. Proceeding to shutdown.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("[Manager] Termination wait interrupted.");
                }
            }

        } catch (Exception e) {
            System.err.println("[Manager] Fatal error in main loop: " + e.getMessage());
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private void handleManagerMessage(SqsClient sqs, Message msg) {
        try {
            BaseMessage base = GSON.fromJson(msg.body(), BaseMessage.class);
            // ... (rest of parsing logic) ...
            
            if (base == null || base.type == null) {
                System.err.println("[Manager] Unknown message: " + msg.body());
                deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                return;
            }

            switch (base.type) {
                case "NEW_JOB":
                    if (terminationRequested) { // [cite: 62]
                        System.out.println("[Manager] Termination requested. Ignoring new job.");
                        deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                        return;
                    }
                    JobMessage job = GSON.fromJson(msg.body(), JobMessage.class);
                    System.out.println("[Manager] NEW_JOB: bucket=" + job.bucket +
                            ", key=" + job.inputKey + ", n=" + job.n);
                    
                    deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                    handleNewJob(job);
                    break;

                case "TERMINATE":
                    System.out.println("[Manager] TERMINATE request received. [cite: 61]");
                    
                    // 1. Set flag to stop accepting new jobs [cite: 62]
                    terminationRequested = true; 

                    // 2. Wait for all active jobs to finish [cite: 63]
                    if (activeJobs.get() > 0) {
                        System.out.println("[Manager] Waiting for " + activeJobs.get() + " active job(s) to finish...");
                        terminationLock.acquire(); // Blocks current thread until activeJobs = 0
                    }

                    // 3. Termination sequence (Runs only after all jobs are complete)
                    terminateAllWorkers();
                    deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                    terminateManagerSelf(); 
                    System.out.println("[Manager] Graceful shutdown. Exiting JVM... [cite: 66]");
                    System.exit(0);
                    break;

                default:
                    System.err.println("[Manager] Unknown type: " + base.type);
                    deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
            }

        } catch (Exception e) {
            System.err.println("[Manager] Error handling manager message: " + e.getMessage());
            e.printStackTrace();
            deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
        }
    }

    // ===============================
    // HANDLE NEW JOB (MODIFIED)
    // ===============================
    private void handleNewJob(JobMessage job) {
        
        // INCREMENT THE JOB COUNT
        activeJobs.incrementAndGet();
        System.out.println("[Manager] Job started. Active jobs: " + activeJobs.get());

        try {
            // 1) Download input file from S3
            // ... (existing download logic) ...
            
            Path tempFile = Files.createTempFile("manager_input_", ".txt");
            downloadFromS3(job.bucket, job.inputKey, tempFile);

            List<String> allLines = Files.readAllLines(tempFile, StandardCharsets.UTF_8);
            List<WorkerTaskMessage> tasks = new ArrayList<>();
            int totalTasks = 0;

            for (String line : allLines) {
                String s = line.trim();
                if (s.isEmpty()) continue;
                
                // Split by ANY whitespace (Tab OR Space) to be robust
                String[] parts = s.split("\\s+", 2);
                
                WorkerTaskMessage task = new WorkerTaskMessage();
                if (parts.length >= 2) {
                    task.analysisType = parts[0].trim().toUpperCase(); 
                    task.url = parts[1].trim();
                    tasks.add(task);
                    totalTasks++;
                } else {
                    System.err.println("[Manager] Skipping invalid line: " + line);
                }
            }

            System.out.println("[Manager] Found " + totalTasks + " valid tasks in input file.");

            if (totalTasks == 0) {
                System.out.println("[Manager] No valid tasks found -> nothing to do.");
                return;
            }

            // 2) Ensure enough workers
            int neededWorkers = Math.max(1, (int) Math.ceil((double) totalTasks / job.n));
            ensureWorkerCount(neededWorkers);

            // 3) Send tasks to workers
            sendTasksToWorkers(tasks); 

            // 4) Collect results
            List<SummaryRow> summary = collectResults(totalTasks);

            // 5) Build summary HTML
            String localSummaryFile = "/tmp/summary_" + System.currentTimeMillis() + ".html";
            writeSummaryHTML(summary, localSummaryFile);

            // 6) Upload summary to S3 [cite: 137]
            String summaryKey = "summaries/" + UUID.randomUUID() + "_summary.html";
            uploadToS3(S3_BUCKET, summaryKey, localSummaryFile);

            // 7) Notify Localapplication [cite: 138]
            sendSummaryMessage(job.callbackQueueUrl, summaryKey);

            Files.deleteIfExists(tempFile);

        } catch (Exception e) {
            System.err.println("[Manager] JOB ERROR: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // DECREMENT THE JOB COUNT WHEN PROCESSING IS FINISHED (Success or Fail)
            activeJobs.decrementAndGet(); 
            System.out.println("[Manager] Job finished. Active jobs: " + activeJobs.get());

            // Release the lock if termination was requested and this was the last job
            if (terminationRequested && activeJobs.get() == 0) {
                terminationLock.release();
            }
        }
    }
    
    // ... (rest of helper methods like downloadFromS3, sendTasksToWorkers, collectResults, etc.) ...
    
    // ===============================
    // DOWNLOAD FROM S3
    // ===============================
    private void downloadFromS3(String bucket, String key, Path outFile) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            GetObjectRequest get = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            try (ResponseInputStream<GetObjectResponse> in = s3.getObject(get);
                 FileOutputStream fos = new FileOutputStream(outFile.toFile())) {

                byte[] buf = new byte[8192];
                int r;
                while ((r = in.read(buf)) != -1) {
                    fos.write(buf, 0, r);
                }
            }
            System.out.println("[S3] Downloaded input: s3://" + bucket + "/" + key);
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to download from S3", e);
        }
    }

    // ===============================
    // SEND TASKS TO WORKERS
    // ===============================
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

    // ===============================
    // COLLECT RESULTS / ERRORS
    // ===============================
    private List<SummaryRow> collectResults(int expected) {
        List<SummaryRow> rows = new ArrayList<>();

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            long start = System.currentTimeMillis();
            long timeoutMs = Duration.ofMinutes(40).toMillis();

            while (rows.size() < expected &&
                    System.currentTimeMillis() - start < timeoutMs) {

                ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                        .queueUrl(WORKER_RESULTS_QUEUE_URL)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(20)
                        .visibilityTimeout(1800)
                        .build();

                List<Message> msgs = sqs.receiveMessage(req).messages();
                if (msgs.isEmpty()) continue;

                for (Message m : msgs) {
                    try {
                        BaseMessage base = GSON.fromJson(m.body(), BaseMessage.class);
                        SummaryRow row = new SummaryRow();
                        row.index = rows.size();

                        if (base != null && "RESULT".equals(base.type)) {
                            ResultMessage r = GSON.fromJson(m.body(), ResultMessage.class);
                            row.analysisType = r.analysisType; // <--- STORE TYPE
                            row.url = r.url;
                            row.s3Key = r.s3Key;
                            row.success = true;
                            row.error = "";
                        } else if (base != null && "ERROR".equals(base.type)) {
                            ErrorMessage e = GSON.fromJson(m.body(), ErrorMessage.class);
                            row.analysisType = "UNKNOWN"; // Can't easily recover type from error msg format
                            row.url = "(ERROR) " + e.originalMessage;
                            row.s3Key = "";
                            row.success = false;
                            row.error = e.error;
                        } else {
                            row.analysisType = "UNKNOWN";
                            row.url = "(UNKNOWN MESSAGE)";
                            row.s3Key = "";
                            row.success = false;
                            row.error = "Unknown type";
                        }
                        rows.add(row);
                    } finally {
                        deleteMessageQuiet(sqs, WORKER_RESULTS_QUEUE_URL, m);
                    }
                }
            }
            System.out.println("[Manager] Collected " + rows.size() + "/" + expected + " results.");
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to collect results", e);
        }
        return rows;
    }

    // ===============================
    // WRITE SUMMARY HTML (FIXED TO MATCH ASSIGNMENT)
    // ===============================
    private void writeSummaryHTML(List<SummaryRow> rows, String outFile) {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get(outFile), StandardCharsets.UTF_8))) {

            pw.println("<html><head><title>Text Analysis Summary</title></head><body>");

            for (SummaryRow r : rows) {
                if (r.success) {
                    // Construct public S3 URL
                    String s3Url = "https://" + S3_BUCKET + ".s3.amazonaws.com/" + r.s3Key;
                    
                    // REQUIRED FORMAT: <analysis type>: <input file> <output file> [cite: 22, 24]
                    // Using <p> tags to separate lines cleanly
                    pw.println("<p>" + r.analysisType + ": " + r.url + " " + s3Url + "</p>");
                } else {
                    // Error format: <analysis type>: <input file> <a short description of the exception> [cite: 25]
                    String type = (r.analysisType != null) ? r.analysisType : "UNKNOWN";
                    pw.println("<p>" + type + ": " + r.url + " " + r.error + "</p>");
                }
            }

            pw.println("</body></html>");
            System.out.println("[Manager] Summary HTML written to " + outFile);

        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to write summary HTML", e);
        }
    }

    // ===============================
    // UPLOAD SUMMARY TO S3
    // ===============================
    private void uploadToS3(String bucket, String key, String localPath) {
        try (S3Client s3 = S3Client.builder().region(AWS_REGION).credentialsProvider(DefaultCredentialsProvider.create()).build()) {

        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                //.acl(ObjectCannedACL.PUBLIC_READ) // You may need this for public access
                .build();

        s3.putObject(put, Paths.get(localPath));
        System.out.println("[S3] Uploaded summary: s3://" + bucket + "/" + key);

        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to upload summary", e);
        }
    }

    // ===============================
    // SEND SUMMARY MESSAGE TO LOCALAPP
    // ===============================
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
                    .queueUrl(callbackQueueUrl)
                    .messageBody(body)
                    .build());

            System.out.println("[Manager] SUMMARY message sent.");
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to send summary message", e);
        }
    }

    // ===============================
    // ENSURE WORKER COUNT
    // ===============================
    private void ensureWorkerCount(int needed) {
    try (Ec2Client ec2 = Ec2Client.builder()
            .region(AWS_REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()) {

        DescribeInstancesResponse resp = ec2.describeInstances();

        long current = resp.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .filter(i -> i.state().name().equals(InstanceStateName.RUNNING)
                        || i.state().name().equals(InstanceStateName.PENDING))
                .filter(i -> i.tags().stream().anyMatch(
                        t -> t.key().equals("Type") && t.value().equals("Worker")))
                .count();

        System.out.println("[EC2] Current workers: " + current + ", needed: " + needed);

        int toLaunch = Math.min(MAX_WORKERS - (int) current, Math.max(0, needed - (int) current));
        if (toLaunch <= 0) {
            System.out.println("[EC2] Worker count OK.");
            return;
        }

        System.out.println("[EC2] Launching " + toLaunch + " workers");

        // ... (rest of user data script and run instances logic remains the same) ...

        String userDataScript =
                "#!/bin/bash\n" +
                "exec > /var/log/user-data.log 2>&1\n" +
                "echo '=== USER-DATA START ==='\n" +
                "\n" +
                "# 1. Detect OS and install Java/AWS CLI\n" +
                "if command -v apt-get &> /dev/null; then\n" +
                "    echo 'Detected Ubuntu'\n" +
                "    apt-get update -y\n" +
                "    apt-get install -y default-jre awscli\n" +
                "    USER_HOME=\"/home/ubuntu\"\n" +
                "elif command -v yum &> /dev/null; then\n" +
                "    echo 'Detected Amazon Linux'\n" +
                "    yum update -y\n" +
                "    yum install -y java-1.8.0-openjdk awscli\n" +
                "    USER_HOME=\"/home/ec2-user\"\n" +
                "else\n" +
                "    echo 'Unknown OS'\n" +
                "    exit 1\n" +
                "fi\n" +
                "\n" +
                "# 2. Setup App Directory\n" +
                "mkdir -p $USER_HOME/app\n" +
                "cd $USER_HOME/app\n" +
                "\n" +
                "# 3. Download JAR\n" +
                "echo 'Downloading worker.jar from S3...'\n" +
                "aws s3 cp s3://khaled-text-analysis-bucket-v2/jars/text-analysis-1.0-SNAPSHOT-remote.jar worker.jar\n" +
                "\n" +
                "# 4. Run Worker\n" +
                "echo 'Starting Worker java process...'\n" +
                "nohup java -cp worker.jar com.example.Worker > worker.log 2>&1 &\n" +
                "echo '=== USER-DATA END ==='\n";

        String userDataBase64 =
                Base64.getEncoder().encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

        RunInstancesRequest runReq = RunInstancesRequest.builder()
                .imageId(getCurrentInstanceAmiId())
                .instanceType(InstanceType.T3_MEDIUM)
                .minCount(1)
                .maxCount(toLaunch)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name("LabInstanceProfile")
                        .build())
                .userData(userDataBase64)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key("Type").value("Worker").build())
                        .build())
                .build();

        ec2.runInstances(runReq);
    } catch (Exception e) {
        System.err.println("[Manager] Failed to launch workers: " + e.getMessage());
        e.printStackTrace();
    }
    }

    // ===============================
    // TERMINATE ALL WORKERS
    // ===============================
    private void terminateAllWorkers() {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            DescribeInstancesResponse resp = ec2.describeInstances();
            List<String> toKill = new ArrayList<>();

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
                System.out.println("[EC2] Terminating " + toKill.size() + " workers: " + toKill);
                ec2.terminateInstances(TerminateInstancesRequest.builder()
                        .instanceIds(toKill)
                        .build());
            } else {
                System.out.println("[EC2] No workers to terminate.");
            }

        } catch (Exception e) {
            System.err.println("[Manager] Failed to terminate workers: " + e.getMessage());
        }
    }

    // ===============================
    // HELPERS
    // ===============================
    private void deleteMessageQuiet(SqsClient sqs, String queueUrl, Message msg) {
        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(msg.receiptHandle())
                    .build());
        } catch (Exception ignored) {
        }
    }
}