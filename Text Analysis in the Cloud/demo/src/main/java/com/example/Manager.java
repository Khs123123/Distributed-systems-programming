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

    // ===============================
    // AWS CONFIGURATION
    // ===============================
    private static final Region AWS_REGION = Region.US_EAST_1;
    // UPDATE IF NECESSARY
    private static final String S3_BUCKET = "khaled-text-analysis-bucket-v2";

    private static final String MANAGER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/070930741423/MANAGER_QUEUE";
    private static final String WORKER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/070930741423/worker-queue";
    private static final String WORKER_RESULTS_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/070930741423/worker-results-queue";

    private static final int MAX_WORKERS = 17; // AWS Limit safeguard [cite: 57]

    private static final int THREADS = Math.min(
            10, Math.max(2, Runtime.getRuntime().availableProcessors() * 2)
    );
    private final ExecutorService executor = Executors.newFixedThreadPool(THREADS);

    private static final Gson GSON = new Gson();
    
    // ===============================
    // CONCURRENCY CONTROL
    // ===============================
    private final AtomicInteger activeJobs = new AtomicInteger(0);
    private final Semaphore terminationLock = new Semaphore(0); 
    private volatile boolean terminationRequested = false;

    // === AMI helper ===
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

    // ===============================
    // MAIN
    // ===============================
    public static void main(String[] args) {
        System.out.println("=== Manager started (Production) ===");
        Manager m = new Manager();
        m.runLoop();
    }

    private void runLoop() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

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
            
            if (terminationRequested) {
                System.out.println("[Manager] Main loop stopped. Waiting for all jobs to finish...");
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
                    if (activeJobs.get() > 0) {
                        System.out.println("[Manager] Waiting for " + activeJobs.get() + " active job(s)...");
                        terminationLock.acquire(); 
                    }
                    terminateAllWorkers();
                    deleteMessageQuiet(sqs, MANAGER_QUEUE_URL, msg);
                    terminateManagerSelf(); 
                    System.exit(0);
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

    // ===============================
    // HANDLE NEW JOB
    // ===============================
    private void handleNewJob(JobMessage job) {
        
        activeJobs.incrementAndGet();
        System.out.println("[Manager] Job started. Active jobs: " + activeJobs.get());

        try {
            // 1) Download input file from S3
            Path tempFile = Files.createTempFile("manager_input_", ".txt");
            downloadFromS3(job.bucket, job.inputKey, tempFile);

            List<String> allLines = Files.readAllLines(tempFile, StandardCharsets.UTF_8);
            List<WorkerTaskMessage> tasks = new ArrayList<>();
            
            // --- TRACKING: Map to track pending tasks (Key: "Type::URL") ---
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
                    
                    // Add to pending map
                    String key = task.analysisType + "::" + task.url;
                    pendingTasks.put(key, task);
                }
            }

            int totalTasks = tasks.size();
            System.out.println("[Manager] Found " + totalTasks + " tasks.");

            if (totalTasks == 0) return;

            // 2) Ensure enough workers
            int neededWorkers = Math.max(1, (int) Math.ceil((double) totalTasks / job.n));
            ensureWorkerCount(neededWorkers);

            // 3) Send tasks to workers
            sendTasksToWorkers(tasks); 

            // 4) Collect results (Pass pendingTasks map)
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
            if (terminationRequested && activeJobs.get() == 0) {
                terminationLock.release();
            }
        }
    }
    
    // ===============================
    // COLLECT RESULTS
    // ===============================
    private List<SummaryRow> collectResults(int expected, Map<String, WorkerTaskMessage> pendingTasks) {
        List<SummaryRow> rows = new ArrayList<>();

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            long start = System.currentTimeMillis();
            
            // -------------------------------------------------------------
            // PRODUCTION TIMEOUT: 40 MINUTES
            // -------------------------------------------------------------
            long timeoutMs = Duration.ofMinutes(40).toMillis(); 

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

                        if (base != null && "RESULT".equals(base.type)) {
                            ResultMessage r = GSON.fromJson(m.body(), ResultMessage.class);
                            row.analysisType = r.analysisType;
                            row.url = r.url;
                            row.s3Key = r.s3Key;
                            row.success = true;
                            row.error = "";
                            taskKey = r.analysisType + "::" + r.url;

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

                        // Remove from pending map if key found
                        if (taskKey != null) {
                            pendingTasks.remove(taskKey);
                        }
                        rows.add(row);
                    } finally {
                        deleteMessageQuiet(sqs, WORKER_RESULTS_QUEUE_URL, m);
                    }
                }
            }
            
            // --- CHECK FOR CRASHED/TIMED-OUT WORKERS ---
            if (!pendingTasks.isEmpty()) {
                System.err.println("[Manager] Timeout reached! " + pendingTasks.size() + " tasks did not return.");
                for (WorkerTaskMessage missing : pendingTasks.values()) {
                    SummaryRow crashedRow = new SummaryRow();
                    crashedRow.analysisType = missing.analysisType;
                    crashedRow.url = missing.url;
                    crashedRow.success = false;
                    crashedRow.s3Key = "";
                    // Reports crash as per assignment requirement
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

    // ===============================
    // EXISTING HELPERS
    // ===============================
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

    private void writeSummaryHTML(List<SummaryRow> rows, String outFile) {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get(outFile), StandardCharsets.UTF_8))) {

            pw.println("<html><head><title>Text Analysis Summary</title></head><body>");

            for (SummaryRow r : rows) {
                if (r.success && r.s3Key != null && !r.s3Key.isEmpty()) {
                    String s3Url = "https://" + S3_BUCKET + ".s3.amazonaws.com/" + r.s3Key;
                    pw.println("<p>" + r.analysisType + ": " + r.url + " " + s3Url + "</p>");
                } else {
                    // Handles explicit errors AND crashes
                    String type = (r.analysisType != null) ? r.analysisType : "UNKNOWN";
                    pw.println("<p>" + type + ": " + r.url + " " + r.error + "</p>");
                }
            }
            pw.println("</body></html>");
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to write summary HTML", e);
        }
    }

    private void uploadToS3(String bucket, String key, String localPath) {
        try (S3Client s3 = S3Client.builder().region(AWS_REGION).credentialsProvider(DefaultCredentialsProvider.create()).build()) {
        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucket).key(key).build();
        s3.putObject(put, Paths.get(localPath));
        } catch (Exception e) {
            throw new RuntimeException("[Manager] Failed to upload summary", e);
        }
    }

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
            if (toLaunch <= 0) return;

            System.out.println("[EC2] Launching " + toLaunch + " workers");
            
            // Using -cp to run explicit Worker class (correct for Fat JAR)
            String userDataScript =
                    "#!/bin/bash\n" +
                    "exec > /var/log/user-data.log 2>&1\n" +
                    "echo '=== USER-DATA START ==='\n" +
                    "if command -v apt-get &> /dev/null; then\n" +
                    "    apt-get update -y\n" +
                    "    apt-get install -y default-jre awscli\n" +
                    "    USER_HOME=\"/home/ubuntu\"\n" +
                    "elif command -v yum &> /dev/null; then\n" +
                    "    yum update -y\n" +
                    "    yum install -y java-1.8.0-openjdk awscli\n" +
                    "    USER_HOME=\"/home/ec2-user\"\n" +
                    "else\n" +
                    "    exit 1\n" +
                    "fi\n" +
                    "mkdir -p $USER_HOME/app\n" +
                    "cd $USER_HOME/app\n" +
                    "aws s3 cp s3://khaled-text-analysis-bucket-v2/jars/text-analysis-1.0-SNAPSHOT-remote.jar worker.jar\n" +
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

    private void deleteMessageQuiet(SqsClient sqs, String queueUrl, Message msg) {
        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl).receiptHandle(msg.receiptHandle()).build());
        } catch (Exception ignored) {}
    }
}