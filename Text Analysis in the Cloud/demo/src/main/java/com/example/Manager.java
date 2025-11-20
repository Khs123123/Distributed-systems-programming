package com.example;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.*;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manager – Full AWS Implementation (Assignment-Compliant)
 *
 * Responsibilities:
 *  - Receives SQS messages from LocalApplications.
 *  - For each NEW_JOB:
 *      → Downloads input file from S3.
 *      → Splits into URL tasks.
 *      → Sends tasks to worker queue.
 *      → Spawns EC2 workers (1 per n tasks, max 19 total).
 *      → Waits for Worker results on results queue.
 *      → Builds summary HTML file.
 *      → Uploads to S3.
 *      → Sends SUMMARY_READY message to LocalApp.
 *  - For TERMINATE:
 *      → Waits for workers to finish, then terminates all EC2 workers.
 *      → Shuts down gracefully.
 */

public class Manager {

    // ======== AWS CONFIGURATION ========
    private static final Region AWS_REGION = Region.EU_CENTRAL_1;
    private static final String S3_BUCKET = "khaled-text-analysis-bucket";
    private static final String MANAGER_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/manager-queue";
    private static final String WORKER_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-queue";
    private static final String WORKER_RESULTS_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-results-queue";
    private static final String WORKER_AMI_ID = "ami-xxxxxxxxxxxxxx";

    // Limit AWS cost & comply with assignment
    private static final int MAX_WORKERS = 19;

    // 🧠 Automatically scale Manager concurrency based on instance size
    private static final int THREAD_COUNT = Math.min(
            10,  // cap
            Math.max(2, Runtime.getRuntime().availableProcessors() * 2)
    );

    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    public static void main(String[] args) {
        System.out.println("=== Manager (AWS VERSION - Dynamic Thread Pool, " + THREAD_COUNT + " threads) ===");
        new Manager().runManagerLoop();
    }

    // === Main Manager Loop ===
    private void runManagerLoop() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            boolean running = true;
            while (running) {
                ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(MANAGER_QUEUE_URL)
                        .waitTimeSeconds(15)
                        .maxNumberOfMessages(1)
                        .build());

                for (Message msg : response.messages()) {
                    String body = msg.body();
                    System.out.println("[Manager] Received: " + body);

                    if (body.startsWith("NEW_JOB")) {
                        executor.submit(() -> handleNewJob(body));
                    } else if (body.equals("TERMINATE")) {
                        System.out.println("[Manager] Terminate signal received.");
                        running = false;
                    }

                    // delete message to prevent reprocessing
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(MANAGER_QUEUE_URL)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }
            }

            System.out.println("[Manager] Shutting down gracefully...");
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
            terminateAllWorkers();

            System.out.println("[Manager] Terminated. Goodbye 👋");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // === Handle a new job from a LocalApplication ===
    private void handleNewJob(String body) {
        try {
            String[] parts = body.split(";");
            String s3Bucket = parts[1];
            String inputKey = parts[2];
            int n = Integer.parseInt(parts[3]);
            String localAppQueueUrl = parts[4];

            System.out.println("[Manager] Starting job for " + inputKey);

            // 1️⃣ Download input file from S3
            Path localInputPath = Paths.get("/tmp/" + UUID.randomUUID() + "_input.txt");
            downloadFromS3(s3Bucket, inputKey, localInputPath);

            // 2️⃣ Read URLs
            List<String[]> tasks = readInputTasks(localInputPath);
            System.out.println("[Manager] Found " + tasks.size() + " URLs to process.");

            // 3️⃣ Send task messages to worker queue
            sendTasksToWorkers(tasks);

            // 4️⃣ Scale up workers based on n
            int needed = (int) Math.ceil((double) tasks.size() / n);
            ensureWorkerCount(needed);

            // 5️⃣ Collect results from workers
            List<String[]> results = collectWorkerResults(tasks.size());

            // 6️⃣ Build HTML summary
            String summaryFile = "/tmp/summary_" + System.currentTimeMillis() + ".html";
            buildSummaryHtml(results, summaryFile);

            // 7️⃣ Upload summary to S3
            String summaryKey = "summaries/" + UUID.randomUUID() + "_summary.html";
            uploadToS3(S3_BUCKET, summaryKey, summaryFile);

            // 8️⃣ Notify LocalApplication
            sendSummaryMessage(localAppQueueUrl, summaryKey);

            System.out.println("[Manager] Completed job for " + inputKey);

        } catch (Exception e) {
            System.err.println("[Manager] ERROR handling job: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // === AWS Utility Methods ===

    private void downloadFromS3(String bucket, String key, Path outputPath) throws IOException {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(), outputPath);
            System.out.println("[S3] Downloaded " + key);
        }
    }

    private void uploadToS3(String bucket, String key, String filePath) throws IOException {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromFile(Paths.get(filePath)));
            System.out.println("[S3] Uploaded summary to s3://" + bucket + "/" + key);
        }
    }

    private List<String[]> readInputTasks(Path inputFile) throws IOException {
        List<String[]> list = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\s+", 2);
                if (parts.length == 2) list.add(parts);
            }
        }
        return list;
    }

    private void sendTasksToWorkers(List<String[]> tasks) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            for (String[] task : tasks) {
                String type = task[0];
                String url = task[1];
                String msg = type + ";" + url;
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(WORKER_QUEUE_URL)
                        .messageBody(msg)
                        .build());
            }
            System.out.println("[SQS] Sent " + tasks.size() + " messages to Worker queue");
        }
    }

    private void ensureWorkerCount(int needed) {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            DescribeInstancesResponse resp = ec2.describeInstances(DescribeInstancesRequest.builder()
                    .filters(Filter.builder().name("instance-state-name").values("running", "pending").build())
                    .build());

            long currentCount = resp.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .filter(i -> i.imageId().equals(WORKER_AMI_ID))
                    .count();

            int toLaunch = Math.min(MAX_WORKERS - (int) currentCount, Math.max(0, needed - (int) currentCount));
            if (toLaunch <= 0) {
                System.out.println("[EC2] No new workers needed. Current=" + currentCount);
                return;
            }

            System.out.println("[EC2] Launching " + toLaunch + " new worker(s)...");
            ec2.runInstances(RunInstancesRequest.builder()
                    .imageId(WORKER_AMI_ID)
                    .instanceType(InstanceType.T3_MICRO)
                    .minCount(1)
                    .maxCount(toLaunch)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .name("WorkerInstanceProfile").build())
                    .build());
        }
    }

    private List<String[]> collectWorkerResults(int expectedCount) {
        List<String[]> results = new ArrayList<>();
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[Manager] Waiting for worker results...");

            while (results.size() < expectedCount) {
                ReceiveMessageResponse resp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(WORKER_RESULTS_QUEUE_URL)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(15)
                        .build());

                for (Message msg : resp.messages()) {
                    String[] parts = msg.body().split(";", 3);
                    results.add(parts);
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(WORKER_RESULTS_QUEUE_URL)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }
            }

        } catch (SqsException e) {
            System.err.println("[Manager] Error collecting results: " + e.awsErrorDetails().errorMessage());
        }
        return results;
    }

    private void buildSummaryHtml(List<String[]> results, String filePath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            writer.write("<html><head><title>Summary</title></head><body>");
            writer.write("<h1>Analysis Results</h1>");
            writer.write("<table border='1'><tr><th>Type</th><th>URL</th><th>Result S3 URL</th></tr>");
            for (String[] res : results) {
                writer.write("<tr><td>" + res[0] + "</td><td>" + res[1] + "</td><td>" + res[2] + "</td></tr>");
            }
            writer.write("</table></body></html>");
        }
    }

    private void sendSummaryMessage(String localAppQueueUrl, String summaryKey) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String msg = "SUMMARY_READY;" + S3_BUCKET + ";" + summaryKey;
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(localAppQueueUrl)
                    .messageBody(msg)
                    .build());
            System.out.println("[SQS] Sent SUMMARY_READY to " + localAppQueueUrl);
        }
    }

    private void terminateAllWorkers() {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            DescribeInstancesResponse resp = ec2.describeInstances();
            List<String> workerIds = new ArrayList<>();
            for (Reservation r : resp.reservations()) {
                for (Instance i : r.instances()) {
                    if (i.imageId().equals(WORKER_AMI_ID)
                            && (i.state().name().equals(InstanceStateName.RUNNING)
                            || i.state().name().equals(InstanceStateName.PENDING))) {
                        workerIds.add(i.instanceId());
                    }
                }
            }

            if (!workerIds.isEmpty()) {
                ec2.terminateInstances(TerminateInstancesRequest.builder()
                        .instanceIds(workerIds)
                        .build());
                System.out.println("[EC2] Terminated workers: " + workerIds.size());
            }

        } catch (Exception e) {
            System.err.println("[Manager] Error terminating workers: " + e.getMessage());
        }
    }
}
