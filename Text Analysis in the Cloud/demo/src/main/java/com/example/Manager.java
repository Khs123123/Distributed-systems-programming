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
import software.amazon.awssdk.core.sync.RequestBody;

/**
 * ============================
 *       MANAGER (FINAL)
 * ============================
 *
 * Responsibilities according to assignment:
 *  ---------------------------------------
 *  ✔ Receives NEW_JOB messages from LocalApplications
 *  ✔ Downloads input file from S3
 *  ✔ Splits into URL tasks (n defines worker-per-job ratio)
 *  ✔ Sends tasks to Workers via worker-queue
 *  ✔ Spawns EC2 workers dynamically
 *  ✔ Collects results from workers via worker-results queue
 *  ✔ Builds summary.html file
 *  ✔ Uploads summary to S3
 *  ✔ Sends SUMMARY_READY;<bucket>;<key> back to LocalApplication
 *  ✔ Handles TERMINATE: shutdown workers + exit
 */

public class Manager {

    // ===============================
    // AWS CONFIGURATION
    // ===============================
    private static final Region AWS_REGION = Region.EU_CENTRAL_1;

    private static final String S3_BUCKET = "khaled-text-analysis-bucket";

    private static final String MANAGER_QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/manager-queue";

    private static final String WORKER_QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-queue";

    private static final String WORKER_RESULTS_QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-results-queue";

    private static final String WORKER_AMI_ID = "ami-xxxxxxxxxxxxxx";

    private static final int MAX_WORKERS = 19;

    // Thread pool for processing multiple NEW_JOB messages simultaneously
    private static final int THREADS = Math.min(
            10, Math.max(2, Runtime.getRuntime().availableProcessors() * 2)
    );

    private final ExecutorService executor = Executors.newFixedThreadPool(THREADS);

    public static void main(String[] args) {
        System.out.println("\n=== Manager Started (" + THREADS + " threads) ===");
        new Manager().run();
    }


    // ===============================
    // MAIN MANAGER LOOP
    // ===============================
    private void run() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            boolean alive = true;

            while (alive) {

                ReceiveMessageResponse resp = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(MANAGER_QUEUE_URL)
                                .waitTimeSeconds(15)       // long polling
                                .maxNumberOfMessages(1)
                                .build()
                );

                for (Message msg : resp.messages()) {

                    String body = msg.body();
                    System.out.println("[Manager] Received: " + body);

                    // NEW JOB
                    if (body.startsWith("NEW_JOB")) {
                        executor.submit(() -> handleNewJob(body));
                    }

                    // TERMINATE
                    else if (body.equals("TERMINATE")) {
                        System.out.println("[Manager] TERMINATE received");
                        alive = false;
                    }

                    // Acknowledge (delete)
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(MANAGER_QUEUE_URL)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }
            }

            System.out.println("[Manager] Graceful shutdown...");
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);

            terminateAllWorkers();
            System.out.println("[Manager] Terminated all workers. Exiting.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ===============================
    // HANDLE NEW JOB
    // ===============================
    private void handleNewJob(String message) {
        try {
            String[] parts = message.split(";");

            String bucket = parts[1];
            String inputKey = parts[2];
            int n = Integer.parseInt(parts[3]);
            String localAppQueueURL = parts[4];

            System.out.println("[Manager] Handling job for: " + inputKey);

            // 1) Download input
            Path inputPath = Paths.get("/tmp/" + UUID.randomUUID() + "_input.txt");
            downloadFromS3(bucket, inputKey, inputPath);

            // 2) Parse input file (URL + type)
            List<String[]> tasks = parseInput(inputPath);
            int totalTasks = tasks.size();
            System.out.println("[Manager] Total URLs: " + totalTasks);

            // 3) Push tasks to worker queue
            sendTasksToWorkerQueue(tasks);

            // 4) Ensure enough workers
            ensureWorkerCount((int) Math.ceil((double) totalTasks / n));

            // 5) Collect worker results
            List<String[]> results = collectResults(totalTasks);

            // 6) Build HTML summary
            String localSummaryFile = "/tmp/summary_" + System.currentTimeMillis() + ".html";
            writeSummaryHTML(results, localSummaryFile);

            // 7) Upload summary to S3
            String summaryKey = "summaries/" + UUID.randomUUID() + "_summary.html";
            uploadToS3(S3_BUCKET, summaryKey, localSummaryFile);

            // 8) Notify LocalApplication
            sendSummaryMessage(localAppQueueURL, summaryKey);

            System.out.println("[Manager] DONE for job: " + inputKey);

        } catch (Exception e) {
            System.err.println("[Manager] JOB ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }


    // ===============================
    // DOWNLOAD FROM S3
    // ===============================
    private void downloadFromS3(String bucket, String key, Path outFile) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            s3.getObject(GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build(),
                    outFile);

            System.out.println("[S3] Downloaded: " + key);
        }
    }


    // ===============================
    // UPLOAD TO S3
    // ===============================
    private void uploadToS3(String bucket, String key, String filePath) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build(),
                    RequestBody.fromFile(Paths.get(filePath)));

            System.out.println("[S3] Uploaded summary: " + key);
        }
    }


    // ===============================
    // PARSE INPUT FILE
    // ===============================
    private List<String[]> parseInput(Path path) throws IOException {
        List<String[]> list = new ArrayList<>();

        try (BufferedReader br = Files.newBufferedReader(path)) {
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] p = line.split("\\s+", 2);
                if (p.length == 2) list.add(p);
            }
        }

        return list;
    }


    // ===============================
    // SEND TASKS TO WORKERS
    // ===============================
    private void sendTasksToWorkerQueue(List<String[]> tasks) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            for (String[] task : tasks) {
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(WORKER_QUEUE_URL)
                        .messageBody(task[0] + ";" + task[1])
                        .build());
            }

            System.out.println("[SQS] Sent " + tasks.size() + " tasks to workers");
        }
    }


    // ===============================
    // SCALE WORKERS
    // ===============================
    private void ensureWorkerCount(int needed) {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            // count current workers
            DescribeInstancesResponse resp = ec2.describeInstances();

            long current = resp.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .filter(i -> i.imageId().equals(WORKER_AMI_ID))
                    .filter(i -> i.state().name().equals(InstanceStateName.RUNNING)
                            || i.state().name().equals(InstanceStateName.PENDING))
                    .count();

            int toLaunch = Math.min(MAX_WORKERS - (int) current, Math.max(0, needed - (int) current));

            if (toLaunch <= 0) {
                System.out.println("[EC2] Worker count OK: " + current);
                return;
            }

            System.out.println("[EC2] Launching " + toLaunch + " Workers");

            ec2.runInstances(RunInstancesRequest.builder()
                    .imageId(WORKER_AMI_ID)
                    .instanceType(InstanceType.T3_MICRO)
                    .minCount(1)
                    .maxCount(toLaunch)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .name("WorkerInstanceProfile")
                            .build())
                    .build());
        }
    }


    // ===============================
    // COLLECT WORKER RESULTS
    // ===============================
    
    private List<String[]> collectResults(int expected) {
        List<String[]> results = new ArrayList<>();

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[Manager] Waiting for " + expected + " worker results...");

            while (results.size() < expected) {

                ReceiveMessageResponse resp = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(WORKER_RESULTS_QUEUE_URL)
                                .maxNumberOfMessages(10)
                                .waitTimeSeconds(15)
                                .build()
                );

                for (Message m : resp.messages()) {
                    String[] parts = m.body().split(";", 3);
                    results.add(parts);

                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(WORKER_RESULTS_QUEUE_URL)
                            .receiptHandle(m.receiptHandle())
                            .build());
                }
            }

            System.out.println("[Manager] Received all worker results.");

        } catch (Exception e) {
            System.err.println("[Manager] Error collecting results: " + e.getMessage());
        }

        return results;
    }


    // ===============================
    // BUILD SUMMARY HTML
    // ===============================
    private void writeSummaryHTML(List<String[]> results, String filePath) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(Paths.get(filePath))) {

            w.write("<html><head><title>Summary</title></head><body>");
            w.write("<h1>Analysis Summary</h1>");
            w.write("<table border='1'>");
            w.write("<tr><th>Type</th><th>URL</th><th>Result</th></tr>");

            for (String[] r : results) {
                w.write("<tr>");
                w.write("<td>" + r[0] + "</td>");
                w.write("<td>" + r[1] + "</td>");
                w.write("<td>" + r[2] + "</td>");
                w.write("</tr>");
            }

            w.write("</table></body></html>");
        }
    }


    // ===============================
    // SEND SUMMARY BACK TO LOCALAPP
    // ===============================
    private void sendSummaryMessage(String localQueue, String key) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String msg = "SUMMARY_READY;" + S3_BUCKET + ";" + key;

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(localQueue)
                    .messageBody(msg)
                    .build());

            System.out.println("[SQS] Sent SUMMARY_READY to LocalApp.");
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

                    if (i.imageId().equals(WORKER_AMI_ID) &&
                            (i.state().name().equals(InstanceStateName.RUNNING)
                                    || i.state().name().equals(InstanceStateName.PENDING))) {
                        toKill.add(i.instanceId());
                    }
                }
            }

            if (!toKill.isEmpty()) {
                ec2.terminateInstances(TerminateInstancesRequest.builder()
                        .instanceIds(toKill)
                        .build());

                System.out.println("[EC2] Terminated workers: " + toKill.size());
            }

        } catch (Exception e) {
            System.err.println("[Manager] Worker termination error: " + e.getMessage());
        }
    }
}
