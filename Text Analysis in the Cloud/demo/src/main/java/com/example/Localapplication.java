package com.example;

import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Base64;

import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.ec2.*;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

public class Localapplication {

    private static final Region AWS_REGION = Region.US_EAST_1;

    private static final String S3_BUCKET = "khaled-text-analysis-bucket";

    private static final String MANAGER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/310078408001/manager-queue";

    private static final String LOCALAPP_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/310078408001/localapp-queue";

    // Tag definitions
    private static final String MANAGER_TAG_KEY = "Project";
    private static final String MANAGER_TAG_VALUE = "TextAnalysisManager";
    private static final String MANAGER_STATUS_TAG = "ManagerStatus";

    // Manager AMI (must exist in your account)
    private static final String MANAGER_AMI_ID = "ami-00e95a9222311e8ed";

    private static final Gson GSON = new Gson();

    // ----------- JSON message classes -----------
    private static class JobMessage {
        String type = "NEW_JOB";
        String bucket;
        String inputKey;
        int n;
        String callbackQueueUrl;
    }

    private static class SummaryMessage {
        String type;
        String bucket;
        String summaryKey;
    }

    private static class TerminateMessage {
        String type = "TERMINATE";
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: java -jar text-analysis-1.0-SNAPSHOT-local.jar input.txt output.html n [terminate]");
            return;
        }

        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = args.length == 4;

        System.out.println("=== LocalApplication (AWS Mode) ===");

        String managerId = ensureManagerRunning();
        System.out.println("[LocalApp] Using Manager: " + managerId);

        String s3Key = "inputs/" + new File(inputFileName).getName();
        uploadToS3(inputFileName, s3Key);

        sendJobMessageToManager(s3Key, n);

        String summaryKey = waitForSummaryMessage();
        if (summaryKey == null) {
            System.err.println("[LocalApp] ERROR: Timeout waiting for summary");
            return;
        }

        downloadFromS3(summaryKey, outputFileName);

        if (terminate) {
            sendTerminateMessage();
        }

        System.out.println("=== LocalApplication Done ===");
    }


    // =========================================================================
    // ENSURE MANAGER EXISTS – launch with UNIVERSAL user-data that starts Manager.jar
    // =========================================================================
    private static String ensureManagerRunning() {

        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[EC2] Searching for Manager instance...");

            // Find ANY instance tagged as Manager
            DescribeInstancesResponse resp = ec2.describeInstances(
                    DescribeInstancesRequest.builder()
                            .filters(
                                    Filter.builder()
                                            .name("tag:" + MANAGER_TAG_KEY)
                                            .values(MANAGER_TAG_VALUE)
                                            .build(),
                                    Filter.builder()
                                            .name("instance-state-name")
                                            .values("pending", "running", "stopping", "stopped")
                                            .build()
                            ).build()
            );

            List<Instance> found = resp.reservations()
                    .stream()
                    .flatMap(r -> r.instances().stream())
                    .collect(Collectors.toList());

            // If found, start if needed
            if (!found.isEmpty()) {
                Instance inst = found.get(0);
                String id = inst.instanceId();

                System.out.println("[EC2] Manager exists: " + id);

                if (inst.state().name().equals(InstanceStateName.STOPPED)) {
                    ec2.startInstances(StartInstancesRequest.builder()
                            .instanceIds(id).build());
                    System.out.println("[EC2] Starting stopped Manager…");

                    ec2.waiter().waitUntilInstanceRunning(
                            DescribeInstancesRequest.builder().instanceIds(id).build()
                    );
                }

                waitForManagerReadyTag(ec2, id);
                return id;
            }

            // ========== Otherwise launch new Manager with user-data ==========
            System.out.println("[EC2] No Manager found → Creating one...");

            // 🔥 UNIVERSAL SCRIPT (Works on both Ubuntu & Amazon Linux)
            String managerUserDataScript =
                    "#!/bin/bash\n" +
                    "exec > /var/log/user-data.log 2>&1\n" +
                    "echo '=== USER-DATA START ==='\n" +
                    "\n" +
                    "# 1. Detect OS and install Java/AWS CLI\n" +
                    "if command -v apt-get &> /dev/null; then\n" +
                    "    echo 'Detected Ubuntu'\n" +
                    "    apt-get update -y\n" +
                    "    apt-get install -y default-jre awscli\n" +
                    "    USER_HOME=\"/home/ubuntu\"\n" +
                    "elif command -v yum &> /dev/null; then\n" +
                    "    echo 'Detected Amazon Linux'\n" +
                    "    yum update -y\n" +
                    "    yum install -y java-1.8.0-openjdk awscli\n" +
                    "    USER_HOME=\"/home/ec2-user\"\n" +
                    "else\n" +
                    "    echo 'Unknown OS'\n" +
                    "    exit 1\n" +
                    "fi\n" +
                    "\n" +
                    "# 2. Setup App Directory\n" +
                    "mkdir -p $USER_HOME/app\n" +
                    "cd $USER_HOME/app\n" +
                    "\n" +
                    "# 3. Download Manager JAR\n" +
                    "echo 'Downloading manager.jar from S3...'\n" +
                    "aws s3 cp s3://khaled-text-analysis-bucket/jars/text-analysis-1.0-SNAPSHOT-remote.jar manager.jar\n" +
                    "\n" +
                    "# 4. Run Manager\n" +
                    "echo 'Starting Manager java process...'\n" +
                    "nohup java -jar manager.jar > manager.log 2>&1 &\n" +
                    "echo '=== USER-DATA END ==='\n";

            String managerUserDataBase64 =
                    Base64.getEncoder().encodeToString(managerUserDataScript.getBytes(StandardCharsets.UTF_8));

            RunInstancesResponse run = ec2.runInstances(
                    RunInstancesRequest.builder()
                            .imageId(MANAGER_AMI_ID)
                            .instanceType(InstanceType.T3_MEDIUM) // Ensuring 4GB RAM to prevent freeze
                            .minCount(1)
                            .maxCount(1)
                            .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                                    .name("LabInstanceProfile")
                                    .build())
                            .tagSpecifications(
                                    TagSpecification.builder()
                                            .resourceType(ResourceType.INSTANCE)
                                            .tags(
                                                    Tag.builder().key(MANAGER_TAG_KEY)
                                                            .value(MANAGER_TAG_VALUE).build(),
                                                    Tag.builder().key(MANAGER_STATUS_TAG)
                                                            .value("Starting").build()
                                            )
                                            .build()
                            )
                            .userData(managerUserDataBase64)
                            .build()
            );

            String newId = run.instances().get(0).instanceId();
            System.out.println("[EC2] Manager launched: " + newId);

            // Wait until running
            ec2.waiter().waitUntilInstanceRunning(
                    DescribeInstancesRequest.builder()
                            .instanceIds(newId)
                            .build()
            );

            // Tag “Ready” is still optional; we keep the old waiting loop
            waitForManagerReadyTag(ec2, newId);

            return newId;

        } catch (Exception e) {
            throw new RuntimeException("[EC2] Manager startup error: " + e.getMessage());
        }
    }


    // =========================================================================
    // WAIT FOR ManagerStatus = Ready TAG (NO-OP NOW)
    // =========================================================================
    private static void waitForManagerReadyTag(Ec2Client ec2, String managerId) {
        // We no longer rely on the ManagerStatus tag.
        // Manager is started automatically via user-data, so just log and return.
        System.out.println("[EC2] Skipping ManagerStatus=Ready check (not used).");
    }

    // =========================================================================
    // S3 UPLOAD
    // =========================================================================
    private static void uploadToS3(String fileName, String key) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            s3.putObject(PutObjectRequest.builder()
                            .bucket(S3_BUCKET)
                            .key(key)
                            .build(),
                    RequestBody.fromFile(Paths.get(fileName)));

            System.out.println("[S3] Uploaded input file.");
        }
    }

    // =========================================================================
    // SEND JOB MESSAGE (JSON)
    // =========================================================================
    private static void sendJobMessageToManager(String s3Key, int n) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            JobMessage job = new JobMessage();
            job.bucket = S3_BUCKET;
            job.inputKey = s3Key;
            job.n = n;
            job.callbackQueueUrl = LOCALAPP_QUEUE_URL;

            String body = GSON.toJson(job);

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(MANAGER_QUEUE_URL)
                    .messageBody(body)
                    .build());

            System.out.println("[SQS] Job (JSON) sent to Manager.");
        }
    }

    // =========================================================================
    // WAIT FOR SUMMARY MESSAGE (JSON)
    // =========================================================================
    private static String waitForSummaryMessage() {

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[SQS] Waiting for summary...");

            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < Duration.ofMinutes(25).toMillis()) {

                ReceiveMessageResponse resp = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(LOCALAPP_QUEUE_URL)
                                .waitTimeSeconds(15)
                                .visibilityTimeout(20)
                                .maxNumberOfMessages(1)
                                .build()
                );

                if (resp.messages().isEmpty())
                    continue;

                Message m = resp.messages().get(0);
                String body = m.body();

                SummaryMessage summary;
                try {
                    summary = GSON.fromJson(body, SummaryMessage.class);
                } catch (Exception e) {
                    summary = null;
                }

                if (summary == null || summary.type == null || !summary.type.equals("SUMMARY")) {
                    // Note: Manager sends "SUMMARY", check Manager.java to match exact string
                    // (Assuming Manager sends "SUMMARY" based on previous context, but check if it's "SUMMARY_READY")
                    // Adjusting to match Manager.java if needed.
                    if (summary != null && "SUMMARY".equals(summary.type)) {
                        // Correct type
                    } else if (summary != null && "SUMMARY_READY".equals(summary.type)) {
                        // Also acceptable if Manager uses this
                    } else {
                        continue;
                    }
                }

                String key = summary.summaryKey;

                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(LOCALAPP_QUEUE_URL)
                        .receiptHandle(m.receiptHandle())
                        .build());

                System.out.println("[SQS] Summary received (JSON).");
                return key;
            }
        }

        return null;
    }

    // =========================================================================
    // DOWNLOAD SUMMARY (Updated to overwrite existing file)
    // =========================================================================
        private static void downloadFromS3(String key, String outFile) {
            try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            // 🔥 FIX: Delete the file if it exists so we don't crash
            Files.deleteIfExists(Paths.get(outFile)); 

            s3.getObject(
                GetObjectRequest.builder()
                        .bucket(S3_BUCKET)
                        .key(key)
                        .build(),
                Paths.get(outFile)
            );

            System.out.println("[S3] Summary downloaded → " + outFile);
        } catch (Exception e) {
            System.err.println("[LocalApp] Error downloading summary: " + e.getMessage());
        }
    }

    // =========================================================================
    // SEND TERMINATE MESSAGE (JSON)
    // =========================================================================
    private static void sendTerminateMessage() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            TerminateMessage t = new TerminateMessage();
            String body = GSON.toJson(t);

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(MANAGER_QUEUE_URL)
                    .messageBody(body)
                    .build());

            System.out.println("[SQS] TERMINATE (JSON) sent.");
        }
    }
}