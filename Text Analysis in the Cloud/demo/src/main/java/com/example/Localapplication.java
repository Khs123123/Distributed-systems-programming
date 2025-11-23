package com.example;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.List;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.ec2.*;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

/**
 * LocalApplication – FINAL AWS VERSION with full distributed synchronization.
 *
 * Launch behavior:
 *   1. Check for ANY Manager instance:
 *        - tagged Project = TextAnalysisManager
 *        - in state: pending / running / stopping / stopped
 *   2. If ANY exists → DO NOT start new one.
 *   3. If none exists:
 *        - launch Manager
 *        - atomically tag ManagerStatus = Starting
 *   4. Wait until Manager changes its tag to ManagerStatus = Ready
 *
 * This prevents race conditions from multiple LocalApplications.
 */
public class Localapplication {

    private static final Region AWS_REGION = Region.US_EAST_1;


    // Replace with your REAL bucket
    private static final String S3_BUCKET = "khaled-text-analysis-bucket";

    // Replace with your REAL queues
    private static final String MANAGER_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/310078408001/manager-queue";
    private static final String LOCALAPP_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/310078408001/localapp-queue";


    // Replace with your real AMI ID
    private static final String MANAGER_AMI_ID = "ami-xxxxxxxxx";

    private static final String MANAGER_TAG_KEY = "Project";
    private static final String MANAGER_TAG_VALUE = "TextAnalysisManager";

    public static void main(String[] args) throws Exception {

        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: java com.example.Localapplication input.txt output.html n [terminate]");
            return;
        }

        String inputFileName  = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = args.length == 4 && args[3].equalsIgnoreCase("terminate");

        System.out.println("=== LocalApplication (AWS Mode) ===");

        // 1) Ensure Manager is running
        ensureManagerRunning();

        // 2) Upload input to S3
        String s3Key = "inputs/" + new File(inputFileName).getName();
        uploadToS3(inputFileName, s3Key);

        // 3) Send job message
        sendJobMessageToManager(s3Key, n);

        // 4) Wait for summary
        String summaryKey = waitForSummaryMessage();
        if (summaryKey == null) {
            System.err.println("[LocalApp] Timeout waiting for summary.");
            return;
        }

        // 5) Download summary
        downloadFromS3(summaryKey, outputFileName);

        // 6) Terminate?
        if (terminate) sendTerminateMessage();

        System.out.println("=== LocalApplication finished successfully ===");
    }

    // ============================================================
    //           🔵  ENSURE MANAGER RUNNING (Cloud Lock)
    // ============================================================
    private static void ensureManagerRunning() {

        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[EC2] Checking for existing Manager...");

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

            List<Instance> list = resp.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .toList();

            if (!list.isEmpty()) {
                System.out.println("[EC2] Manager already exists: " + list.get(0).instanceId());
                return;
            }

            // ========== No Manager → Launch new one ==========
            System.out.println("[EC2] Launching NEW Manager...");

            RunInstancesResponse run = ec2.runInstances(
                    RunInstancesRequest.builder()
                            .imageId(MANAGER_AMI_ID)
                            .instanceType(InstanceType.T3_MICRO)
                            .minCount(1)
                            .maxCount(1)
                            .tagSpecifications(
                                    TagSpecification.builder()
                                            .resourceType(ResourceType.INSTANCE)
                                            .tags(
                                                    Tag.builder()
                                                            .key(MANAGER_TAG_KEY)
                                                            .value(MANAGER_TAG_VALUE)
                                                            .build(),
                                                    Tag.builder()
                                                            .key("ManagerStatus")
                                                            .value("Starting")
                                                            .build()
                                            ).build()
                            )
                            .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                                    .name("ManagerInstanceProfile")
                                    .build())
                            .build()
            );

            String managerId = run.instances().get(0).instanceId();
            System.out.println("[EC2] Manager created: " + managerId);

            Ec2Waiter waiter = ec2.waiter();
            waiter.waitUntilInstanceRunning(
                    DescribeInstancesRequest.builder()
                            .instanceIds(managerId)
                            .build()
            );

            System.out.println("[EC2] Manager is RUNNING. Waiting for READY tag...");

            waitForManagerReadyTag(ec2, managerId);
            System.out.println("[EC2] Manager is READY.");

        } catch (Exception e) {
            System.err.println("[EC2] ensureManagerRunning ERROR: " + e.getMessage());
        }
    }

    // ============================================================
    //                WAIT FOR MANAGER READY TAG
    // ============================================================
    private static void waitForManagerReadyTag(Ec2Client ec2, String managerId) {

        for (int i = 0; i < 60; i++) { // up to 5 minutes
            DescribeInstancesResponse resp = ec2.describeInstances(
                    DescribeInstancesRequest.builder()
                            .instanceIds(managerId)
                            .build()
            );

            Instance inst = resp.reservations().get(0).instances().get(0);

            boolean ready = inst.tags().stream()
                    .anyMatch(t -> t.key().equals("ManagerStatus") && t.value().equals("Ready"));

            if (ready) return;

            try { Thread.sleep(5000); } catch (Exception ignore) {}
        }

        System.out.println("[EC2] WARNING: Manager did not set READY tag after 5 minutes.");
    }

    // ============================================================
    //                       S3 UPLOAD
    // ============================================================
    private static void uploadToS3(String fileName, String key) {
        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(AWS_REGION)
                .build()) {

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(S3_BUCKET)
                            .key(key)
                            .build(),
                    RequestBody.fromFile(Paths.get(fileName))
            );

            System.out.println("[S3] Uploaded file to s3://" + S3_BUCKET + "/" + key);

        } catch (Exception e) {
            System.err.println("[S3] ERROR: " + e.getMessage());
        }
    }

    // ============================================================
    //                      SEND JOB MESSAGE
    // ============================================================
    private static void sendJobMessageToManager(String s3Key, int n) {
        try (SqsClient sqs = SqsClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(AWS_REGION)
                .build()) {

            String body = "NEW_JOB;" + S3_BUCKET + ";" + s3Key + ";" + n + ";" + LOCALAPP_QUEUE_URL;

            sqs.sendMessage(
                    SendMessageRequest.builder()
                            .queueUrl(MANAGER_QUEUE_URL)
                            .messageBody(body)
                            .build()
            );

            System.out.println("[SQS] NEW_JOB message sent.");

        } catch (Exception e) {
            System.err.println("[SQS] ERROR sending job: " + e.getMessage());
        }
    }

    // ============================================================
    //                 WAIT FOR SUMMARY MESSAGE
    // ============================================================
    private static String waitForSummaryMessage() {

        try (SqsClient sqs = SqsClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(AWS_REGION)
                .build()) {

            System.out.println("[SQS] Waiting for summary...");

            Instant start = Instant.now();

            while (Duration.between(start, Instant.now()).toMinutes() < 25) {

                ReceiveMessageResponse resp = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(LOCALAPP_QUEUE_URL)
                                .waitTimeSeconds(15)
                                .maxNumberOfMessages(1)
                                .visibilityTimeout(20)
                                .build()
                );

                if (resp.messages().isEmpty()) continue;

                Message msg = resp.messages().get(0);

                if (msg.body().startsWith("SUMMARY_READY")) {
                    String[] parts = msg.body().split(";");
                    String key = parts[2];

                    sqs.deleteMessage(
                            DeleteMessageRequest.builder()
                                    .queueUrl(LOCALAPP_QUEUE_URL)
                                    .receiptHandle(msg.receiptHandle())
                                    .build()
                    );

                    System.out.println("[SQS] SUMMARY_ready → " + key);
                    return key;
                }
            }
        } catch (Exception e) {
            System.err.println("[SQS] ERROR receiving summary: " + e.getMessage());
        }

        return null;
    }

    // ============================================================
    //                   DOWNLOAD SUMMARY FROM S3
    // ============================================================
    private static void downloadFromS3(String key, String outputFile) {

        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(S3_BUCKET)
                            .key(key)
                            .build(),
                    Paths.get(outputFile)
            );

            System.out.println("[S3] Summary downloaded → " + outputFile);

        } catch (Exception e) {
            System.err.println("[S3] ERROR: " + e.getMessage());
        }
    }

    // ============================================================
    //                   SEND TERMINATE MESSAGE
    // ============================================================
    private static void sendTerminateMessage() {

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            sqs.sendMessage(
                    SendMessageRequest.builder()
                            .queueUrl(MANAGER_QUEUE_URL)
                            .messageBody("TERMINATE")
                            .build()
            );

            System.out.println("[SQS] TERMINATE message sent.");

        } catch (Exception e) {
            System.err.println("[SQS] ERROR sending terminate: " + e.getMessage());
        }
    }
}
