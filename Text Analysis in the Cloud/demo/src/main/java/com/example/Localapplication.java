// package com.example;

// import java.io.BufferedReader;
// import java.io.BufferedWriter;
// import java.io.File;
// import java.io.FileReader;
// import java.io.FileWriter;
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.UUID;

// /**
//  * Step 3: Local Application (local version, NO AWS yet).
//  *
//  * Usage:
//  *   java -jar demo-1.0-SNAPSHOT.jar inputFileName outputFileName n [terminate]
//  *
//  * What this version does:
//  *   - Parses arguments.
//  *   - Checks that the input file exists.
//  *   - Reads each line: <TYPE>\t<URL>
//  *   - Stores them as job requests.
//  *   - Generates a jobId (to be used later with Manager).
//  *   - Creates a SIMPLE HTML output file listing all the requests.
//  *
//  * NO:
//  *   - No S3
//  *   - No SQS
//  *   - No EC2
//  *
//  * This is just to "make sure the local application works"
//  * as the assignment's getting-started section says.
//  */
// public class Localapplication {

//     // Represents one line in the input file.
//     private static class JobRequest {
//         String analysisType;  // POS / CONSTITUENCY / DEPENDENCY
//         String url;           // URL of the text file

//         JobRequest(String analysisType, String url) {
//             this.analysisType = analysisType;
//             this.url = url;
//         }
//     }

//     public static void main(String[] args) {
//         try {
//             runLocalApplication(args);
//         } catch (Exception e) {
//             System.err.println("ERROR: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     private static void runLocalApplication(String[] args) throws IOException {
//         // 1. Parse and validate arguments
//         if (args.length < 3) {
//             System.out.println("Usage: java -jar demo-1.0-SNAPSHOT.jar inputFileName outputFileName n [terminate]");
//             return;
//         }

//         String inputFileName = args[0];
//         String outputFileName = args[1];
//         int n = Integer.parseInt(args[2]);  // max URLs per worker (used later by Manager)
//         boolean terminate = (args.length >= 4 && args[3].equalsIgnoreCase("terminate"));

//         System.out.println("=== LocalApplication (LOCAL VERSION, NO AWS) ===");
//         System.out.println("inputFileName  = " + inputFileName);
//         System.out.println("outputFileName = " + outputFileName);
//         System.out.println("n              = " + n);
//         System.out.println("terminate      = " + terminate);

//         // 2. Check input file exists
//         File inputFile = new File(inputFileName);
//         if (!inputFile.exists()) {
//             throw new IOException("Input file does not exist: " + inputFile.getAbsolutePath());
//         }
//         System.out.println("Input file found at: " + inputFile.getAbsolutePath());

//         // 3. Read and parse the input file lines
//         List<JobRequest> jobs = readJobRequests(inputFile);
//         System.out.println("Found " + jobs.size() + " job(s) in input file.");

//         if (jobs.isEmpty()) {
//             System.out.println("No jobs to process. Exiting.");
//             return;
//         }

//         // 4. Generate a jobId (used later to correlate with Manager)
//         String jobId = UUID.randomUUID().toString();
//         System.out.println("Generated jobId = " + jobId);

//         // 5. For now, we just SIMULATE what we would send to the Manager
//         simulateSendingToManager(jobs, jobId, n, terminate);

//         // 6. Create a SIMPLE HTML output locally to "make sure it works"
//         createDummyHtmlOutput(outputFileName, jobs, jobId, n, terminate);

//         System.out.println("Dummy HTML written to: " + new File(outputFileName).getAbsolutePath());
//         System.out.println("=== LocalApplication (local version) FINISHED ===");
//     }

//     /**
//      * Reads the input file and parses each line as:
//      *   <TYPE>\t<URL>
//      * where:
//      *   TYPE ∈ { POS, CONSTITUENCY, DEPENDENCY } (as per assignment).
//      */
//     private static List<JobRequest> readJobRequests(File inputFile) throws IOException {
//         List<JobRequest> jobs = new ArrayList<>();

//         try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
//             String line;
//             int lineNumber = 0;

//             while ((line = reader.readLine()) != null) {
//                 lineNumber++;

//                 line = line.trim();
//                 if (line.isEmpty()) {
//                     continue; // skip empty lines
//                 }

//                 // Expect "<TYPE> <URL>" (separated by whitespace: space or tab)
//                 String[] parts = line.split("\\s+", 2); // split on ANY whitespace
//                 if (parts.length != 2) {
//                     System.err.println("WARNING: Invalid line format at line " + lineNumber +
//                     ". Expected: <TYPE><whitespace><URL>. Got: " + line);
//                 continue;
//             }   


//                 String type = parts[0].trim();
//                 String url = parts[1].trim();

//                 // Just basic validation for now
//                 if (!(type.equals("POS") || type.equals("CONSTITUENCY") || type.equals("DEPENDENCY"))) {
//                     System.err.println("WARNING: Unknown analysis type at line " + lineNumber + ": " + type);
//                 }

//                 jobs.add(new JobRequest(type, url));
//             }
//         }

//         return jobs;
//     }

//     /**
//      * For now, just print what we *would* send to the Manager,
//      * but do NOT actually use AWS.
//      */
//     private static void simulateSendingToManager(List<JobRequest> jobs,
//                                                  String jobId,
//                                                  int n,
//                                                  boolean terminate) {
//         System.out.println("--- SIMULATION: This is what we'd send to Manager (later via SQS) ---");
//         System.out.println("NEW_JOB message:");
//         System.out.println("  jobId = " + jobId);
//         System.out.println("  total jobs = " + jobs.size());
//         System.out.println("  n (max URLs per worker) = " + n);
//         System.out.println("  terminate after job? = " + terminate);

//         System.out.println();
//         System.out.println("Each job line:");
//         int i = 0;
//         for (JobRequest job : jobs) {
//             System.out.println("  [" + (++i) + "] type=" + job.analysisType + ", url=" + job.url);
//         }

//         System.out.println("--------------------------------------------------------------------");
//     }

//     /**
//      * Creates a small dummy HTML file that lists all job requests.
//      * Later, this HTML will be actually created by the Manager from real Worker results.
//      */
//     private static void createDummyHtmlOutput(String outputFileName,
//                                               List<JobRequest> jobs,
//                                               String jobId,
//                                               int n,
//                                               boolean terminate) throws IOException {
//         File outFile = new File(outputFileName);

//         try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
//             writer.write("<!DOCTYPE html>\n");
//             writer.write("<html><head><meta charset=\"UTF-8\"><title>Dummy Summary</title></head><body>\n");
//             writer.write("<h1>Dummy Summary (LocalApplication Local Version)</h1>\n");
//             writer.write("<p><b>Job ID:</b> " + jobId + "</p>\n");
//             writer.write("<p><b>Parameter n:</b> " + n + "</p>\n");
//             writer.write("<p><b>Terminate:</b> " + terminate + "</p>\n");

//             writer.write("<h2>Requested Analyses</h2>\n");
//             writer.write("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
//             writer.write("<tr><th>#</th><th>Type</th><th>URL</th></tr>\n");

//             int index = 0;
//             for (JobRequest job : jobs) {
//                 index++;
//                 writer.write("<tr>");
//                 writer.write("<td>" + index + "</td>");
//                 writer.write("<td>" + escapeHtml(job.analysisType) + "</td>");
//                 writer.write("<td><a href=\"" + escapeHtml(job.url) + "\">" + escapeHtml(job.url) + "</a></td>");
//                 writer.write("</tr>\n");
//             }

//             writer.write("</table>\n");
//             writer.write("<p><i>This is a dummy summary created locally. Later, the real summary will be created by the Manager using Worker results from S3.</i></p>\n");
//             writer.write("</body></html>\n");
//         }
//     }

//     /**
//      * Very small helper to escape HTML special chars.
//      */
//     private static String escapeHtml(String s) {
//         if (s == null) return "";
//         return s.replace("&", "&amp;")
//                 .replace("\"", "&quot;")
//                 .replace("<", "&lt;")
//                 .replace(">", "&gt;");
//     }
// }


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
 * LocalApplication – FULL AWS VERSION (Final)
 *
 * Assignment responsibilities:
 *  1. Check/start Manager EC2.
 *  2. Upload input file to S3.
 *  3. Send message to Manager via SQS.
 *  4. Wait for summary message from Manager.
 *  5. Download summary file from S3.
 *  6. Optionally send terminate message.
 *
 * Usage:
 *   java com.example.Localapplication input.txt output.html n [terminate]
 */
public class Localapplication {

    private static final Region AWS_REGION = Region.EU_CENTRAL_1;

    // 🪣 Replace with your actual S3 bucket
    private static final String S3_BUCKET = "khaled-text-analysis-bucket";

    // 📬 Replace with your real SQS queue URLs
    private static final String MANAGER_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/manager-queue";
    private static final String LOCALAPP_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/localapp-queue";

    // ⚙️ EC2 config for Manager
    private static final String MANAGER_AMI_ID = "ami-xxxxxxxxxxxxxxx";
    private static final String MANAGER_TAG_KEY = "Project";
    private static final String MANAGER_TAG_VALUE = "TextAnalysisManager";

    public static void main(String[] args) throws Exception {

        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: java com.example.Localapplication input.txt output.html n [terminate]");
            return;
        }

        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = args.length == 4 && args[3].equalsIgnoreCase("terminate");

        System.out.println("=== LocalApplication (FULL AWS VERSION) ===");
        System.out.println("inputFileName  = " + inputFileName);
        System.out.println("outputFileName = " + outputFileName);
        System.out.println("n              = " + n);
        System.out.println("terminate      = " + terminate);

        // 1️⃣ Ensure Manager EC2 is running
        ensureManagerRunning();

        // 2️⃣ Upload input file to S3
        String s3Key = "inputs/" + new File(inputFileName).getName();
        uploadToS3(inputFileName, s3Key);

        // 3️⃣ Send job message to Manager
        sendJobMessageToManager(s3Key, n);

        // 4️⃣ Wait for summary message (with timeout)
        String summaryKey = waitForSummaryMessage();

        if (summaryKey == null) {
            System.err.println("[LocalApp] Timeout: no summary received after 30 minutes. Exiting.");
            return;
        }

        // 5️⃣ Download summary file from S3
        downloadFromS3(summaryKey, outputFileName);

        // 6️⃣ Optionally send terminate message
        if (terminate) {
            sendTerminateMessage();
        }

        System.out.println("=== LocalApplication finished successfully ✅ ===");
    }

    // ------------------------------------------------------
    // Step 1: Ensure Manager EC2 is running (with waiter)
    // ------------------------------------------------------
    private static void ensureManagerRunning() {
        try (Ec2Client ec2 = Ec2Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[EC2] Checking for running Manager...");

            DescribeInstancesResponse resp = ec2.describeInstances(DescribeInstancesRequest.builder()
                    .filters(Filter.builder().name("tag:" + MANAGER_TAG_KEY).values(MANAGER_TAG_VALUE).build())
                    .build());

            boolean isRunning = resp.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .anyMatch(i -> i.state().name().equals(InstanceStateName.RUNNING));

            if (isRunning) {
                System.out.println("[EC2] Manager is already running ✅");
                return;
            }

            System.out.println("[EC2] No running Manager found. Launching new instance...");

            RunInstancesResponse runResult = ec2.runInstances(RunInstancesRequest.builder()
                    .imageId(MANAGER_AMI_ID)
                    .instanceType(InstanceType.T3_MICRO)
                    .minCount(1).maxCount(1)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                            .name("ManagerInstanceProfile").build())
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.INSTANCE)
                            .tags(Tag.builder().key(MANAGER_TAG_KEY).value(MANAGER_TAG_VALUE).build())
                            .build())
                    .build());

            String managerId = runResult.instances().get(0).instanceId();
            Ec2Waiter waiter = ec2.waiter();

            waiter.waitUntilInstanceRunning(DescribeInstancesRequest.builder()
                    .instanceIds(managerId)
                    .build());

            System.out.println("[EC2] Manager is now running ✅");

        } catch (Ec2Exception e) {
            System.err.println("EC2 ERROR: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ------------------------------------------------------
    // Step 2: Upload input to S3
    // ------------------------------------------------------
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

            System.out.println("[S3] Uploaded file to s3://" + S3_BUCKET + "/" + key);

        } catch (S3Exception e) {
            System.err.println("S3 upload error: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ------------------------------------------------------
    // Step 3: Send SQS message to Manager
    // ------------------------------------------------------
    private static void sendJobMessageToManager(String s3Key, int n) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String message = "NEW_JOB;" + S3_BUCKET + ";" + s3Key + ";" + n + ";" + LOCALAPP_QUEUE_URL;
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(MANAGER_QUEUE_URL)
                    .messageBody(message)
                    .build());

            System.out.println("[SQS] Sent NEW_JOB message to Manager queue");

        } catch (SqsException e) {
            System.err.println("SQS send error: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ------------------------------------------------------
    // Step 4: Wait for summary message (with 30 min timeout)
    // ------------------------------------------------------
    private static String waitForSummaryMessage() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            System.out.println("[SQS] Waiting for summary message...");

            Instant start = Instant.now();
            while (Duration.between(start, Instant.now()).toMinutes() < 30) {

                ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(LOCALAPP_QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(15)
                        .visibilityTimeout(20)
                        .build());

                List<Message> messages = response.messages();
                if (messages.isEmpty()) continue;

                for (Message msg : messages) {
                    String body = msg.body();
                    if (body.startsWith("SUMMARY_READY")) {
                        String[] parts = body.split(";");
                        String s3Key = parts[2];
                        System.out.println("[SQS] Received SUMMARY_READY for: " + s3Key);

                        // Delete after reading
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(LOCALAPP_QUEUE_URL)
                                .receiptHandle(msg.receiptHandle())
                                .build());

                        return s3Key;
                    }
                }
            }

        } catch (SqsException e) {
            System.err.println("SQS receive error: " + e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    // ------------------------------------------------------
    // Step 5: Download summary file from S3
    // ------------------------------------------------------
    private static void downloadFromS3(String key, String outputFile) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            s3.getObject(GetObjectRequest.builder()
                            .bucket(S3_BUCKET)
                            .key(key)
                            .build(),
                    Paths.get(outputFile));

            System.out.println("[S3] Downloaded summary → " + outputFile);
            System.out.println("Summary URL: https://s3.console.aws.amazon.com/s3/object/"
                    + S3_BUCKET + "/" + key);

        } catch (S3Exception e) {
            System.err.println("S3 download error: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ------------------------------------------------------
    // Step 6: Send terminate message
    // ------------------------------------------------------
    private static void sendTerminateMessage() {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(MANAGER_QUEUE_URL)
                    .messageBody("TERMINATE")
                    .build());

            System.out.println("[SQS] Sent TERMINATE message to Manager queue");

        } catch (SqsException e) {
            System.err.println("SQS terminate error: " + e.awsErrorDetails().errorMessage());
        }
    }
}
