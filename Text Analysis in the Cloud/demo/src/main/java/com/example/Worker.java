package com.example;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Worker (AWS version)
 *
 * Life cycle:
 *   1. Poll messages from worker SQS queue.
 *   2. For each message:
 *      - Parse: TYPE;URL
 *      - Download text
 *      - Run Stanford CoreNLP analysis
 *      - Upload result to S3
 *      - Send result message to results SQS
 *      - Delete message from input queue
 *   3. On errors:
 *      - Send error message to results SQS
 *      - Continue to next message
 *
 * The worker runs indefinitely on its EC2 instance until terminated by the Manager.
 */
public class Worker {

    // ⚙️ AWS Configuration
    private static final Region AWS_REGION = Region.EU_CENTRAL_1;

    // 🪣 Replace with your bucket and SQS URLs
    private static final String S3_BUCKET = "khaled-text-analysis-bucket";
    private static final String WORKER_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-queue";
    private static final String RESULTS_QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-results-queue";

    // 🧠 Stanford NLP pipeline
    private static final StanfordCoreNLP pipeline = createPipeline();

    private static StanfordCoreNLP createPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,parse,depparse");
        System.out.println("[Worker] Loading Stanford CoreNLP models...");
        return new StanfordCoreNLP(props);
    }

    public static void main(String[] args) {
        System.out.println("=== Worker started (AWS version) ===");

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            while (true) {
                ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(WORKER_QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(15)
                        .visibilityTimeout(60)
                        .build());

                List<Message> messages = response.messages();
                if (messages.isEmpty()) continue;

                for (Message msg : messages) {
                    String body = msg.body();
                    System.out.println("[Worker] Received task: " + body);

                    try {
                        handleTask(body);
                        // Remove successfully processed message
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .receiptHandle(msg.receiptHandle())
                                .build());
                    } catch (Exception e) {
                        System.err.println("[Worker] Error processing message: " + e.getMessage());
                        sendErrorMessage(body, e.getMessage());
                        // Still delete it to avoid infinite retries
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .receiptHandle(msg.receiptHandle())
                                .build());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[Worker] FATAL ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ------------------------------------------------------
    // Task processing logic
    // ------------------------------------------------------
    private static void handleTask(String messageBody) throws Exception {
        String[] parts = messageBody.split(";", 2);
        if (parts.length != 2)
            throw new IllegalArgumentException("Invalid message format: expected <TYPE>;<URL>");

        String type = parts[0].trim();
        String url = parts[1].trim();

        // Download text from the URL
        String text = downloadText(url);

        // Analyze text
        String result = analyzeText(text, type);

        // Save result to temporary file
        String localResultFile = "/tmp/result_" + UUID.randomUUID() + ".txt";
        Files.write(Paths.get(localResultFile), result.getBytes());

        // Upload to S3
        String s3Key = "results/" + Paths.get(localResultFile).getFileName();
        uploadToS3(localResultFile, s3Key);

        // Send result message to Manager
        sendResultMessage(type, url, s3Key);
        System.out.println("[Worker] Completed " + type + " for " + url);
    }

    // ------------------------------------------------------
    // Download text from URL (safe size)
    // ------------------------------------------------------
    private static String downloadText(String urlString) throws IOException {
        StringBuilder sb = new StringBuilder();
        URL url = new URL(urlString);

        final int MAX_LINES = 80;
        final int MAX_CHARS = 8000;

        try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;
            int lines = 0;
            while ((line = in.readLine()) != null) {
                if (lines >= MAX_LINES || sb.length() >= MAX_CHARS) {
                    sb.append("\n[TRUNCATED]\n");
                    break;
                }
                sb.append(line).append("\n");
                lines++;
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------
    // Run analysis
    // ------------------------------------------------------
    private static String analyzeText(String text, String typeStr) {
        Annotation doc = new Annotation(text);
        pipeline.annotate(doc);

        switch (typeStr.toUpperCase()) {
            case "POS":
                return posToString(doc);
            case "CONSTITUENCY":
                return constituencyToString(doc);
            case "DEPENDENCY":
                return dependencyToString(doc);
            default:
                return "[ERROR] Unknown analysis type: " + typeStr;
        }
    }

    private static String posToString(Annotation doc) {
        StringBuilder sb = new StringBuilder();
        for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                sb.append(token.word()).append("[").append(token.tag()).append("] ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private static String constituencyToString(Annotation doc) {
        StringWriter sw = new StringWriter();
        for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            tree.pennPrint(new PrintWriter(sw));
            sw.write("\n");
        }
        return sw.toString();
    }

    private static String dependencyToString(Annotation doc) {
        StringBuilder sb = new StringBuilder();
        for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
            SemanticGraph dependencies = sentence.get(
                    SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
            sb.append(dependencies.toString(SemanticGraph.OutputFormat.LIST));
            sb.append("\n\n");
        }
        return sb.toString();
    }

    // ------------------------------------------------------
    // Upload result file to S3
    // ------------------------------------------------------
    private static void uploadToS3(String filePath, String key) {
        try (S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            s3.putObject(PutObjectRequest.builder()
                            .bucket(S3_BUCKET)
                            .key(key)
                            .build(),
                    RequestBody.fromFile(Paths.get(filePath)));

            System.out.println("[S3] Uploaded analysis result to s3://" + S3_BUCKET + "/" + key);
        } catch (S3Exception e) {
            System.err.println("[S3] Upload error: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ------------------------------------------------------
    // Send success message to results SQS
    // ------------------------------------------------------
    private static void sendResultMessage(String type, String url, String s3Key) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String msg = "RESULT;" + type + ";" + url + ";s3://" + S3_BUCKET + "/" + s3Key;
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(RESULTS_QUEUE_URL)
                    .messageBody(msg)
                    .build());

        } catch (SqsException e) {
            System.err.println("[SQS] Error sending result: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ------------------------------------------------------
    // Send error message to results SQS
    // ------------------------------------------------------
    private static void sendErrorMessage(String originalBody, String errorText) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String msg = "ERROR;" + originalBody + ";Reason=" + errorText;
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(RESULTS_QUEUE_URL)
                    .messageBody(msg)
                    .build());

        } catch (SqsException e) {
            System.err.println("[SQS] Failed to send error message: " + e.awsErrorDetails().errorMessage());
        }
    }
}
