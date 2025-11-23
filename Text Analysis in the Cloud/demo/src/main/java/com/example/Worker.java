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

/**
 * Worker (AWS Version)
 *
 * - Polls messages from Worker SQS queue.
 * - Each message: TYPE ; URL
 * - Downloads the text from the URL.
 * - Runs Stanford CoreNLP analysis.
 * - Uploads result file to S3.
 * - Sends message to Manager results queue: TYPE ; URL ; S3_KEY
 * - Deletes original worker queue message.
 * - On error: sends ERROR message and deletes the task.
 */

public class Worker {

    private static final Region AWS_REGION = Region.US_EAST_1;


    // ---------------- AWS CONFIG ----------------
    private static final String S3_BUCKET = "khaled-text-analysis-bucket";
    private static final String WORKER_QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-queue";
    private static final String RESULTS_QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/XXXXXXX/worker-results-queue";

    // ---------------- NLP PIPELINE ----------------
    private static final StanfordCoreNLP pipeline = createPipeline();

    private static StanfordCoreNLP createPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,parse,depparse");
        System.out.println("[Worker] Loading Stanford CoreNLP models...");
        return new StanfordCoreNLP(props);
    }

    public static void main(String[] args) {
        System.out.println("=== Worker started (AWS) ===");

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            while (true) {

                ReceiveMessageResponse response = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .maxNumberOfMessages(1)
                                .waitTimeSeconds(15)
                                .visibilityTimeout(60)
                                .build());

                for (Message msg : response.messages()) {

                    try {
                        processTask(msg.body());

                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .receiptHandle(msg.receiptHandle())
                                .build());

                    } catch (Exception e) {
                        System.err.println("[Worker] Task error: " + e);

                        sendErrorMessage(msg.body(), e.getMessage());

                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .receiptHandle(msg.receiptHandle())
                                .build());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[Worker] FATAL ERROR: " + e);
        }
    }

    // ----------------------------------------------------------
    // MAIN TASK HANDLING
    // ----------------------------------------------------------
    private static void processTask(String message) throws Exception {
        String[] parts = message.split(";", 2);
        if (parts.length != 2)
            throw new IllegalArgumentException("Invalid message format: TYPE;URL");

        String type = parts[0].trim();
        String url = parts[1].trim();

        String text = downloadText(url);
        String analysis = analyzeText(text, type);

        String localResult = "/tmp/" + UUID.randomUUID() + "_analysis.txt";
        Files.write(Paths.get(localResult), analysis.getBytes());

        String s3Key = "results/" + Paths.get(localResult).getFileName();

        uploadToS3(localResult, s3Key);

        sendResultMessage(type, url, s3Key);

        System.out.println("[Worker] Completed: " + type + " for " + url);
    }

    // ----------------------------------------------------------
    // DOWNLOAD TEXT SAFELY
    // ----------------------------------------------------------
    private static String downloadText(String urlString) throws IOException {
        StringBuilder sb = new StringBuilder();
        URL url = new URL(urlString);

        final int MAX_LINES = 80;
        final int MAX_CHARS = 8000;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;
            int lines = 0;

            while ((line = br.readLine()) != null) {
                if (sb.length() >= MAX_CHARS || lines >= MAX_LINES) {
                    sb.append("\n[TRUNCATED]\n");
                    break;
                }
                sb.append(line).append("\n");
                lines++;
            }
        }

        return sb.toString();
    }

    // ----------------------------------------------------------
    // NLP ANALYSIS
    // ----------------------------------------------------------
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
                return "[ERROR] Unknown type: " + typeStr;
        }
    }

    private static String posToString(Annotation doc) {
        StringBuilder sb = new StringBuilder();
        for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class))
                sb.append(token.word()).append("[").append(token.tag()).append("] ");
            sb.append("\n");
        }
        return sb.toString();
    }

    private static String constituencyToString(Annotation doc) {
        StringWriter sw = new StringWriter();
        for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            tree.pennPrint(new PrintWriter(sw));
            sw.append("\n");
        }
        return sw.toString();
    }

    private static String dependencyToString(Annotation doc) {
        StringBuilder sb = new StringBuilder();
        for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
            SemanticGraph g = sentence.get(
                    SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
            sb.append(g.toString(SemanticGraph.OutputFormat.LIST));
            sb.append("\n\n");
        }
        return sb.toString();
    }

    // ----------------------------------------------------------
    // S3 UPLOAD
    // ----------------------------------------------------------
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

        } catch (S3Exception e) {
            System.err.println("[Worker] S3 upload error: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ----------------------------------------------------------
    // SEND RESULTS TO MANAGER
    // ----------------------------------------------------------
    private static void sendResultMessage(String type, String url, String s3Key) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String msg = type + ";" + url + ";" + s3Key;

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(RESULTS_QUEUE_URL)
                    .messageBody(msg)
                    .build());

        } catch (SqsException e) {
            System.err.println("[Worker] Error sending result: " + e.awsErrorDetails().errorMessage());
        }
    }

    // ----------------------------------------------------------
    // SEND ERROR TO MANAGER
    // ----------------------------------------------------------
    private static void sendErrorMessage(String original, String error) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            String msg = "ERROR;" + original + ";" + error;

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(RESULTS_QUEUE_URL)
                    .messageBody(msg)
                    .build());

        } catch (SqsException e) {
            System.err.println("[Worker] Error sending ERROR: " + e.awsErrorDetails().errorMessage());
        }
    }
}
