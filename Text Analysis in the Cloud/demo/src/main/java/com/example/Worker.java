package com.example;

import com.google.gson.Gson;

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


public class Worker {

    private static final Region AWS_REGION = Region.US_EAST_1;

    private static final String S3_BUCKET = "khaled-text-analysis-bucket-v3";

    private static final String RESULTS_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/597918329386/worker-results-queue";

    private static final String WORKER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/597918329386/worker-queue";

    private static final Gson GSON = new Gson();

    // JSON message classes
    private static class WorkerTaskMessage { String type; String analysisType; String url; }
    private static class ResultMessage { String type = "RESULT"; String analysisType; String url; String s3Key; }
    private static class ErrorMessage { String type = "ERROR"; String originalMessage; String error; }

    // NLP PIPELINE 
    private static final StanfordCoreNLP pipeline = createPipeline();

    private static StanfordCoreNLP createPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,parse,depparse");
        System.out.println("[Worker] Loading Stanford CoreNLP...");
        return new StanfordCoreNLP(props);
    }

    public static void main(String[] args) {
        System.out.println("=== Worker started (Production) ===");

        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            while (true) {
                // Long polling for messages
                ReceiveMessageResponse response = sqs.receiveMessage(
                        ReceiveMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .maxNumberOfMessages(1)
                                .waitTimeSeconds(15)
                                .visibilityTimeout(3600) // 30 mins visibility
                                .build());

                for (Message msg : response.messages()) {
                    try {
                        processTask(msg.body());

                        // Delete on success
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(WORKER_QUEUE_URL)
                                .receiptHandle(msg.receiptHandle())
                                .build());

                    } catch (Exception e) {
                        System.err.println("[Worker] Task error: " + e);

                        // Capture and Clean Error String
                        String fullError = e.toString();
                        int urlIndex = fullError.indexOf("http");
                        
                        String cleanError;
                        if (urlIndex > 0) {
                            cleanError = fullError.substring(0, urlIndex).trim(); 
                        } else {
                            cleanError = fullError;
                        }

                        // Send the CLEAN error description to the Manager
                        sendErrorMessage(msg.body(), cleanError);

                        // Delete to prevent infinite loops on bad messages
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


    // MAIN TASK HANDLING
    private static void processTask(String message) throws Exception {
        WorkerTaskMessage task = GSON.fromJson(message, WorkerTaskMessage.class);
        if (task == null || task.type == null || !"TASK".equals(task.type)) {
            throw new IllegalArgumentException("Invalid task JSON: " + message);
        }

        String type = task.analysisType != null ? task.analysisType.trim() : "";
        String url = task.url != null ? task.url.trim() : "";

        if (type.isEmpty() || url.isEmpty()) {
            throw new IllegalArgumentException("Missing analysisType or url in task JSON");
        }

        String text = downloadText(url);

        // Output file
        String localResult = "/tmp/" + UUID.randomUUID() + "_analysis.txt";
        BufferedWriter writer = Files.newBufferedWriter(Paths.get(localResult));

        // Split into lines (line-by-line)
        String[] lines = text.split("\n");

        for (String line : lines) {
            if (line.trim().isEmpty()) continue;
            writer.write(">>> LINE >>> " + line + "\n");
            // Safely analyze each line
            String analysis = safeAnalyzeLine(line, type);
            writer.write(analysis + "\n\n");
        }

        writer.close();

        // Upload to S3
        String s3Key = "results/" + Paths.get(localResult).getFileName();
        uploadToS3(localResult, s3Key);

        // Notify Manager (JSON)
        sendResultMessage(type, url, s3Key);

        System.out.println("[Worker] Completed task: " + type + " for " + url);
    }

    // DOWNLOAD TEXT SAFELY
    private static String downloadText(String urlString) throws IOException {
        StringBuilder sb = new StringBuilder();
        URL url = new URL(urlString);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    // SAFE LINE-BY-LINE ANALYSIS
    private static String safeAnalyzeLine(String line, String type) {
        try {
            Annotation doc = new Annotation(line);
            pipeline.annotate(doc);

            switch (type.toUpperCase()) {
                case "POS":
                    return posToString(doc);
                case "CONSTITUENCY":
                    return constituencyToString(doc);
                case "DEPENDENCY":
                    return dependencyToString(doc);
                default:
                    return "[ERROR] Unknown TYPE " + type;
            }
        } catch (Exception e) {
            return "[ERROR parsing line] " + e.getMessage();
        }
    }

    // NLP OUTPUT FUNCTIONS
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

    // S3 UPLOAD
    private static void uploadToS3(String filePath, String key) {
        try (S3Client s3 = S3Client.builder().region(AWS_REGION).credentialsProvider(DefaultCredentialsProvider.create()).build()) {

            s3.putObject(PutObjectRequest.builder()
                        .bucket(S3_BUCKET)
                        .key(key)
                        .build(),
                RequestBody.fromFile(Paths.get(filePath)));

        } catch (S3Exception e) {
            System.err.println("[Worker] S3 upload error: " + e.awsErrorDetails().errorMessage());
        }
    }

    // SEND RESULT TO MANAGER (JSON)
    private static void sendResultMessage(String type, String url, String s3Key) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            ResultMessage r = new ResultMessage();
            r.analysisType = type;
            r.url = url;
            r.s3Key = s3Key;

            String body = GSON.toJson(r);

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(RESULTS_QUEUE_URL)
                    .messageBody(body)
                    .build());

        } catch (SqsException e) {
            System.err.println("[Worker] Error sending result: " + e.awsErrorDetails().errorMessage());
        }
    }
     
    // SEND ERROR MESSAGE (JSON)
    private static void sendErrorMessage(String original, String error) {
        try (SqsClient sqs = SqsClient.builder()
                .region(AWS_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {

            ErrorMessage em = new ErrorMessage();
            em.originalMessage = original;
            em.error = error;

            String body = GSON.toJson(em);

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(RESULTS_QUEUE_URL)
                    .messageBody(body)
                    .build());

        } catch (SqsException e) {
            System.err.println("[Worker] Error sending ERROR: " + e.awsErrorDetails().errorMessage());
        }
    }
}
