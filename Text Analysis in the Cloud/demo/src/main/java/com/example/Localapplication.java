package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Step 3: Local Application (local version, NO AWS yet).
 *
 * Usage:
 *   java -jar demo-1.0-SNAPSHOT.jar inputFileName outputFileName n [terminate]
 *
 * What this version does:
 *   - Parses arguments.
 *   - Checks that the input file exists.
 *   - Reads each line: <TYPE>\t<URL>
 *   - Stores them as job requests.
 *   - Generates a jobId (to be used later with Manager).
 *   - Creates a SIMPLE HTML output file listing all the requests.
 *
 * NO:
 *   - No S3
 *   - No SQS
 *   - No EC2
 *
 * This is just to "make sure the local application works"
 * as the assignment's getting-started section says.
 */
public class Localapplication {

    // Represents one line in the input file.
    private static class JobRequest {
        String analysisType;  // POS / CONSTITUENCY / DEPENDENCY
        String url;           // URL of the text file

        JobRequest(String analysisType, String url) {
            this.analysisType = analysisType;
            this.url = url;
        }
    }

    public static void main(String[] args) {
        try {
            runLocalApplication(args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runLocalApplication(String[] args) throws IOException {
        // 1. Parse and validate arguments
        if (args.length < 3) {
            System.out.println("Usage: java -jar demo-1.0-SNAPSHOT.jar inputFileName outputFileName n [terminate]");
            return;
        }

        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);  // max URLs per worker (used later by Manager)
        boolean terminate = (args.length >= 4 && args[3].equalsIgnoreCase("terminate"));

        System.out.println("=== LocalApplication (LOCAL VERSION, NO AWS) ===");
        System.out.println("inputFileName  = " + inputFileName);
        System.out.println("outputFileName = " + outputFileName);
        System.out.println("n              = " + n);
        System.out.println("terminate      = " + terminate);

        // 2. Check input file exists
        File inputFile = new File(inputFileName);
        if (!inputFile.exists()) {
            throw new IOException("Input file does not exist: " + inputFile.getAbsolutePath());
        }
        System.out.println("Input file found at: " + inputFile.getAbsolutePath());

        // 3. Read and parse the input file lines
        List<JobRequest> jobs = readJobRequests(inputFile);
        System.out.println("Found " + jobs.size() + " job(s) in input file.");

        if (jobs.isEmpty()) {
            System.out.println("No jobs to process. Exiting.");
            return;
        }

        // 4. Generate a jobId (used later to correlate with Manager)
        String jobId = UUID.randomUUID().toString();
        System.out.println("Generated jobId = " + jobId);

        // 5. For now, we just SIMULATE what we would send to the Manager
        simulateSendingToManager(jobs, jobId, n, terminate);

        // 6. Create a SIMPLE HTML output locally to "make sure it works"
        createDummyHtmlOutput(outputFileName, jobs, jobId, n, terminate);

        System.out.println("Dummy HTML written to: " + new File(outputFileName).getAbsolutePath());
        System.out.println("=== LocalApplication (local version) FINISHED ===");
    }

    /**
     * Reads the input file and parses each line as:
     *   <TYPE>\t<URL>
     * where:
     *   TYPE ∈ { POS, CONSTITUENCY, DEPENDENCY } (as per assignment).
     */
    private static List<JobRequest> readJobRequests(File inputFile) throws IOException {
        List<JobRequest> jobs = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;

                line = line.trim();
                if (line.isEmpty()) {
                    continue; // skip empty lines
                }

                // Expect "<TYPE> <URL>" (separated by whitespace: space or tab)
                String[] parts = line.split("\\s+", 2); // split on ANY whitespace
                if (parts.length != 2) {
                    System.err.println("WARNING: Invalid line format at line " + lineNumber +
                    ". Expected: <TYPE><whitespace><URL>. Got: " + line);
                continue;
            }   


                String type = parts[0].trim();
                String url = parts[1].trim();

                // Just basic validation for now
                if (!(type.equals("POS") || type.equals("CONSTITUENCY") || type.equals("DEPENDENCY"))) {
                    System.err.println("WARNING: Unknown analysis type at line " + lineNumber + ": " + type);
                }

                jobs.add(new JobRequest(type, url));
            }
        }

        return jobs;
    }

    /**
     * For now, just print what we *would* send to the Manager,
     * but do NOT actually use AWS.
     */
    private static void simulateSendingToManager(List<JobRequest> jobs,
                                                 String jobId,
                                                 int n,
                                                 boolean terminate) {
        System.out.println("--- SIMULATION: This is what we'd send to Manager (later via SQS) ---");
        System.out.println("NEW_JOB message:");
        System.out.println("  jobId = " + jobId);
        System.out.println("  total jobs = " + jobs.size());
        System.out.println("  n (max URLs per worker) = " + n);
        System.out.println("  terminate after job? = " + terminate);

        System.out.println();
        System.out.println("Each job line:");
        int i = 0;
        for (JobRequest job : jobs) {
            System.out.println("  [" + (++i) + "] type=" + job.analysisType + ", url=" + job.url);
        }

        System.out.println("--------------------------------------------------------------------");
    }

    /**
     * Creates a small dummy HTML file that lists all job requests.
     * Later, this HTML will be actually created by the Manager from real Worker results.
     */
    private static void createDummyHtmlOutput(String outputFileName,
                                              List<JobRequest> jobs,
                                              String jobId,
                                              int n,
                                              boolean terminate) throws IOException {
        File outFile = new File(outputFileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
            writer.write("<!DOCTYPE html>\n");
            writer.write("<html><head><meta charset=\"UTF-8\"><title>Dummy Summary</title></head><body>\n");
            writer.write("<h1>Dummy Summary (LocalApplication Local Version)</h1>\n");
            writer.write("<p><b>Job ID:</b> " + jobId + "</p>\n");
            writer.write("<p><b>Parameter n:</b> " + n + "</p>\n");
            writer.write("<p><b>Terminate:</b> " + terminate + "</p>\n");

            writer.write("<h2>Requested Analyses</h2>\n");
            writer.write("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
            writer.write("<tr><th>#</th><th>Type</th><th>URL</th></tr>\n");

            int index = 0;
            for (JobRequest job : jobs) {
                index++;
                writer.write("<tr>");
                writer.write("<td>" + index + "</td>");
                writer.write("<td>" + escapeHtml(job.analysisType) + "</td>");
                writer.write("<td><a href=\"" + escapeHtml(job.url) + "\">" + escapeHtml(job.url) + "</a></td>");
                writer.write("</tr>\n");
            }

            writer.write("</table>\n");
            writer.write("<p><i>This is a dummy summary created locally. Later, the real summary will be created by the Manager using Worker results from S3.</i></p>\n");
            writer.write("</body></html>\n");
        }
    }

    /**
     * Very small helper to escape HTML special chars.
     */
    private static String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("\"", "&quot;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }
}
