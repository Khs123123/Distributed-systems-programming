package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.text.html.HTML;

/**
 * Local-only Manager implementation (NO AWS YET).
 *
 * This is step 4 of the assignment: "Write the manager code, run it on your computer and make sure it works."
 *
 * Usage (for local tests):
 *   java -cp target/demo-1.0-SNAPSHOT.jar com.example.Manager inputFileName outputSummary.html n
 *
 * For now, Manager:
 *   - Reads the input file (same format as LocalApplication: <TYPE> <URL> per line).
 *   - Treats each line as a "task".
 *   - Simulates "processing" the task (no real Worker or Stanford yet).
 *   - Produces a summary HTML file with one row per task.
 *
 * Later, this Manager will:
 *   - Receive tasks from LocalApplication via SQS.
 *   - Spawn real Worker instances.
 *   - Aggregate real analysis results from Workers.
 */
public class Manager {

    /** Represents one task (one line from input). */
    private static class Task {  //this becomes the body of an SQS message to workers.
        String type; // POS / CONSTITUENCY / DEPENDENCY
        String url;

        Task(String type, String url) {
            this.type = type;
            this.url = url;
        }
    }

    /** Represents the result of processing a task. */
    private static class TaskResult { //This will later come from: Worker → SQS → Manager
        Task task;
        String resultText; // What Worker produced (fake for now)
        String errorText;  // Non-null if there was an error

        TaskResult(Task task, String resultText, String errorText) {
            this.task = task;
            this.resultText = resultText;
            this.errorText = errorText;
        }
    }

    public static void main(String[] args) {
        try {
            runManager(args);
        } catch (Exception e) {
            System.err.println("MANAGER ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void runManager(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java com.example.Manager inputFileName outputSummary.html n");
            return;
        }

        String inputFileName = args[0];
        String outputSummaryName = args[1];
        int n = Integer.parseInt(args[2]); // will be used later for scaling workers

        System.out.println("=== Manager (LOCAL VERSION, NO AWS) ===");
        System.out.println("inputFileName      = " + inputFileName);
        System.out.println("outputSummaryFile  = " + outputSummaryName);
        System.out.println("n (max per worker) = " + n);

        File inputFile = new File(inputFileName);
        if (!inputFile.exists()) {
            throw new IOException("Input file does not exist: " + inputFile.getAbsolutePath());
        }
        System.out.println("Input file found at: " + inputFile.getAbsolutePath());

        // 1. Read tasks from input
        List<Task> tasks = readTasks(inputFile);
        System.out.println("Manager read " + tasks.size() + " task(s).");

        if (tasks.isEmpty()) {
            System.out.println("No tasks to process. Exiting Manager.");
            return;
        }

        // 2. "Process" tasks (simulate Worker)
        List<TaskResult> results = processTasksLocally(tasks); //In AWS mode: Manager will not analyze anything in Java Instead it will: Send each task to SQS Wait for workers to respond Aggregate results But your local simulation is perfect for testing before moving to AWS.

        // 3. Build summary HTML
        File outputFile = new File(outputSummaryName);
        writeSummaryHtml(outputFile, results, n);
        //In AWS mode:
        //Manager will upload this file to S3
        //Send SQS message to LocalApplication
        //LocalApplication downloads it
        //Saves it as output HTML

        System.out.println("Summary HTML written to: " + outputFile.getAbsolutePath());
        System.out.println("=== Manager (LOCAL VERSION) FINISHED ===");
    }

    /**
     * Reads the input file and returns a list of tasks.
     * Format per line:
     *    <TYPE> <URL>
     * where <TYPE> is POS, CONSTITUENCY, or DEPENDENCY.
     */
    private static List<Task> readTasks(File inputFile) throws IOException {
        List<Task> tasks = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // split on ANY whitespace (space or tab)
                String[] parts = line.split("\\s+", 2);
                if (parts.length != 2) {
                    System.err.println("[Manager] WARNING: Bad line " + lineNumber +
                            ". Expected: <TYPE><whitespace><URL>. Got: " + line);
                    continue;
                }

                String type = parts[0].trim();
                String url = parts[1].trim();
                tasks.add(new Task(type, url));
            }
        }

        return tasks;
    }

    /**
     * Simulates processing of all tasks.
     * For now, we don't call a real Worker or Stanford – we just fake a result.
     * Later, this method will send tasks to Workers and collect real results.
     */
    private static List<TaskResult> processTasksLocally(List<Task> tasks) {
    List<TaskResult> results = new ArrayList<>();
    Workers worker = new Workers(); // our local worker

    int index = 0;
    for (Task t : tasks) {
        index++;
        System.out.println("[Manager] Processing task " + index +
                " (type=" + t.type + ", url=" + t.url + ")");

        try {
            String analysis = worker.analyzeUrl(t.url, t.type);

            // For the summary HTML we don't want to dump megabytes,
            // so we'll store the full analysis, but display only a prefix.
            String clipped = truncate(analysis, 500); // first 500 chars
            TaskResult tr = new TaskResult(t, clipped, null);
            results.add(tr);
        } catch (Exception e) {
            String errorMsg = "Exception: " + e.getClass().getSimpleName()
                    + " - " + e.getMessage();
            System.err.println("[Manager] ERROR while processing task: " + errorMsg);
            TaskResult tr = new TaskResult(t, null, errorMsg);
            results.add(tr);
        }
    }

    return results;
}


    /**
     * Writes a simple HTML summary file listing all tasks and their (fake) results.
     * Later, this will mirror the final summary the assignment asks for.
     */
    private static void writeSummaryHtml(File outputFile,
                                         List<TaskResult> results,
                                         int n) throws IOException {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("<!DOCTYPE html>\n");
            writer.write("<html><head><meta charset=\"UTF-8\"><title>Manager Summary (Local)</title></head><body>\n");
            writer.write("<h1>Manager Summary (Local Test)</h1>\n");
            writer.write("<p><b>Total tasks:</b> " + results.size() + "</p>\n");
            writer.write("<p><b>Parameter n (max per worker):</b> " + n + "</p>\n");

            writer.write("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
            writer.write("<tr><th>#</th><th>Type</th><th>URL</th><th>Result</th><th>Error</th></tr>\n");

            int i = 0;
            for (TaskResult r : results) {
                i++;
                writer.write("<tr>");
                writer.write("<td>" + i + "</td>");
                writer.write("<td>" + escapeHtml(r.task.type) + "</td>");
                writer.write("<td><a href=\"" + escapeHtml(r.task.url) + "\">" +
                        escapeHtml(r.task.url) + "</a></td>");
                String displayResult = (r.resultText == null ? "" : r.resultText);
                writer.write("<td><pre>" + escapeHtml(displayResult) + "</pre></td>");

                writer.write("<td>" + (r.errorText == null ? "" : escapeHtml(r.errorText)) + "</td>");
                writer.write("</tr>\n");
            }

            writer.write("</table>\n");
            writer.write("<p><i>This is a LOCAL test version of the Manager. "
                    + "Later, this summary will be based on real Worker results from S3.</i></p>\n");
            writer.write("</body></html>\n");
        }
    }

    private static String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("\"", "&quot;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return "";
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen) + "... [truncated]";
    }

}
// public static void runManager(String[] args) throws IOException {

//     // 1. Parse arguments (same for local & AWS)
//     String inputFileName = args[0];
//     String outputSummaryName = args[1];
//     int n = Integer.parseInt(args[2]);

//     if (LOCAL_TEST_NO_AWS) {
//         // ======================================
//         //   🔵 LOCAL VERSION (WHAT YOU HAVE NOW)
//         // ======================================

//         File inputFile = new File(inputFileName);
//         List<Task> tasks = readTasks(inputFile);

//         List<TaskResult> results = processTasksLocally(tasks);

//         File outputFile = new File(outputSummaryName);
//         writeSummaryHtml(outputFile, results, n);

//         return;
//     }

//     // ==========================================
//     //   🟩 AWS VERSION (WHAT YOU WILL ADD)
//     // ==========================================

//     // 1. Download input file from S3
//     // 2. Read tasks using readTasks()
//     // 3. For each task, send SQS message to worker queue
//     // 4. Start EC2 Workers based on n (scaling logic)
//     // 5. Collect Worker results from SQS "results queue"
//     // 6. Build summary HTML (same function: writeSummaryHtml)
//     // 7. Upload summary to S3
//     // 8. Send SQS message to LocalApplication
//     // 9. If terminate flag -> shut down workers and self-terminate

// }
