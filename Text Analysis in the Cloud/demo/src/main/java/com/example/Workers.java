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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.Properties;

/**
 * Local Worker implementation (NO AWS).
 *
 * Given a URL and analysis type (POS / CONSTITUENCY / DEPENDENCY),
 * it downloads the text and runs the Stanford CoreNLP pipeline.
 */
public class Workers {

    public enum AnalysisType {
        POS,
        CONSTITUENCY,
        DEPENDENCY
    }

    // One static pipeline shared by all worker calls (heavy to build).
    private static final StanfordCoreNLP pipeline = createPipeline();

    private static StanfordCoreNLP createPipeline() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, parse, depparse");
        System.out.println("[Worker] Loading Stanford CoreNLP models (first time only)...");
        StanfordCoreNLP pl = new StanfordCoreNLP(props);
        System.out.println("[Worker] Models loaded.");
        return pl;
    }

    /**
     * High-level API used by Manager:
     *
     * @param urlString URL of the text file
     * @param typeStr   "POS", "CONSTITUENCY", or "DEPENDENCY"
     * @return a String with the analysis result
     */
    public String analyzeUrl(String urlString, String typeStr) throws IOException {
        AnalysisType type = AnalysisType.valueOf(typeStr.toUpperCase());
        return analyzeUrl(urlString, type);
    }

    public String analyzeUrl(String urlString, AnalysisType type) throws IOException {
        String text = downloadText(urlString);
        return analyzeText(text, type);
    }

    /**
     * Download text from the given URL.
     */
    // private String downloadText(String urlString) throws IOException {
    //     StringBuilder sb = new StringBuilder();
    //     URL url = new URL(urlString);

    //     try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
    //         String line;
    //         while ((line = in.readLine()) != null) {
    //             sb.append(line).append("\n");
    //         }
    //     }

    //     return sb.toString();
    // }

    private String downloadText(String urlString) throws IOException {
    StringBuilder sb = new StringBuilder();
    URL url = new URL(urlString);

    // Only read a prefix of the file to avoid OutOfMemoryError.
    final int MAX_LINES = 80;      // at most 80 lines
    final int MAX_CHARS = 8000;    // and at most 8000 characters

    try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
        String line;
        int lineCount = 0;

        while ((line = in.readLine()) != null) {
            if (lineCount >= MAX_LINES || sb.length() >= MAX_CHARS) {
                sb.append("\n[TRUNCATED]\n");
                break;
            }

            if (sb.length() + line.length() + 1 > MAX_CHARS) {
                int remaining = MAX_CHARS - sb.length();
                if (remaining > 0) {
                    sb.append(line, 0, remaining);
                }
                sb.append("\n[TRUNCATED]\n");
                break;
            }

            sb.append(line).append("\n");
            lineCount++;
        }
    }

    return sb.toString();
}


    /**
     * Run POS / Constituency / Dependency parsing on the given text.
     * Returns a String with the result.
     */
    private String analyzeText(String text, AnalysisType type) {
        Annotation document = new Annotation(text);
        pipeline.annotate(document);

        switch (type) {
            case POS:
                return posToString(document);
            case CONSTITUENCY:
                return constituencyToString(document);
            case DEPENDENCY:
                return dependencyToString(document);
            default:
                return "UNKNOWN ANALYSIS TYPE";
        }
    }

    private String posToString(Annotation document) {
        StringBuilder sb = new StringBuilder();
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                sb.append(word).append("[").append(pos).append("] ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String constituencyToString(Annotation document) {
        StringWriter sw = new StringWriter();
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            tree.pennPrint(new java.io.PrintWriter(sw));
            sw.write("\n");
        }
        return sw.toString();
    }

    private String dependencyToString(Annotation document) {
        StringBuilder sb = new StringBuilder();
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            SemanticGraph dependencies = sentence.get(
                    SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class
            );
            sb.append(dependencies.toString(SemanticGraph.OutputFormat.LIST));
            sb.append("\n\n");
        }
        return sb.toString();
    }
}
