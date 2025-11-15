package com.example; // Or your package name

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP; // Correct import
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

/**
 * A simple class to test the Stanford Parser locally, as recommended
 * in the assignment's "Getting Started" section [cite: 148-149].
 *
 * EFFICIENT VERSION: Creates only one pipeline.
 */
public class AnalysisTester {

    // We don't even need the static variable, we can
    // just pass the pipeline as a parameter.
    
    public static void main(String[] args) {
        // This is the text we will analyze.
        String textToAnalyze = "The quick brown fox jumps over the lazy dog.";

        // --- 1. Create ONE pipeline for ALL tasks ---
        // We include all annotators we'll need:
        // 'pos' for POS
        // 'parse' for Constituency (also needs pos)
        // 'depparse' for Dependency (also needs pos)
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, parse, depparse");
        
        System.out.println("Loading Stanford CoreNLP models... (This may take a moment)");
        // This is the only time we create the heavy object.
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        System.out.println("Models loaded.");

        System.out.println("---");
        System.out.println("Starting analysis for text: \"" + textToAnalyze + "\"");
        System.out.println("---");

        // --- 2. Create the Annotation object ONCE ---
        // The Annotation object will hold the results of all analyses.
        Annotation document = new Annotation(textToAnalyze);
        pipeline.annotate(document);

        // --- 3. Run all tests using the SAME annotated document ---
        System.out.println("### 1. POS (Part-of-Speech) TAGGING ###");
        // We just need to read the results from the document
        performPosTagging(document);
        System.out.println("---------------------------------------");

        System.out.println("### 2. CONSTITUENCY PARSING ###");
        performConstituencyParsing(document);
        System.out.println("---------------------------------------");
        
        System.out.println("### 3. DEPENDENCY PARSING ###");
        performDependencyParsing(document);
        System.out.println("---------------------------------------");
    }

    /**
     * Performs Part-of-Speech tagging.
     * We pass the *annotated document* so we don't re-run the pipeline.
     */
    private static void performPosTagging(Annotation document) {
        // Loop over sentences
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            // Loop over tokens (words)
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                System.out.println(word + " [" + pos + "]");
            }
        }
    }

    /**
     * Performs Constituency parsing.
     */
    private static void performConstituencyParsing(Annotation document) {
        // Loop over sentences and get the parse tree
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            // .pennPrint() gives the standard tree format
            tree.pennPrint(System.out); 
        }
    }

    /**
     * Performs Dependency parsing.
     */
    private static void performDependencyParsing(Annotation document) {
        // Loop over sentences and get the dependencies
        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            SemanticGraph dependencies = sentence.get(
                SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class
            );
            // .toString(SemanticGraph.OutputFormat.LIST) gives a simple readable list
            System.out.println(dependencies.toString(SemanticGraph.OutputFormat.LIST));
        }
    }
}