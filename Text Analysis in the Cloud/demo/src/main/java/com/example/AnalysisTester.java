package com.example; // Or your package name

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;


import java.util.Properties;

/**
 * A simple class to test the Stanford Parser locally, as recommended
 * in the assignment's "Getting Started" section.
 */
public class AnalysisTester {

    public static void main(String[] args) {
        // This is the text we will analyze.
        String textToAnalyze = "The quick brown fox jumps over the lazy dog.";

        System.out.println("Starting analysis for text: \"" + textToAnalyze + "\"");
        System.out.println("---");

        // --- 1. Test POS (Part-of-Speech) Analysis ---
        //
        System.out.println("### 1. POS (Part-of-Speech) TAGGING ###");
        performPosTagging(textToAnalyze);
        System.out.println("---------------------------------------");

        // --- 2. Test Constituency Analysis ---
        //
        System.out.println("### 2. CONSTITUENCY PARSING ###");
        performConstituencyParsing(textToAnalyze);
        System.out.println("---------------------------------------");
        
        // --- 3. Test Dependency Analysis ---
        //
        System.out.println("### 3. DEPENDENCY PARSING ###");
        performDependencyParsing(textToAnalyze);
        System.out.println("---------------------------------------");
    }

    /**
     * Performs Part-of-Speech tagging.
     */
    private static void performPosTagging(String text) {
        // Create a pipeline with only the necessary annotators
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos");
        
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);

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
    private static void performConstituencyParsing(String text) {
        // Create a pipeline that includes the parser
        Properties props = new Properties();
        // This pipeline will also do POS tagging, which is required for parsing
        props.setProperty("annotators", "tokenize, ssplit, pos, parse"); 
        
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);

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
    private static void performDependencyParsing(String text) {
        // Create a pipeline that includes the dependency parser
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, depparse"); 
        
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);

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