package com.assignment3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {
    public static final Set<String> NOUN_TAGS = new HashSet<>(Arrays.asList("NN", "NNS", "NNP", "NNPS"));
    public static final Set<String> VERB_TAGS = new HashSet<>(Arrays.asList("VB", "VBD", "VBG", "VBN", "VBP", "VBZ"));
    public static final Set<String> PREP_TAGS = new HashSet<>(Arrays.asList("IN", "TO"));
    
    public static final Set<String> AUX_VERBS = new HashSet<>(Arrays.asList(
        "be", "been", "being", "is", "are", "was", "were", "am",
        "have", "has", "had", "having", "do", "does", "did", "doing",
        "will", "would", "shall", "should", "may", "might", "can", "could", "must",
        "'s", "'re", "'ve", "'d", "'ll", "'m"
    ));

    public static boolean isNoun(String posTag) { return NOUN_TAGS.contains(posTag); }
    public static boolean isVerb(String posTag) { return VERB_TAGS.contains(posTag); }
    public static boolean isAuxiliaryVerb(String word) {
        if (word == null) return false;
        return AUX_VERBS.contains(word.toLowerCase());
    }
}