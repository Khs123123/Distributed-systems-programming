package com.example;

import edu.stanford.nlp.pipeline.Annotation;

public interface StanfordCoreNLP {

    void annotate(Annotation document);

}
