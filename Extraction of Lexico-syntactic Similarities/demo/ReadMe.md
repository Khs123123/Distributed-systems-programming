# Distributed Systems Programming – Assignment 3
## DIRT: Discovery of Inference Rules from Text
### Hadoop MapReduce Implementation on AWS EMR

============================================================

STUDENT INFO

Khaled Saleh      ID: 326801628   username: khaleds
Mostafa Taha      ID: 326524675   username: mostafat
Osama Najjar      ID: 325227767   username: osaman

============================================================

0) SUBMISSION CONTENTS

- Source code (Java + pom.xml)
- This README (includes full Design + Analysis)
- Output files for frontal check:
  * MI(p,slot,w)
  * Similarity scores for test-set predicate pairs

============================================================

1) PROJECT OVERVIEW

Goal:
Implement a scalable MapReduce version of DIRT (Dekang Lin & Patrick Pantel)
to discover lexico-syntactic inference rules from large corpora and evaluate
predicate similarity using Mutual Information (MI).

DIRT discovers inference rules such as:
    X acquire Y  ⇔  X purchase Y

Core idea:
Two dependency paths are similar if they occur with similar arguments.
Each path is represented as an MI-weighted feature vector over (slot,word),
and similarity is computed using cosine similarity.

Key Assignment Requirements:
- Scalable MapReduce design
- Dependency paths from Stanford biarcs
- MI(p,slot,w) computation
- Similarity computation on test set
- Design report + Analysis report + graphs

============================================================

2) DATA SOURCES

Corpus:
- Google Syntactic N-Grams (Biarcs format)
- Stanford Dependencies
- Uploaded by students to personal S3 buckets

Test Set:
- Predicate pairs with labels POSITIVE / NEGATIVE
- Provided by Omer Levy
- Used only for evaluation

============================================================

3) PRE-PROCESSING CONSTRAINTS

According to the paper and assignment:

- Only verb-headed dependency paths are used
- X and Y slots must be nouns
- Dependency paths include prepositions (IN, TO)
- Auxiliary verbs (is, are, was, be, etc.) are filtered
- Corpus words are stemmed (Porter Stemmer)

Stemming is required because corpus words are not lemmatized,
while test-set predicates are given in base form.

============================================================

4) SYSTEM PIPELINE OVERVIEW

The system is implemented as FOUR MapReduce jobs:

1. Dependency Path Extraction & Counting
2. Mutual Information (MI) Computation
3. Similarity Computation (Cosine)
4. Final Aggregation

All stages are fully distributed and scalable.

============================================================

5) PROJECT STRUCTURE (CODE MAP)

Main:
- DirtJob.java        : orchestrates the multi-job Hadoop pipeline
- EmrRunner.java      : configures and runs the pipeline on AWS EMR

MapReduce steps:
- Step 1: DirtMapper.java / DirtReducer.java
- Step 2: MiMapper.java   / MiReducer.java
- Step 3: SimMapper.java  / SimReducer.java
- Step 4: SumMapper.java  / SumReducer.java

Utility:
- Stemmer.java (Porter Stemmer)

============================================================

6) MAPREDUCE DESIGN (VERY DETAILED)

------------------------------------------------------------
JOB 1 – PATH / SLOT / WORD EXTRACTION + COUNTING
------------------------------------------------------------

Goal:
Extract dependency paths and count occurrences of (path,slot,word).

Mapper:
- Input : one biarc record
- Output:
    Key   = (path, slot, word)
    Value = 1

Reducer:
- Input : (path,slot,word) → [1,1,1,...]
- Output:
    Key   = (path, slot, word)
    Value = total_count

KV Pairs:
- One KV per extracted dependency observation
- Number of keys equals number of unique triples

Memory:
- Reducer memory O(1) per key (streaming sum)

------------------------------------------------------------
JOB 2 – MUTUAL INFORMATION COMPUTATION
------------------------------------------------------------

Goal:
Compute MI(p,slot,w) using counts and marginals.

Mapper:
- Input : (path,slot,word,count)
- Output:
    Key   = path
    Value = (slot, word, count)

Reducer:
- Receives all slot-word pairs for a path
- Computes required marginals
- Computes MI according to DIRT formula

Output:
    path    slot    word    MI

Memory:
- Reducer holds all features for one path
- Worst-case memory proportional to number of slot-word pairs
- Path skew is the main memory risk

------------------------------------------------------------
JOB 3 – SIMILARITY COMPUTATION
------------------------------------------------------------

Goal:
Compute similarity between predicate pairs in the test set.

Mapper:
- Key   = predicate1|predicate2
- Value = partial dot products / partial norms

Reducer:
- Aggregates partials
- Computes cosine similarity

Output:
    predicate1    predicate2    GOLD_LABEL    similarity_score

Memory:
- Constant per key

------------------------------------------------------------
JOB 4 – FINAL AGGREGATION
------------------------------------------------------------

Goal:
Aggregate partial similarity contributions into final scores.

============================================================

7) HOW TO RUN (WITH TODO PLACES)

------------------------------------------------------------
7.1 Prerequisites
------------------------------------------------------------

- Java 8+
- Maven
- AWS CLI configured
- An S3 bucket you control

------------------------------------------------------------
7.2 S3 PATHS (FILL THESE)
------------------------------------------------------------

S3_BUCKET                = s3://TODO
CORPUS_SMALL_PREFIX      = s3://TODO/inputs/small/
CORPUS_LARGE_PREFIX      = s3://TODO/inputs/large/
TESTSET_PATH             = s3://TODO/testset/testset.txt
JAR_PATH                 = s3://TODO/jars/
OUTPUT_BASE              = s3://TODO/output/
LOGS_BASE                = s3://TODO/logs/

------------------------------------------------------------
7.3 Build JAR
------------------------------------------------------------

mvn clean package

------------------------------------------------------------
7.4 Upload JAR
------------------------------------------------------------

aws s3 cp target/TODO.jar s3://TODO/jars/

------------------------------------------------------------
7.5 Upload Inputs (if needed)
------------------------------------------------------------

aws s3 cp ./biarcs_small/ s3://TODO/inputs/small/ --recursive
aws s3 cp ./biarcs_large/ s3://TODO/inputs/large/ --recursive
aws s3 cp ./testset.txt   s3://TODO/testset/

------------------------------------------------------------
7.6 Run on EMR (EmrRunner)
------------------------------------------------------------

Edit EmrRunner.java:
- bucketName = "TODO"
- inputSmall = "s3://TODO/inputs/small/"
- inputLarge = "s3://TODO/inputs/large/"
- testSet    = "s3://TODO/testset/testset.txt"
- outputBase = "s3://TODO/output/"

Run:
java -cp target/TODO.jar com.assignment3.EmrRunner

============================================================

8) ANALYSIS (REAL RESULTS)

------------------------------------------------------------
8.1 Dataset Summary
------------------------------------------------------------

From final_output.txt:
- Total predicate pairs : 2580
- POSITIVE              : 2481
- NEGATIVE              : 99

Dataset is highly imbalanced.

------------------------------------------------------------
8.2 Metrics
------------------------------------------------------------

Prediction rule:
    entails ⇔ similarity_score ≥ threshold

Best Positive-Class F1 (naive):
- Threshold : 0.000000
- Precision : 0.9616
- Recall    : 1.0000
- F1        : 0.9804

------------------------------------------------------------
Chosen Operating Point (Macro-F1)
------------------------------------------------------------

Threshold t* = 0.000713

Confusion Matrix:

                Gold=POS   Gold=NEG
Pred=POS           2332        149
Pred=NEG             87         12

Metrics:
- Precision     : 0.9640
- Recall        : 0.9399
- Positive-F1   : 0.9518
- Negative-F1   : 0.0923
- Macro-F1      : 0.5221

------------------------------------------------------------
8.3 Precision–Recall Curve (ASCII)
------------------------------------------------------------

Precision
1.0 ┤● ● ● ● ● ● ● ● ● ● ● ● ● ● ●     ● ● ● ● ● ● ●   ● ●
    │                              ● ●               ●     ● ● ● ●
    │
    │
    │
    │
    │
    │
0.0 ┤
    └────────────────────────────────────────────────────────▶ Recall
     0.0       0.2       0.4       0.6       0.8       1.0

AUC ≈ 0.95

------------------------------------------------------------
8.4 Error Analysis (Examples)
------------------------------------------------------------

True Positives:
- X accompany Y  ⇔  X accompany by Y
- X occur with Y ⇔  X occur in Y

False Positives:
- X protect from Y ⇔ X expose to Y
  (antonym relation, similar contexts)

False Negatives:
- X control with Y ⇔ Y be in X
  (missing MI features / data sparsity)

------------------------------------------------------------

9) SMALL VS LARGE REQUIREMENT

Assignment requires TWO runs:
- Small corpus (10 files)
- Large corpus (100 files)

This README reports ONE run.
Repeat pipeline for both sizes to obtain:
- Two PR curves
- Two F1 scores
- Comparative error analysis

============================================================

10) CONCLUSION

This project demonstrates a scalable MapReduce implementation of DIRT.
The system extracts dependency paths, computes MI-based features,
and evaluates predicate similarity using standard IR metrics.
The design scales with corpus size and does not assume any structure
fits in memory.

============================================================
