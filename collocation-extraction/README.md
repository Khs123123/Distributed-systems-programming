Distributed Systems Programming Assignment 2
Collocation Extraction (Top 100 per Decade) using Hadoop MapReduce on AWS EMR

STUDENT INFO

Khaled Saleh ID 1 : 326801628 username:khaleds
Mostafa Taha ID 2 : 326524675 username:mostafat
Osama najjar ID 3 : 325227767 username:osaman

1) PROJECT OVERVIEW

Goal:
Extract the top 100 collocations for each decade (1990 1999, 2000 2009, ...) for BOTH English and Hebrew Google N Grams (2 grams), ranked by Log Likelihood Ratio (LLR).

Definition:
A collocation is a pair of ordered words (w1, w2) that co occur more often than expected by chance.

Key requirement:
The system must be scalable and must NOT assume that any decade pairs or unigram lists can fit in memory. Also, avoid generating redundant KV pairs.

2) DATA SOURCES (AWS S3)

Bigrams (2 gram datasets):
English: s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data
Hebrew : s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data

Unigrams (1 gram datasets):
English: s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data
Hebrew : s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data

Format:
SequenceFile + LZO block compression.
InputFormat used: SequenceFileInputFormat.

3) STOP WORDS FILTERING

We filter stop words at the Mapper level for BOTH unigrams and bigrams.

Rules:
If w is a stop word: ignore unigram count for w.
If (w1,w2) contains stop word in either position: discard bigram entirely.
This reduces noise and improves collocation quality.

Stop word lists:
The English and Hebrew stop words are included in the project resources (e.g., inside Step1_Mapper.java or separate text files loaded via DistributedCache).

4) LLR METRIC (WHAT WE COMPUTE)

For each ordered bigram (w1,w2) within a given decade we compute:
c1 = count(w1)
c2 = count(w2)
c12 = count(w1 w2)
N = total number of words in the corpus (for that decade)

Then compute:
LLR(w1,w2) as in the assignment formula.

We output for each decade:
Top 100 bigrams with highest LLR, sorted descending.

5) MAPREDUCE DESIGN (4 JOBS)

JOB 1: Total Corpus Size per Decade
Input: Unigrams (1 gram)
Mapper:
Parse (word, year, count)
Convert year to decade bucket (e.g., 1993 to 1990)
If word is stop word, skip
Add word count to decade counter (Hadoop Counters)
Reducer:
Not strictly required for logic, but used for aggregation.
Optimization:
We DO NOT write N(decade) to an output file.
Instead we use Hadoop Counters ("DecadeCounts") and pass these values into Job 3 configuration.

JOB 2: Join c1 with bigrams (w1,w2)
Goal: Enrich bigrams with c1 (count of w1)
Inputs: Unigrams (for c1) and Bigrams (for c12)
Reducer:
Read first record (c1)
For each subsequent bigram record, output (Decade, w1, w2, c1, c12)

JOB 3: Join c2 and compute LLR
Goal: For each bigram record from Job2, join with c2 (count of w2) and compute LLR.
Inputs: Output of Job2 and Unigrams (for c2)
Reducer:
Read c2 first
Retrieve N(decade) from configuration (passed from Job1 counters)
Compute LLR using c1, c2, c12, and N.
Output: Decade, "w1 w2", LLR

JOB 4: Top 100 per decade
Input: Job3 output
Mapper: Key = Decade, Value = (LLR, Bigram)
Reducer:
Maintain a PriorityQueue (min heap) of size 100
Push each candidate; if size > 100, pop smallest
Output descending LLR order.

6) STATISTICS

We report the number of key value pairs sent from mappers to reducers and their total size, WITH and WITHOUT local aggregation (combiner).

Statistics taken from Job 1 (Word Count N Calculation) on the English dataset.

Metric: Map Output Records
WITH Combiner (Optimized): 167,502,138
WITHOUT Combiner (Unoptimized): 167,502,138

Metric: Combine Input Records
WITH Combiner (Optimized): 167,502,138
WITHOUT Combiner (Unoptimized): 0

Metric: Combine Output Records
WITH Combiner (Optimized): 2,010,826
WITHOUT Combiner (Unoptimized): 0

Metric: Records Sent to Reducer
WITH Combiner (Optimized): 2,010,826
WITHOUT Combiner (Unoptimized): 167,502,138

Notes:
Combiner was applied to: Job 1 (Step1_Mapper Step1_Reducer).
Evidence taken from: Hadoop job counters in the Syslogs.
Conclusion: Local aggregation reduced network traffic by approximately 98.8% (from 167M records to 2M records).

7) MANUAL OUTPUT ANALYSIS 

A) 10 GOOD Collocations

English (5):
1) [1760] civil war (LLR: 23,056.35): A strong, domain specific historical term identifying a specific type of conflict.
2) [1820] human mind (LLR: 197,057.64): A distinct semantic concept common in the philosophical and literary discourse of the 19th century.
3) [1970] working class (LLR: 846,346.61): A standard sociological entity; the words together create a specific social category.
4) [2000] decision making (LLR: 2,234,597.58): A recognized noun phrase in psychology and business, representing a complex cognitive process.
5) [2000] public health (LLR: 1,856,725.01): A fixed compound noun representing a specific field of study and government policy.

Hebrew (5):
1) [1760] ספר תורה (LLR: 90.64): A specific religious object; a strong noun noun construct.
2) [1820] משה רבינו (LLR: 765.06): A fixed honorific title referring to a specific historical figure.
3) [1970] בית הספר (LLR: 109,728.26): A standard lexicalized term for an educational institution.
4) [1970] ראש הממשלה (LLR: 164,583.76): A definitive political title and role.
5) [2000] משרד הביטחון (LLR: 148,717.24): A specific named entity (government body).

B) 10 BAD Collocations

English (5):
1) [1760] fo great (LLR: 108,467.03): Reason: OCR Error. The archaic long s was misread as f. It should be so great.
2) [1760] fome time (LLR: 108,349.04): Reason: OCR Error. Similar to above; some time was misread as fome time.
3) [1610] art thou (LLR: 78.96): Reason: Archaic Grammar. Represents are you. These are essentially stop words in 17th century English that passed through our modern stop word filter.
4) [2000] years later (LLR: 3,142,937.08): Reason: Temporal Phrase. While statistically frequent, this is a compositional time marker rather than a distinct semantic entity or idiom.
5) [1820] great number (LLR: 408,769.81): Reason: Generic Quantifier. Despite the high LLR (indicating strong statistical usage in that era), it is semantically weak. It functions as a determiner rather than a unique concept like human mind.

Hebrew (5):
1) [1970] אלא גם (LLR: 607,053.69): Reason: Grammatical Connector. Part of a common structure. It has high co occurrence but no independent semantic meaning.
2) [2000] כדי שלא (LLR: 660,437.95): Reason: Prepositional Conjunction phrase. High frequency is due to grammatical structure, not identifying a unique entity.
3) [1970] אינו אלא (LLR: 328,195.46): Reason: Rhetorical Structure. A common argumentative phrase in Hebrew, but not a collocation in the entity extraction sense.
4) [1970] גם אני (LLR: 131,011.21): Reason: Pronoun + Particle. A very common grammatical combination, but functionally it is just a subject with an additive particle.
5) [1970] אני רוצה (LLR: 230,397.31): Reason: Subject + Verb. This is a sentence fragment starter, not a fixed idiom or noun phrase.

**Bad collocations are not errors in the LLR computation, but rather a known limitation of purely statistical association measures. The 
Log-Likelihood Ratio captures strong co-occurrence patterns, including grammatical constructions, OCR artifacts, and high-frequency 
functional phrases, which may achieve very high LLR values despite lacking independent semantic meaning. Therefore, manual linguistic 
analysis is required to distinguish statistically strong but semantically weak collocations from meaningful lexical units.

8) HOW TO RUN

Prerequisites:
Java 8+
Maven
AWS CLI configured
An S3 bucket for: jar, logs, output

Build:
mvn clean package

Upload jar:
aws s3 cp target/assignment2-1.0-SNAPSHOT.jar s3://mosta-s3-bucket/jars/

Run on EMR (Example Command):
java -cp target/assignment2-1.0-SNAPSHOT.jar il.ac.bgu.cs.dsp.EmrRunner

Note: The EmrRunner class programmatically configures the 4 step job flow, sets the input paths to the Google N Grams public datasets, and points the output to our S3 bucket.

9) OUTPUT LOCATIONS

English output:
s3://mosta-s3-bucket/output/TEST_ENGLISH_With Local Aggregation1766243511865/
s3://mosta-s3-bucket/output/TEST_ENGLISH_Without Local Aggregation1766239203918/

Hebrew output:
s3://mosta-s3-bucket/output/TEST_HEBREW_With Local Aggregation1766252618912/
s3://mosta-s3-bucket/output/TEST_HEBREW_Without Local Aggregation1766237219190/

Logs:
s3://mosta-s3-bucket/logs/

10) COST DEVELOPMENT NOTES

During debugging we tested on a small subset before running on full corpus.
We used minimal cluster sizes (e.g., m4.large) to reduce cost and in the end we used (m5.xlarge).
All EMR clusters and temporary resources were terminated immediately after completion.