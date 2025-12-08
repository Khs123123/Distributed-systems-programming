package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Step2_MapperBigram extends Mapper<LongWritable, Text, Step2_Key, Step2_Value> {
    
    // ... Copy the STOP_WORDS static block from the previous version here ...
    private static final Set<String> STOP_WORDS = new HashSet<>();
    static {
        // ... PASTE YOUR STOP WORDS LIST HERE ...
        STOP_WORDS.addAll(Arrays.asList("a", "the")); // Example
    }
    
    private boolean isStopWord(String word) {
        if (word == null || word.trim().isEmpty()) return true;
        return STOP_WORDS.contains(word.toLowerCase());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length >= 4) {
            String w1 = parts[0];
            String w2 = parts[1];
            if (isStopWord(w1) || isStopWord(w2)) return;

            try {
                int year = Integer.parseInt(parts[2]);
                String decade = String.valueOf((year / 10) * 10);
                long count = Long.parseLong(parts[3]);

                // NEW: Use Step2_Key with TYPE_BIGRAM (2)
                Step2_Key newKey = new Step2_Key(decade, w1, Step2_Value.TYPE_BIGRAM);
                
                context.write(newKey, new Step2_Value(Step2_Value.TYPE_BIGRAM, w2, count));
            } catch (Exception e) { }
        }
    }
}