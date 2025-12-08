package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Step2_MapperUnigram extends Mapper<LongWritable, Text, Step2_Key, Step2_Value> {
    
    // ... Copy the STOP_WORDS static block from the previous version here ...
    // (I am omitting the long list here to save space, but make sure to keep it!)
    private static final Set<String> STOP_WORDS = new HashSet<>();
    static {
        // ... PASTE YOUR STOP WORDS LIST HERE ...
         STOP_WORDS.addAll(Arrays.asList("a", "the", "and", "of", "to")); // Example, put full list back
    }

    private boolean isStopWord(String word) {
        if (word == null || word.trim().isEmpty()) return true;
        return STOP_WORDS.contains(word.toLowerCase());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length >= 3) {
            String w1 = parts[0];
            if (isStopWord(w1)) return;

            try {
                int year = Integer.parseInt(parts[1]);
                String decade = String.valueOf((year / 10) * 10);
                long count = Long.parseLong(parts[2]);

                // NEW: Use Step2_Key with TYPE_UNIGRAM (1)
                Step2_Key newKey = new Step2_Key(decade, w1, Step2_Value.TYPE_UNIGRAM);
                
                context.write(newKey, new Step2_Value(Step2_Value.TYPE_UNIGRAM, "", count));
            } catch (Exception e) { }
        }
    }
}