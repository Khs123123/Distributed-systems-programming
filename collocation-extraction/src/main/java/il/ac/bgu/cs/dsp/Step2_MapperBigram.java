package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step2_MapperBigram extends Mapper<LongWritable, Text, Step2_Key, Step2_Value> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        
        if (parts.length >= 3) {
            String[] bigram = parts[0].split(" ");
            if (bigram.length == 2) {
                String w1 = bigram[0].trim();
                String w2 = bigram[1].trim();

                // FILTER: Strict check for valid letters only (English + Hebrew)
                if (w1.length() < 2 || !w1.matches("^[a-zA-Z\u0590-\u05FF]+$")) return;
                if (w2.length() < 2 || !w2.matches("^[a-zA-Z\u0590-\u05FF]+$")) return;

                try {
                    int year = Integer.parseInt(parts[1]);
                    String decade = String.valueOf((year / 10) * 10);
                    long count = Long.parseLong(parts[2]);

                    // Key: Decade, w1, w2, TYPE_BIGRAM
                    Step2_Key outKey = new Step2_Key(decade, w1, w2, Step2_Key.TYPE_BIGRAM);
                    
                    // FIX: Use the correct 3-arg constructor (Type, w2, Count)
                    // We pass 'w2' so the Reducer can retrieve it later
                    context.write(outKey, new Step2_Value(Step2_Value.TYPE_BIGRAM, w2, count));
                    
                } catch (NumberFormatException e) { }
            }
        }
    }
}