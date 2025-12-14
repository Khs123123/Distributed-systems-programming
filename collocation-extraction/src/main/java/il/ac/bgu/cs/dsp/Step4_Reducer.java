package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
    
    // Helper class to store and sort collocations
    private static class Pair implements Comparable<Pair> {
        double llr;
        String bigram;

        public Pair(double llr, String bigram) {
            this.llr = llr;
            this.bigram = bigram;
        }

        // Sort descending (High LLR first)
        @Override
        public int compareTo(Pair other) {
            return Double.compare(other.llr, this.llr);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Pair> top100 = new ArrayList<>();
        
        // 1. Collect all values for this decade
        for (Text val : values) {
            // Val format: "LLR <tab> w1 w2"
            String[] parts = val.toString().split("\t");
            if (parts.length == 2) {
                try {
                    double llr = Double.parseDouble(parts[0]);
                    String bigram = parts[1];
                    top100.add(new Pair(llr, bigram));
                } catch (Exception e) {}
            }
        }
        
        // 2. Sort List
        Collections.sort(top100);
        
        // 3. Keep only top 100
        int count = 0;
        for (Pair p : top100) {
            if (count++ >= 100) break;
            
            // Final Output: 
            // Decade <tab> Bigram <tab> LLR
            context.write(key, new Text(p.bigram + "\t" + p.llr));
        }
    }
}