package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class Step4_Reducer extends Reducer<Text, Text, Text, Text> {

    // Helper class to store score and text together
    private static class ResultPair implements Comparable<ResultPair> {
        double score;
        String val;

        public ResultPair(double score, String val) {
            this.score = score;
            this.val = val;
        }

        @Override
        public int compareTo(ResultPair other) {
            // Ascending order is needed for the PriorityQueue to remove the smallest element easily
            return Double.compare(this.score, other.score);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // PriorityQueue to keep the top 100 elements. 
        // The head of this queue is the smallest element (lowest score).
        PriorityQueue<ResultPair> pq = new PriorityQueue<>();

        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 2) {
                try {
                    double score = Double.parseDouble(parts[0]);
                    String phrase = parts[1]; // "w1 w2"

                    pq.add(new ResultPair(score, phrase));

                    // If we have more than 100, remove the one with the lowest score
                    if (pq.size() > 100) {
                        pq.poll();
                    }
                } catch (NumberFormatException e) {
                    // Ignore parsing errors
                }
            }
        }

        // Now we have the top 100, but they are sorted smallest-to-largest.
        // We need descending order.
        List<ResultPair> finalResults = new ArrayList<>();
        while (!pq.isEmpty()) {
            finalResults.add(pq.poll());
        }
        
        // Reverse to get highest score first (Descending order)
        Collections.reverse(finalResults);

        // Output format: Decade <tab> "Bigram: LLR"
        // Example: 1990  apple pie: 543.21
        for (ResultPair pair : finalResults) {
            context.write(key, new Text(pair.val + "\t" + pair.score));
        }
    }
}