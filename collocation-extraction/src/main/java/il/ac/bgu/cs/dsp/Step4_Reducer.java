package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
    
    // Helper class to store LLR and Bigram
    private static class Pair implements Comparable<Pair> {
        double llr;
        String bigram;

        public Pair(double llr, String bigram) {
            this.llr = llr;
            this.bigram = bigram;
        }

        // Default Sort: Ascending (Smallest LLR first)
        // This is needed for the Min-Heap logic
        @Override
        public int compareTo(Pair other) {
            return Double.compare(this.llr, other.llr);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        // MEMORY FIX: Use a PriorityQueue (Min-Heap) with a fixed size.
        // This ensures we never hold more than 101 objects in memory.
        PriorityQueue<Pair> top100Heap = new PriorityQueue<>();
        
        for (Text val : values) {
            // Val format from Mapper: "LLR <tab> w1 w2"
            // Example: "540.23   computer science"
            String[] parts = val.toString().split("\t");
            
            if (parts.length >= 2) {
                try {
                    double llr = Double.parseDouble(parts[0]);
                    String bigram = parts[1];
                    
                    // Add current pair to the heap
                    top100Heap.add(new Pair(llr, bigram));
                    
                    // If we have more than 100 items, remove the one with the SMALLEST LLR.
                    // This keeps only the "Top 100" highest scores in the heap.
                    if (top100Heap.size() > 100) {
                        top100Heap.poll(); 
                    }
                    
                } catch (Exception e) {
                    // Ignore parsing errors
                }
            }
        }
        
        // Now top100Heap contains the top 100, but they are sorted Smallest -> Largest.
        // Let's reverse them so the output is Rank 1 (Highest) -> Rank 100.
        List<Pair> finalSortedList = new ArrayList<>();
        while (!top100Heap.isEmpty()) {
            finalSortedList.add(top100Heap.poll());
        }
        
        // Reverse to get Descending order (Highest LLR first)
        Collections.reverse(finalSortedList);
        
        // Write the output
        for (Pair p : finalSortedList) {
            // Final Output: Decade <tab> Bigram <tab> LLR
            // Example: 1990    computer science    540.23
            context.write(key, new Text(p.bigram + "\t" + p.llr));
        }
    }
}