package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
    
    private static class Pair implements Comparable<Pair> {
        double llr;
        String bigram;

        public Pair(double llr, String bigram) {
            this.llr = llr;
            this.bigram = bigram;
        }

        @Override
        public int compareTo(Pair other) {
            return Double.compare(this.llr, other.llr);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        PriorityQueue<Pair> top100Heap = new PriorityQueue<>();
        
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length >= 2) {
                try {
                    double llr = Double.parseDouble(parts[0]);
                    String bigram = parts[1];
                    top100Heap.add(new Pair(llr, bigram));
                    if (top100Heap.size() > 100) {
                        top100Heap.poll(); 
                    }
                } catch (Exception e) {}
            }
        }
        
        List<Pair> finalSortedList = new ArrayList<>();
        while (!top100Heap.isEmpty()) {
            finalSortedList.add(top100Heap.poll());
        }
        Collections.reverse(finalSortedList);
        
        for (Pair p : finalSortedList) {
            context.write(key, new Text(p.bigram + "\t" + p.llr));
        }
    }
}