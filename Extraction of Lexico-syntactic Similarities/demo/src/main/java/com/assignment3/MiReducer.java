package com.assignment3;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> counts = new HashMap<>();
        long total = 0;
        for (Text val : values) {
            String[] p = val.toString().split("\t");
            int c = Integer.parseInt(p[1]);
            counts.put(p[0], c);
            total += c;
        }
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            double mi = Math.log((double) e.getValue() / total); 
            // Changed filter to >= 0 for testing
            if (mi >= 0) context.write(key, new Text(e.getKey() + "\t" + mi));
        }
    }
}