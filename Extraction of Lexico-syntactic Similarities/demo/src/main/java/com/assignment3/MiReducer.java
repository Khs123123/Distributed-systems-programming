package com.assignment3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // key = head \t slot
        // values: child \t count

        Map<String, Integer> childCounts = new HashMap<>();
        int total = 0;

        for (Text v : values) {
            String[] p = v.toString().split("\t");
            if (p.length < 2) continue;
            String child = p[0];
            int c = Integer.parseInt(p[1]);

            childCounts.merge(child, c, Integer::sum);
            total += c;
        }

        // Output: pattern \t child \t mi
        // simplified MI: log(count / total)  (as in your pipeline)
        for (Map.Entry<String, Integer> e : childCounts.entrySet()) {
            String child = e.getKey();
            int c = e.getValue();

            double mi = Math.log((double) c / (double) total);
            context.write(key, new Text(child + "\t" + mi));
        }
    }
}
