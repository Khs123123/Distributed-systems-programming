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
        // Key: Head \t Path
        Map<String, Integer> wordCounts = new HashMap<>();
        double totalCountForPath = 0;

        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) continue;
            String word = parts[0];
            int count = Integer.parseInt(parts[1]);
            wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
            totalCountForPath += count;
        }

        if (totalCountForPath == 0) return;

        // Calculate MI (Rule 1: For ALL paths)
        double totalMiSumForPath = 0;
        Map<String, Double> miMap = new HashMap<>();
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            // Using absolute log to ensure positive scores for Step 3
            double mi = Math.abs(Math.log((double) entry.getValue() / totalCountForPath));
            miMap.put(entry.getKey(), mi);
            totalMiSumForPath += mi;
        }

        for (Map.Entry<String, Double> entry : miMap.entrySet()) {
            // Output format: Word \t MI_Score \t Total_Path_MI
            context.write(key, new Text(entry.getKey() + "\t" + entry.getValue() + "\t" + totalMiSumForPath));
        }
    }
}