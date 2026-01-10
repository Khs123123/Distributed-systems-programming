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

        // Key: Head + Path
        // Values: List of "SlotWord \t Count"

        Map<String, Integer> wordCounts = new HashMap<>();
        double totalCountForPath = 0;

        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) continue;

            String word = parts[0];
            try {
                int count = Integer.parseInt(parts[1]);
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                totalCountForPath += count;
            } catch (NumberFormatException e) {
                // Ignore errors
            }
        }

        if (totalCountForPath == 0) return;

        // חישוב Score
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            String word = entry.getKey();
            int count = entry.getValue();

            double score = Math.log(count / totalCountForPath);

            // הפלט נשלח לשלב 3 (Sim)
            context.write(key, new Text(word + "\t" + score));
        }
    }
}