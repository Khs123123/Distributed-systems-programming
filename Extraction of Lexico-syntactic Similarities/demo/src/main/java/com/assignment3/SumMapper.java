package com.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // קלט צפוי: word1 \t word2 \t partialScore
        String[] parts = value.toString().split("\t");
        
        if (parts.length < 3) return;

        String w1 = parts[0];
        String w2 = parts[1];
        String score = parts[2];

        // המפתח הוא הזוג, והערך הוא הציון החלקי
        context.write(new Text(w1 + "\t" + w2), new Text(score));
    }
}