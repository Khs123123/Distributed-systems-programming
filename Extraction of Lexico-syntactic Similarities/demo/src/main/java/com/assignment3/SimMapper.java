package com.assignment3;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input: Head \t Path \t Word \t MI \t TotalPathMI
        String[] parts = value.toString().split("\t");
        if (parts.length < 5) return;

        String head = parts[0];
        String path = parts[1];
        String word = parts[2];
        String mi = parts[3];
        String totalMi = parts[4];

        // Join context: The dependency slot (e.g. nsubj) and the word
        String outKey = path + "\t" + word; 
        // Data: The verb head, its MI for this word, and its total MI sum
        String outValue = head + "\t" + mi + "\t" + totalMi;

        context.write(new Text(outKey), new Text(outValue));
    }
}