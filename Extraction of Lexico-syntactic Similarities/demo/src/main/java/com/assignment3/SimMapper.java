package com.assignment3;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: Head \t Slot \t Word \t Score
        String[] parts = value.toString().split("\t");
        
        if (parts.length < 4) return;

        String head = parts[0];
        String slot = parts[1];
        String word = parts[2];
        String score = parts[3];

        // Key: Slot + Word (כדי לקבץ מילים דומות)
        String outKey = slot + "\t" + word;
        
        // Value: Head + Score (אנחנו לא צריכים את ה-Slot בערך עצמו)
        String outValue = head + "\t" + score;

        context.write(new Text(outKey), new Text(outValue));
    }
}