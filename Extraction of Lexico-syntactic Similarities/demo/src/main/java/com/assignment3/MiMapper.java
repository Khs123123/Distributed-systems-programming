package com.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // מקבל את הפלט של שלב 1:
        // Format: Head \t Path \t SlotWord \t Count
        
        String line = value.toString();
        String[] parts = line.split("\t");
        
        if (parts.length >= 4) {
            String head = parts[0];
            String path = parts[1];
            String word = parts[2];
            String count = parts[3];
            
            // Key: Head + Path (כדי שה-Reducer יקבל את כל המילים ששיכות לאותו נתיב)
            String outKey = head + "\t" + path;
            
            // Value: Word + Count
            String outValue = word + "\t" + count;
            
            context.write(new Text(outKey), new Text(outValue));
        }
    }
}