package com.assignment3;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int lastTab = line.lastIndexOf("\t");
        if (lastTab == -1) return;
        
        // נרמול המפתח (הזוג והתווית) כבר בשלב המיפוי
        String rawKey = line.substring(0, lastTab).trim();
        String normalizedKey = rawKey.replaceAll("\\s+", "\t");
        
        // הערך הוא ה-slot:num:den
        String valPart = line.substring(lastTab + 1).trim();
        
        context.write(new Text(normalizedKey), new Text(valPart));
    }
}