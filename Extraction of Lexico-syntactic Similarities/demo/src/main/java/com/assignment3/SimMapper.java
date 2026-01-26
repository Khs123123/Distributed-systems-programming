package com.assignment3;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t"); 
        // פורמט מצופה משלב 2: Head, Path, Word, MI, SumMI_Per_Slot
        if (parts.length < 5) return;
        
        String path = parts[1].toLowerCase();
        String slot;

        // מיפוי יחסים לסלוטים (DIRT Specification)
        if (path.contains("subj") || path.contains("agent")) {
            slot = "SlotX";
        } else if (path.contains("obj") || path.contains("pobj")) {
            slot = "SlotY";
        } else {
            return; // התעלמות מיחסים שאינם X או Y
        }
        
        // מפתח: סלוט + מילה (למשל: SlotX\tapple)
        // ערך: פועל + MI + SumMI של הפועל בסלוט
        context.write(new Text(slot + "\t" + parts[2]), new Text(parts[0] + "\t" + parts[3] + "\t" + parts[4]));
    }
}