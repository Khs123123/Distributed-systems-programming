package com.assignment3;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DirtMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split by any whitespace to handle tabs or spaces correctly
        String[] parts = value.toString().split("\\s+");
        
        // Standard Biarcs: 0=Head, 1=Relation, 3=Child (Index 2 is usually a count)
        if (parts.length < 4) return;

        String head = parts[0].toLowerCase();
        String relation = parts[1].toLowerCase();
        String child = parts[3].toLowerCase();

        // Flexible check for Verb (VB) and Noun (NN) tags
        if (head.contains("vb") && child.contains("nn")) {
            
            // Extract the word by splitting at the first tag separator (/ or _)
            String rawHead = parts[0].split("/|_")[0];
            String rawChild = parts[3].split("/|_")[0];

            String sHead = stem(rawHead);
            String sChild = stem(rawChild);

            // DIRT logic: Find the slot in the relation column
            String slot = null;
            if (relation.contains("subj")) slot = "X";
            else if (relation.contains("obj")) slot = "Y";

            if (slot != null) {
                // Key format: path [TAB] slot [TAB] word
                context.write(new Text(sHead + "\t" + slot + "\t" + sChild), one);
            }
        }
    }

    private String stem(String w) {
        Stemmer s = new Stemmer();
        for (char c : w.toLowerCase().toCharArray()) s.add(c);
        s.stem();
        return s.toString();
    }
}