package com.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // step1 line format:
        // head \t slot \t child \t count
        String[] p = value.toString().split("\t");
        if (p.length < 4) return;

        String head = p[0];
        String slot = p[1];
        String child = p[2];
        String count = p[3];

        // Group by pattern: head + slot
        context.write(new Text(head + "\t" + slot), new Text(child + "\t" + count));
    }
}
