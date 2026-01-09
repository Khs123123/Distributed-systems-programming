package com.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // step2 output:
        // head \t slot \t child \t mi
        String[] p = value.toString().split("\t");
        if (p.length < 4) return;

        String pattern = p[0] + "\t" + p[1]; // head \t slot
        String child = p[2];
        String mi = p[3];

        // group by (slot, child)
        context.write(new Text(p[1] + "\t" + child), new Text(pattern + "\t" + mi));
    }
}
