package com.assignment3;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] p = value.toString().split("\t");
        if (p.length == 4) {
            context.write(new Text(p[1] + "\t" + p[2]), new Text(p[0] + "\t" + p[3]));
        }
    }
}