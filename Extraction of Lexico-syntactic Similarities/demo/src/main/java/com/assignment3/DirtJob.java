package com.assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DirtJob {

    public static void main(String[] args) throws Exception {

        int startIndex = 0;
        if (args.length > 0 && args[0].equals("com.assignment3.DirtJob")) startIndex = 1;

        if (args.length - startIndex != 3) {
            System.err.println("Usage: DirtJob <raw_input> <test_set_input> <output_base>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String rawInput = args[startIndex];
        String testSet = args[startIndex + 1];
        String outputBase = args[startIndex + 2];

        // Step 1
        Job j1 = Job.getInstance(conf, "DIRT Step 1");
        j1.setJarByClass(DirtJob.class);
        j1.setMapperClass(DirtMapper.class);
        j1.setReducerClass(DirtReducer.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j1, new Path(rawInput));
        FileOutputFormat.setOutputPath(j1, new Path(outputBase + "/step1"));
        if (!j1.waitForCompletion(true)) System.exit(1);

        // Step 2
        Job j2 = Job.getInstance(conf, "DIRT Step 2");
        j2.setJarByClass(DirtJob.class);
        j2.setMapperClass(MiMapper.class);
        j2.setReducerClass(MiReducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j2, new Path(outputBase + "/step1"));
        FileOutputFormat.setOutputPath(j2, new Path(outputBase + "/step2"));
        if (!j2.waitForCompletion(true)) System.exit(1);

        // Step 3
        Job j3 = Job.getInstance(conf, "DIRT Step 3");
        j3.getConfiguration().set("dirt.testset.path", testSet);
        j3.setJarByClass(DirtJob.class);
        j3.setMapperClass(SimMapper.class);
        j3.setReducerClass(SimReducer.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j3, new Path(outputBase + "/step2"));
        FileOutputFormat.setOutputPath(j3, new Path(outputBase + "/final"));

        System.exit(j3.waitForCompletion(true) ? 0 : 1);
    }
}
