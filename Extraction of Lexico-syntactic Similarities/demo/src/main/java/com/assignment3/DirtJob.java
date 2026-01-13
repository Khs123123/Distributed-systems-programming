package com.assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

        // ✅ Correct "Requester Pays" for s3a://
        conf.setBoolean("fs.s3a.requester.pays.enabled", true);

        String rawInput  = args[startIndex];
        String testSet   = args[startIndex + 1];
        String outputBase = args[startIndex + 2];

        // ---------------------------------------------------------
        // Step 1: Extraction & Parsing
        // ---------------------------------------------------------
        Job j1 = Job.getInstance(conf, "DIRT Step 1: Extraction");
        j1.setJarByClass(DirtJob.class);
        j1.setMapperClass(DirtMapper.class);
        j1.setReducerClass(DirtReducer.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);

        // ✅ Add each comma-separated path separately
        String[] inputPaths = rawInput.split(",");
        for (String path : inputPaths) {
            String p = path.trim();
            if (!p.isEmpty()) {
                FileInputFormat.addInputPath(j1, new Path(p));
            }
        }

        FileOutputFormat.setOutputPath(j1, new Path(outputBase + "/step1"));
        if (!j1.waitForCompletion(true)) System.exit(1);

        // ---------------------------------------------------------
        // Step 2: MI Calculation
        // ---------------------------------------------------------
        Job j2 = Job.getInstance(conf, "DIRT Step 2: MI Calculation");
        j2.setJarByClass(DirtJob.class);
        j2.setMapperClass(MiMapper.class);
        j2.setReducerClass(MiReducer.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j2, new Path(outputBase + "/step1"));
        FileOutputFormat.setOutputPath(j2, new Path(outputBase + "/step2"));
        if (!j2.waitForCompletion(true)) System.exit(1);

        // ---------------------------------------------------------
        // Step 3: Similarity (Filtered for Test Set)
        // ---------------------------------------------------------
        Job j3 = Job.getInstance(conf, "DIRT Step 3: Similarity");
        j3.getConfiguration().set("dirt.testset.path", testSet);

        j3.setJarByClass(DirtJob.class);
        j3.setMapperClass(SimMapper.class);
        j3.setReducerClass(SimReducer.class);

        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j3, new Path(outputBase + "/step2"));
        FileOutputFormat.setOutputPath(j3, new Path(outputBase + "/final"));
        if (!j3.waitForCompletion(true)) System.exit(1);

        // ---------------------------------------------------------
        // Step 4: Aggregation (Final unique pair scores)
        // ---------------------------------------------------------
        Job j4 = Job.getInstance(conf, "DIRT Step 4: Aggregation");
        j4.setJarByClass(DirtJob.class);
        j4.setMapperClass(SumMapper.class);
        j4.setReducerClass(SumReducer.class);

        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j4, new Path(outputBase + "/final"));
        FileOutputFormat.setOutputPath(j4, new Path(outputBase + "/aggregated_result"));

        System.exit(j4.waitForCompletion(true) ? 0 : 1);
    }
}
