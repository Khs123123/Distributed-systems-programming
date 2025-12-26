package il.ac.bgu.cs.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;     
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ExtractCollocations {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: ExtractCollocations <1-gram input> <2-gram input> <output path> <language>");
            System.exit(-1);
        }

        String input1Gram = args[0];
        String input2Gram = args[1];
        String basePath = args[2];
        
        Configuration conf = new Configuration();


        // Job 1: Calculate Total Word Count (N) per Decade
        Job job1 = Job.getInstance(conf, "Step 1: Calculate N");
        job1.setJarByClass(ExtractCollocations.class);
        job1.setMapperClass(Step1_Mapper.class);
        job1.setCombinerClass(Step1_Reducer.class);
        job1.setReducerClass(Step1_Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        
        // Use SequenceFileInputFormat for Google N-Grams data
        job1.setInputFormatClass(SequenceFileInputFormat.class); 
        job1.setOutputFormatClass(TextOutputFormat.class); 
        
        MultipleInputs.addInputPath(job1, new Path(input1Gram), SequenceFileInputFormat.class, Step1_Mapper.class);
        
        Path outputStep1 = new Path(basePath + "/step1_output");
        FileOutputFormat.setOutputPath(job1, outputStep1);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // Retrieve the counters generated in Job 1 (N per decade)
        // We serialize them into a string to pass into the Configuration for Job 3
        StringBuilder decadeCountsSerialized = new StringBuilder();
        CounterGroup decadeGroup = job1.getCounters().getGroup("DecadeCounts");
        
        for (Counter counter : decadeGroup) {
            if (decadeCountsSerialized.length() > 0) {
                decadeCountsSerialized.append(",");
            }
            decadeCountsSerialized.append(counter.getName()).append("=").append(counter.getValue());
        }
        
        System.out.println("DEBUG: Serialized Decade Counts: " + decadeCountsSerialized.toString());


        // Job 2: Join Unigram Counts (C1) with Bigrams
        Job job2 = Job.getInstance(conf, "Step 2: Join c1");
        job2.setJarByClass(ExtractCollocations.class);
        job2.setMapOutputKeyClass(Step2_Key.class);
        job2.setMapOutputValueClass(Step2_Value.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setPartitionerClass(Step2_Partitioner.class);
        job2.setGroupingComparatorClass(Step2_GroupingComparator.class);
        
        MultipleInputs.addInputPath(job2, new Path(input1Gram), SequenceFileInputFormat.class, Step2_MapperUnigram.class);
        MultipleInputs.addInputPath(job2, new Path(input2Gram), SequenceFileInputFormat.class, Step2_MapperBigram.class);
        
        job2.setReducerClass(Step2_Reducer.class);
        job2.setOutputFormatClass(TextOutputFormat.class); 

        Path outputStep2 = new Path(basePath + "/step2_output");
        FileOutputFormat.setOutputPath(job2, outputStep2);

        if (!job2.waitForCompletion(true)) System.exit(1);

        // Job 3: Join Word2 Counts (C2) and Calculate LLR
        Configuration conf3 = new Configuration();
        
        if (decadeCountsSerialized.length() > 0) {
            conf3.set("DecadeCounts", decadeCountsSerialized.toString());
        } else {
            System.err.println("WARNING: No Decade Counts found in Step 1!");
        }
        
        Job job3 = Job.getInstance(conf3, "Step 3: Calculate LLR");
        job3.setJarByClass(ExtractCollocations.class);
        job3.setMapOutputKeyClass(Step3_Key.class);
        job3.setMapOutputValueClass(Step3_Value.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setPartitionerClass(Step3_Partitioner.class);
        job3.setGroupingComparatorClass(Step3_GroupingComparator.class);

        // Input 1: Original Unigrams (for C2 counts)
        MultipleInputs.addInputPath(job3, new Path(input1Gram), SequenceFileInputFormat.class, Step3_MapperUnigram.class);
        
        // Input 2: Step 2 Output (Partial Bigram Data)
        MultipleInputs.addInputPath(job3, outputStep2, TextInputFormat.class, Step3_MapperStep2.class);
        
        job3.setReducerClass(Step3_Reducer.class);
        Path outputStep3 = new Path(basePath + "/step3_output");
        FileOutputFormat.setOutputPath(job3, outputStep3);

        if (!job3.waitForCompletion(true)) System.exit(1);

        // Job 4: Top 100 Collocations per Decade
        Job job4 = Job.getInstance(conf, "Step 4: Top 100");
        job4.setJarByClass(ExtractCollocations.class);
        job4.setMapperClass(Step4_Mapper.class);
        job4.setReducerClass(Step4_Reducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job4, outputStep3);
        Path outputFinal = new Path(basePath + "/final_output");
        FileOutputFormat.setOutputPath(job4, outputFinal);

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}