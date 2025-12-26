package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step3_MapperUnigram extends Mapper<LongWritable, Text, Step3_Key, Step3_Value> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length >= 3) {
            String w = parts[0]; 
            try {
                int year = Integer.parseInt(parts[1]);
                String decade = String.valueOf((year / 10) * 10);
                long count = Long.parseLong(parts[2]); 

                Step3_Key newKey = new Step3_Key(decade, w, Step3_Value.TYPE_UNIGRAM);
                context.write(newKey, new Step3_Value(count));
            } catch (Exception e) {}
        }
    }
}