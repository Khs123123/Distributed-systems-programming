package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input Format form Step 3: Decade <tab> w1 <tab> w2 <tab> LLR
        String[] parts = value.toString().split("\t");
        
        if (parts.length >= 4) {
            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            String llr = parts[3];

            // Key: Decade (e.g., "1990")
            // Value: "LLR <tab> w1 w2"
            context.write(new Text(decade), new Text(llr + "\t" + w1 + " " + w2));
        }
    }
}