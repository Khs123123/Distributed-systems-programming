package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Expected Input from Step 3: 
        // Decade <tab> w1 w2 <tab> LLR
        // Example: "1990\tcomputer science\t540.23"
        
        String line = value.toString();
        String[] parts = line.split("\t"); 
        
        // We expect at least 3 parts: [Decade, "w1 w2", LLR]
        if (parts.length >= 3) {
            String decade = parts[0];
            String bigram = parts[1]; // "w1 w2"
            String llrStr = parts[2];
            
            try {
                double llr = Double.parseDouble(llrStr);
                
                // We want to sort by LLR in the Reducer.
                // To do this, we can make the Key a "Composite Key" or just pass it to Reducer to sort locally.
                // For simplicity, let's output:
                // Key: Decade
                // Value: LLR + TAB + Bigram
                
                context.write(new Text(decade), new Text(llr + "\t" + bigram));
                
            } catch (NumberFormatException e) {
                // Ignore lines with bad math
            }
        }
    }
}