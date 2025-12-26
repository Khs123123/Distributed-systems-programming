package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] parts = line.split("\t"); 
        
        if (parts.length >= 3) {
            String decade = parts[0];
            String bigram = parts[1];
            String llrStr = parts[2];
            
            try {
                double llr = Double.parseDouble(llrStr);
                context.write(new Text(decade), new Text(llr + "\t" + bigram));
                
            } catch (NumberFormatException e) {
            }
        }
    }
}