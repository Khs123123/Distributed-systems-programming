package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1_Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    // Define a Global Counter
    public static enum Counter { N }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        
        // INCREMENT GLOBAL COUNTER (Calculates N)
        context.getCounter(Counter.N).increment(sum);
        
        context.write(key, new LongWritable(sum));
    }
}