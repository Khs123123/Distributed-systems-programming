package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Step1_Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        
        // Sum up all the counts for this decade
        for (LongWritable val : values) {
            sum += val.get();
        }
        
        result.set(sum);
        context.write(key, result); // Output example: "1990  5000000000"
    }
}