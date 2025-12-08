package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Step2_Reducer extends Reducer<Step2_Key, Step2_Value, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    public void reduce(Step2_Key key, Iterable<Step2_Value> values, Context context) throws IOException, InterruptedException {
        long c1 = -1;
        
        // Iterate through values
        for (Step2_Value value : values) {
            // Because of secondary sort, TYPE_UNIGRAM (1) always comes first!
            if (value.getType() == Step2_Value.TYPE_UNIGRAM) {
                c1 = value.getCount();
            } 
            else if (value.getType() == Step2_Value.TYPE_BIGRAM) {
                // If we haven't seen c1 yet, something is wrong (or word1 doesn't exist alone), skip.
                if (c1 != -1) {
                    long c12 = value.getCount();
                    String w2 = value.getWord2();
                    
                    // Output format: "Decade w1 w2" -> "c1 c12"
                    outputKey.set(key.getDecade() + "\t" + key.getWord1() + "\t" + w2);
                    outputValue.set(c1 + "\t" + c12);
                    
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
}