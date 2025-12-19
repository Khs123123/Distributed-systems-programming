package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2_Reducer extends Reducer<Step2_Key, Step2_Value, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

// In Step2_Reducer.java

@Override
public void reduce(Step2_Key key, Iterable<Step2_Value> values, Context context) throws IOException, InterruptedException {
    long c1 = -1;
    
    // Variables for Bigram Aggregation
    String currentW2 = null;
    long currentBigramSum = 0;

    for (Step2_Value value : values) {
        if (value.getType() == Step2_Value.TYPE_UNIGRAM) {
            c1 = value.getCount();
        } 
        else if (value.getType() == Step2_Value.TYPE_BIGRAM) {
            // If c1 is missing, we can't calculate LLR, so skip
            if (c1 == -1) continue;

            String w2 = value.getWord2();

            // If we hit a NEW w2, emit the PREVIOUS one
            if (currentW2 != null && !w2.equals(currentW2)) {
                outputKey.set(key.getDecade() + "\t" + key.getWord1() + "\t" + currentW2);
                outputValue.set(c1 + "\t" + currentBigramSum);
                context.write(outputKey, outputValue);
                
                // Reset for new word
                currentBigramSum = 0;
            }

            // Update current
            currentW2 = w2;
            currentBigramSum += value.getCount();
        }
    }

    // Don't forget to emit the LAST bigram in the list!
    if (currentW2 != null && c1 != -1) {
        outputKey.set(key.getDecade() + "\t" + key.getWord1() + "\t" + currentW2);
        outputValue.set(c1 + "\t" + currentBigramSum);
        context.write(outputKey, outputValue);
        }
    }
}