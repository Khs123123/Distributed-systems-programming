package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3_Reducer extends Reducer<Step3_Key, Step3_Value, Text, Text> {

    private long N = 0; 

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getLong("N", 0);
    }

    @Override
    protected void reduce(Step3_Key key, Iterable<Step3_Value> values, Context context) throws IOException, InterruptedException {
        long c2 = 0;
        List<Step3_Value> step2DataBuffer = new ArrayList<>();

        for (Step3_Value val : values) {
            if (val.getType() == Step3_Value.TYPE_UNIGRAM) {
                c2 = val.getC12(); // c2 stored in c12 field for unigrams
            } else {
                // Must create new object for buffer (Deep Copy)
                step2DataBuffer.add(new Step3_Value(val.getW1(), val.getC1(), val.getC12()));
            }
        }

        if (c2 > 0 && !step2DataBuffer.isEmpty() && N > 0) {
            for (Step3_Value data : step2DataBuffer) {
                String w1 = data.getW1();
                String w2 = key.getWord();
                String decade = key.getDecade();
                
                // Double check for garbage
                if (!w1.matches("^[a-zA-Z\u0590-\u05FF]+$")) continue;

                double llr = calcLogLikelihoodRatio(data.getC1(), c2, data.getC12(), N);
                
                // Write standard format for Step 4 to read:
                // Decade <tab> w1 w2 <tab> LLR
                context.write(new Text(decade), new Text(w1 + " " + w2 + "\t" + llr));
            }
        }
    }
    
    private double calcLogLikelihoodRatio(long c1, long c2, long c12, long N) {
        double k11 = c12;
        double k12 = c2 - c12;
        double k21 = c1 - c12;
        double k22 = N - (c1 + c2 - c12);

        // Safety check
        if (k11 < 0 || k12 < 0 || k21 < 0 || k22 < 0) return 0;

        double logL = 2 * (
            entry(k11) + entry(k12) + entry(k21) + entry(k22)
            - entry(k11 + k12) - entry(k11 + k21) - entry(k12 + k22) - entry(k21 + k22)
            + entry(N)
        );
        
        return logL;
    }

    private double entry(double k) {
        if (k <= 0) return 0;
        return k * Math.log(k);
    }
}