package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3_Reducer extends Reducer<Step3_Key, Step3_Value, Text, Text> {

    private Map<String, Long> decadeNMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String serialized = context.getConfiguration().get("DecadeCounts", "");
        
        if (!serialized.isEmpty()) {
            String[] pairs = serialized.split(",");
            for (String pair : pairs) {
                String[] kv = pair.split("=");
                if (kv.length == 2) {
                    try {
                        String decade = kv[0];
                        long count = Long.parseLong(kv[1]);
                        decadeNMap.put(decade, count);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }
    }

    @Override
    protected void reduce(Step3_Key key, Iterable<Step3_Value> values, Context context) throws IOException, InterruptedException {
        long c2 = 0;
        List<Step3_Value> step2DataBuffer = new ArrayList<>();

        Long decadeN = decadeNMap.get(key.getDecade());
        
        if (decadeN == null || decadeN <= 0) {
            return; 
        }
        long N = decadeN;

        for (Step3_Value val : values) {
            if (val.getType() == Step3_Value.TYPE_UNIGRAM) {
                c2 = val.getC12();
            } else {
                step2DataBuffer.add(new Step3_Value(val.getW1(), val.getC1(), val.getC12()));
            }
        }

        if (c2 > 0 && !step2DataBuffer.isEmpty()) {
            for (Step3_Value data : step2DataBuffer) {
                String w1 = data.getW1();
                String w2 = key.getWord();
                String decade = key.getDecade();
                
                if (!w1.matches("^[a-zA-Z\u0590-\u05FF]+$")) continue;

                double llr = calcLogLikelihoodRatio(data.getC1(), c2, data.getC12(), N);
                
                //output: Decade <tab> w1 w2 <tab> LLR
                context.write(new Text(decade), new Text(w1 + " " + w2 + "\t" + llr));
            }
        }
    }
    
    private double calcLogLikelihoodRatio(long c1, long c2, long c12, long N) {
        double k11 = c12;
        double k12 = c2 - c12;
        double k21 = c1 - c12;
        double k22 = N - (c1 + c2 - c12);

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