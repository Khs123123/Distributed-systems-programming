package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3_Reducer extends Reducer<Step3_Key, Step3_Value, Text, Text> {

    // Map to store N for each decade (e.g., "1990" -> 5000000, "2000" -> 8500000)
    private Map<String, Long> decadeNMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // --- FIX: Read the serialized decade counts from configuration ---
        // Expected format: "1990=50000,2000=60000,..."
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
                        // Ignore malformed entries
                    }
                }
            }
        }
    }

    @Override
    protected void reduce(Step3_Key key, Iterable<Step3_Value> values, Context context) throws IOException, InterruptedException {
        long c2 = 0;
        List<Step3_Value> step2DataBuffer = new ArrayList<>();

        // --- FIX: Retrieve the specific N for the current decade ---
        Long decadeN = decadeNMap.get(key.getDecade());
        
        // If N is missing for this decade, we cannot calculate LLR properly.
        // However, to be safe, we skip or use a default (but skipping is safer).
        if (decadeN == null || decadeN <= 0) {
            return; 
        }
        long N = decadeN; // Use the correct local N

        for (Step3_Value val : values) {
            if (val.getType() == Step3_Value.TYPE_UNIGRAM) {
                c2 = val.getC12(); // c2 value (stored in c12 field for Unigram type)
            } else {
                // Must deep copy the value to buffer it
                step2DataBuffer.add(new Step3_Value(val.getW1(), val.getC1(), val.getC12()));
            }
        }

        if (c2 > 0 && !step2DataBuffer.isEmpty()) {
            for (Step3_Value data : step2DataBuffer) {
                String w1 = data.getW1();
                String w2 = key.getWord();
                String decade = key.getDecade();
                
                // Double check for garbage (redundant but safe)
                if (!w1.matches("^[a-zA-Z\u0590-\u05FF]+$")) continue;

                double llr = calcLogLikelihoodRatio(data.getC1(), c2, data.getC12(), N);
                
                // Write output: Decade <tab> w1 w2 <tab> LLR
                context.write(new Text(decade), new Text(w1 + " " + w2 + "\t" + llr));
            }
        }
    }
    
    private double calcLogLikelihoodRatio(long c1, long c2, long c12, long N) {
        double k11 = c12;
        double k12 = c2 - c12;
        double k21 = c1 - c12;
        double k22 = N - (c1 + c2 - c12);

        // Safety check for invalid math (Negative counts due to data noise)
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