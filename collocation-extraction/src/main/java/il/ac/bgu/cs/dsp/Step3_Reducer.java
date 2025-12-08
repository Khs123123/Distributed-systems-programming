package il.ac.bgu.cs.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Step3_Reducer extends Reducer<Step3_Key, Step3_Value, Text, Text> {
    
    private Map<String, Long> decadeTotals = new HashMap<>();
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String step1OutputPath = conf.get("pathN");
        if (step1OutputPath == null) return; 

        Path path = new Path(step1OutputPath + "/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        
        if (fs.exists(path)) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 2) {
                        decadeTotals.put(parts[0].trim(), Long.parseLong(parts[1].trim()));
                    }
                }
            }
        }
    }

    @Override
    public void reduce(Step3_Key key, Iterable<Step3_Value> values, Context context) throws IOException, InterruptedException {
        long c2 = -1;
        
        for (Step3_Value value : values) {
            if (value.getType() == Step3_Value.TYPE_UNIGRAM) {
                c2 = value.getC12(); 
            } 
            else if (value.getType() == Step3_Value.TYPE_STEP2_DATA) {
                if (c2 != -1) {
                    String w1 = value.getW1();
                    long c1 = value.getC1();
                    long c12 = value.getC12();
                    String decade = key.getDecade();

                    if (decadeTotals.containsKey(decade)) {
                        long N = decadeTotals.get(decade);
                        double llr = calculateLLR(c1, c2, c12, N);
                        
                        outputKey.set(decade + "\t" + w1 + "\t" + key.getWord());
                        outputValue.set(String.valueOf(llr));
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    private double calculateLLR(long c1, long c2, long c12, long N) {
        double p = (double) c2 / N;
        double p1 = (double) c12 / c1;
        double p2 = (double) (c2 - c12) / (N - c1);

        double term1 = logL(c12, c1, p);
        double term2 = logL(c2 - c12, N - c1, p);
        double term3 = logL(c12, c1, p1);
        double term4 = logL(c2 - c12, N - c1, p2);
        double logLambda = term1 + term2 - term3 - term4;
        
        return -2 * logLambda;
    }

    private double logL(long k, long n, double x) {
        if (x == 0 || x == 1) return 0;
        return k * Math.log(x) + (n - k) * Math.log(1 - x);
    }
}