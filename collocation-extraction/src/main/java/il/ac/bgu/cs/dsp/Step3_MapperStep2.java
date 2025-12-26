package il.ac.bgu.cs.dsp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step3_MapperStep2 extends Mapper<Object, Text, Step3_Key, Step3_Value> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length >= 5) {
            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            long c1 = Long.parseLong(parts[3]);
            long c12 = Long.parseLong(parts[4]);

            Step3_Key newKey = new Step3_Key(decade, w2, Step3_Value.TYPE_STEP2_DATA);
            context.write(newKey, new Step3_Value(w1, c1, c12));
        }
    }
}