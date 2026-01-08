package com.assignment3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> paths = new ArrayList<>();
        for (Text val : values) paths.add(val.toString());
        
        for (int i = 0; i < paths.size(); i++) {
            for (int j = i + 1; j < paths.size(); j++) {
                String[] p1 = paths.get(i).split("\t"), p2 = paths.get(j).split("\t");
                // Similarity calculation based on shared word intersection 
                double sharedMi = Double.parseDouble(p1[1]) + Double.parseDouble(p2[1]);
                context.write(new Text(p1[0] + "," + p2[0]), new Text(key.toString().split("\t")[0] + "\t" + sharedMi));
            }
        }
    }
}