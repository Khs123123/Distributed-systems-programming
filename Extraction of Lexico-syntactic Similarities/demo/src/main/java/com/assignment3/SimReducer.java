package com.assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // key = slot \t word
        // values = pattern \t mi

        List<String> patterns = new ArrayList<>();
        List<Double> mis = new ArrayList<>();

        for (Text v : values) {
            String[] p = v.toString().split("\t");
            if (p.length < 3) continue;
            // pattern is 2 fields: head \t slot, then mi
            String pattern = p[0] + "\t" + p[1];
            double mi = Double.parseDouble(p[2]);

            patterns.add(pattern);
            mis.add(mi);
        }

        // For each pair sharing this word, emit contribution
        // (You can sum later if needed)
        for (int i = 0; i < patterns.size(); i++) {
            for (int j = i + 1; j < patterns.size(); j++) {
                String p1 = patterns.get(i);
                String p2 = patterns.get(j);
                double contrib = Math.min(mis.get(i), mis.get(j));

                context.write(new Text(p1 + "\t" + p2), new Text(key.toString() + "\t" + contrib));
            }
        }
    }
}
