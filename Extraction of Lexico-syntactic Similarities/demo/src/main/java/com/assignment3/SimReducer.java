package com.assignment3;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String> testPairsMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String testSetPath = conf.get("dirt.testset.path");
        if (testSetPath != null) {
            Path path = new Path(testSetPath);
            FileSystem fs = path.getFileSystem(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] phrases = line.trim().split("\t");
                    if (phrases.length >= 2) {
                        String v1 = Stemmer.stem(phrases[0].split("\\s+")[0]);
                        String v2 = Stemmer.stem(phrases[1].split("\\s+")[0]);
                        testPairsMap.put(v1 + "\t" + v2, phrases[0] + "\t" + phrases[1]);
                        testPairsMap.put(v2 + "\t" + v1, phrases[1] + "\t" + phrases[0]);
                    }
                }
            } catch (Exception e) {}
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> verbs = new ArrayList<>();
        List<Double> mis = new ArrayList<>();
        List<Double> pathTotals = new ArrayList<>();

        for (Text v : values) {
            String[] p = v.toString().split("\t");
            if (p.length < 3) continue;
            verbs.add(p[0]); mis.add(Double.parseDouble(p[1])); pathTotals.add(Double.parseDouble(p[2]));
        }

        for (int i = 0; i < verbs.size(); i++) {
            for (int j = i + 1; j < verbs.size(); j++) {
                String pairKey = verbs.get(i) + "\t" + verbs.get(j);
                if (testPairsMap.containsKey(pairKey)) {
                    // DIRT Formula 3: Shared Information / Total Information
                    double numerator = mis.get(i) + mis.get(j);
                    double denominator = pathTotals.get(i) + pathTotals.get(j);
                    double partialSim = (denominator == 0) ? 0 : numerator / denominator;
                    context.write(new Text(testPairsMap.get(pairKey)), new Text(String.valueOf(partialSim)));
                }
            }
        }
    }
}