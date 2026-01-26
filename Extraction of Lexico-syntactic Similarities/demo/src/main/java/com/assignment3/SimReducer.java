package com.assignment3;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String> goldStandard = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) return;
        
        try (BufferedReader br = new BufferedReader(new FileReader(new File(cacheFiles[0].getPath()).getName()))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.trim().split("\t");
                if (p.length >= 3) {
                    String v1 = cleanAndStem(p[0]);
                    String v2 = cleanAndStem(p[1]);
                    // מפתח משולב לחיפוש מהיר
                    goldStandard.put(v1 + "###" + v2, p[0] + "\t" + p[1] + "\t" + p[2]);
                }
            }
        }
    }

    private String cleanAndStem(String raw) {
        if (raw == null || raw.isEmpty()) return "";
        String verbOnly = raw.replaceAll("\\b[XY]\\b", "").trim().split("\\s+")[0];
        return Stemmer.stem(verbOnly.toLowerCase());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String slot = key.toString().split("\t")[0];
        
        // 1. Deduplication: שמירת ה-MI הטוב ביותר לכל פועל עבור המילה הזו
        Map<String, double[]> bestVerbs = new HashMap<>(); // verb -> {mi, sumMI}

        for (Text v : values) {
            String[] p = v.toString().split("\t");
            if (p.length < 3) continue;
            try {
                String verb = p[0];
                double mi = Double.parseDouble(p[1]);
                double sumMI = Double.parseDouble(p[2]);
                
                if (!bestVerbs.containsKey(verb) || mi > bestVerbs.get(verb)[0]) {
                    bestVerbs.put(verb, new double[]{mi, sumMI});
                }
            } catch (NumberFormatException e) { continue; }
        }

        // 2. יצירת צמדים והשוואה ל-Gold Standard
        List<String> verbList = new ArrayList<>(bestVerbs.keySet());
        for (int i = 0; i < verbList.size(); i++) {
            for (int j = i + 1; j < verbList.size(); j++) {
                String vName1 = verbList.get(i);
                String vName2 = verbList.get(j);

                String lookup = goldStandard.get(cleanAndStem(vName1) + "###" + cleanAndStem(vName2));
                if (lookup == null) lookup = goldStandard.get(cleanAndStem(vName2) + "###" + cleanAndStem(vName1));

                if (lookup != null) {
                    double mi1 = bestVerbs.get(vName1)[0];
                    double mi2 = bestVerbs.get(vName2)[0];
                    double den1 = bestVerbs.get(vName1)[1];
                    double den2 = bestVerbs.get(vName2)[1];

                    double numeratorPart = Math.min(mi1, mi2);
                    double pairDenominator = den1 + den2; 
                    
                    context.write(new Text(lookup), new Text(slot + ":" + numeratorPart + ":" + pairDenominator));
                }
            }
        }
    }
}