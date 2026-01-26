package com.assignment3;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Double> finalResults = new LinkedHashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) return;
        
        // Get the filename only (e.g., "full_test_set.txt")
        // Hadoop creates a local symlink with this name in the working directory
        String localFileName = new Path(cacheFiles[0].getPath()).getName();
        
        try (BufferedReader br = new BufferedReader(new FileReader(localFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    String normalized = line.replaceAll("\\s+", "\t");
                    finalResults.put(normalized, 0.0);
                }
            }
        } catch (Exception e) {
            // Log the error to stderr so you can see it in EMR logs
            System.err.println("Error loading cache file '" + localFileName + "': " + e.getMessage());
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> numSum = new HashMap<>();
        Map<String, Double> denMax = new HashMap<>();
        
        for (Text val : values) {
            String[] p = val.toString().split(":");
            if (p.length < 3) continue;
            
            String slot = p[0];
            try {
                numSum.put(slot, numSum.getOrDefault(slot, 0.0) + Double.parseDouble(p[1]));
                denMax.put(slot, Math.max(denMax.getOrDefault(slot, 0.0), Double.parseDouble(p[2])));
            } catch (NumberFormatException e) { continue; }
        }
        
        double sX = calcLin(numSum, denMax, "SlotX");
        double sY = calcLin(numSum, denMax, "SlotY");
        double finalScore = Math.sqrt(sX * sY);

        // עדכון ישיר של המפה (המפתח כבר מנורמל מה-Mapper)
        finalResults.put(key.toString(), finalScore);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // פליטת כל 2580 השורות מהמפה לקובץ הפלט
        for (Map.Entry<String, Double> entry : finalResults.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(String.format("%.6f", entry.getValue())));
        }
    }

    private double calcLin(Map<String, Double> ns, Map<String, Double> ds, String slot) {
        double n = ns.getOrDefault(slot, 0.0), d = ds.getOrDefault(slot, 0.0);
        return (d <= 0) ? 0 : Math.min(1.0, n / d);
    }
}