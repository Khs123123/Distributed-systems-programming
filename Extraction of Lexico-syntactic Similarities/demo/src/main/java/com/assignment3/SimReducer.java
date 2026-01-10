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
    private static final int MAX_PATHS_PER_WORD = 1000;
    
    // שינוי: במקום Set, נשתמש ב-Map כדי לשמור את השם המקורי
    // Key: StemmedPair (e.g. "associ \t attribut")
    // Value: OriginalPair (e.g. "associate with \t attribute to")
    private Map<String, String> testPairsMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String testSetPath = conf.get("dirt.testset.path");

        if (testSetPath != null) {
            Path path = new Path(testSetPath);
            FileSystem fs = path.getFileSystem(conf);
            
            if (fs.exists(path)) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] phrases = line.trim().split("\t");
                        
                        if (phrases.length >= 2) {
                            String phrase1 = phrases[0].trim(); // "associate with"
                            String phrase2 = phrases[1].trim(); // "attribute to"
                            
                            // חילוץ הפועל הראשון לטובת Stemming
                            String verb1 = phrase1.split("\\s+")[0]; 
                            String verb2 = phrase2.split("\\s+")[0];

                            String w1 = Stemmer.stem(verb1); // "associ"
                            String w2 = Stemmer.stem(verb2); // "attribut"
                            
                            // שמירת המיפוי מהגרסה המקוצרת לגרסה המקורית המלאה
                            testPairsMap.put(w1 + "\t" + w2, phrase1 + "\t" + phrase2);
                            testPairsMap.put(w2 + "\t" + w1, phrase2 + "\t" + phrase1);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error reading test set: " + e.getMessage());
                }
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> heads = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        int count = 0;

        for (Text v : values) {
            String[] parts = v.toString().split("\t");
            if (parts.length < 2) continue;
            try {
                heads.add(parts[0]); 
                scores.add(Double.parseDouble(parts[1]));
                count++;
            } catch (Exception e) { continue; }
            if (count > MAX_PATHS_PER_WORD) break;
        }

        if (heads.size() < 2) return;

        for (int i = 0; i < heads.size(); i++) {
            for (int j = i + 1; j < heads.size(); j++) {
                String h1 = heads.get(i); // stemmed (e.g. associ)
                String h2 = heads.get(j); // stemmed (e.g. attribut)
                
                String checkKey = h1 + "\t" + h2;
                
                // בדיקה אם הזוג קיים במפה שלנו
                if (testPairsMap.containsKey(checkKey)) {
                    double score = scores.get(i) + scores.get(j);
                    
                    // שליפה של השמות המקוריים (המלאים) מהמפה
                    String originalPair = testPairsMap.get(checkKey);
                    
                    // כתיבת הפלט עם השמות המקוריים!
                    context.write(new Text(originalPair), new Text(String.valueOf(score)));
                }
            }
        }
    }
}