package com.assignment3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DirtReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        // Key: Head + Path (e.g. "eat \t dobj")
        // Values: List of "SlotWord \t Count" (e.g. "cake \t 10", "pie \t 5", "cake \t 2")
        
        // אנחנו צריכים לסכום את הספירות עבור כל מילה בנפרד בתוך הנתיב הזה
        Map<String, Integer> wordCounts = new HashMap<>();
        
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length < 2) continue;
            
            String slotWord = parts[0];
            try {
                int count = Integer.parseInt(parts[1]);
                wordCounts.put(slotWord, wordCounts.getOrDefault(slotWord, 0) + count);
            } catch (NumberFormatException e) {
                continue;
            }
        }
        
        // פלט: שורה לכל מילה ייחודית
        // Format: Head \t Path \t SlotWord \t TotalCount
        // זה הפורמט ש-MiMapper (שלב 2) מצפה לקבל
        
        for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
            String slotWord = entry.getKey();
            int totalCount = entry.getValue();
            
            // Output Key: Head \t Path
            // Output Value: SlotWord \t TotalCount
            context.write(key, new Text(slotWord + "\t" + totalCount));
        }
    }
}