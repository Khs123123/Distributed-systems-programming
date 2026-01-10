package com.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalScore = 0.0;
        
        for (Text val : values) {
            try {
                totalScore += Double.parseDouble(val.toString());
            } catch (NumberFormatException e) {
                // התעלמות מערכים לא תקינים
            }
        }
        
        // כתיבת התוצאה הסופית: word1 \t word2 \t totalScore
        context.write(key, new Text(String.valueOf(totalScore)));
    }
}