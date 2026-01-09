package com.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DirtMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    public enum C {
        LINES,
        BAD_FORMAT,
        NO_REL,
        NO_VB,
        NO_NN,
        EMITTED
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.getCounter(C.LINES).increment(1);

        // BIARC lines are TAB-separated
        String[] parts = value.toString().split("\t");
        if (parts.length < 4) {
            context.getCounter(C.BAD_FORMAT).increment(1);
            return;
        }

        // Head token and Child token
        String headTok = parts[0];
        String relTok  = parts[1];
        String childTok = parts[3];

        // relation -> slot X/Y
        String slot = relationToSlot(relTok);
        if (slot == null) {
            context.getCounter(C.NO_REL).increment(1);
            return;
        }

        // POS checks (head must be VB*, child must be NN*)
        String headPos = extractPos(headTok);
        String childPos = extractPos(childTok);

        if (headPos == null || !headPos.startsWith("VB")) {
            context.getCounter(C.NO_VB).increment(1);
            return;
        }
        if (childPos == null || !childPos.startsWith("NN")) {
            context.getCounter(C.NO_NN).increment(1);
            return;
        }

        // Normalize words: split on "/" or "_" and stem the FIRST word-part
        String headWord = Stemmer.stem(extractWord(headTok));
        String childWord = Stemmer.stem(extractWord(childTok));

        if (headWord.isEmpty() || childWord.isEmpty()) {
            context.getCounter(C.BAD_FORMAT).increment(1);
            return;
        }

        // Key: head \t slot \t child
        context.write(new Text(headWord + "\t" + slot + "\t" + childWord), ONE);
        context.getCounter(C.EMITTED).increment(1);
    }

    private static String relationToSlot(String rel) {
        if (rel == null) return null;
        String r = rel.toLowerCase();
        if (r.contains("subj")) return "X";
        if (r.contains("obj")) return "Y";
        return null;
    }

    // "eat/VB" or "eat/VBD" or "eat_VB" → "VB" / "VBD"
    private static String extractPos(String token) {
        if (token == null) return null;
        String t = token.trim();

        // prefer slash POS: word/POS
        int lastSlash = t.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < t.length() - 1) {
            String pos = t.substring(lastSlash + 1);
            // strip after underscore if exists: POS_extra
            int us = pos.indexOf('_');
            if (us >= 0) pos = pos.substring(0, us);
            return pos.toUpperCase();
        }

        // fallback underscore: word_POS
        int us = t.lastIndexOf('_');
        if (us >= 0 && us < t.length() - 1) {
            String pos = t.substring(us + 1);
            return pos.toUpperCase();
        }

        return null;
    }

    // "eat/VB" or "eat_VB" or "eat/VB_x" → "eat"
    private static String extractWord(String token) {
        if (token == null) return "";
        String t = token.trim().toLowerCase();
        // split on / or _
        String[] parts = t.split("[/_]");
        return (parts.length > 0) ? parts[0] : "";
    }
}
