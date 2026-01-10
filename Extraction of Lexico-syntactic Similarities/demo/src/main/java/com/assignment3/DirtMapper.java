package com.assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DirtMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Set<String> STOP_WORDS = new HashSet<>();

    static {
        STOP_WORDS.add("is");
        STOP_WORDS.add("are");
        STOP_WORDS.add("was");
        STOP_WORDS.add("were");
        STOP_WORDS.add("am");
        STOP_WORDS.add("been");
        STOP_WORDS.add("being");
        STOP_WORDS.add("be");
        STOP_WORDS.add("have");
        STOP_WORDS.add("has");
        STOP_WORDS.add("had");
    }

    private static class Token {
        String word;
        String pos;
        String dep;
        int headIndex;
        int originalIndex;

        public Token(String rawToken, int index) {
            int lastSlash = rawToken.lastIndexOf('/');
            String headStr = rawToken.substring(lastSlash + 1);
            
            String temp = rawToken.substring(0, lastSlash);
            int prevSlash = temp.lastIndexOf('/');
            String depStr = temp.substring(prevSlash + 1);
            
            temp = temp.substring(0, prevSlash);
            int posSlash = temp.lastIndexOf('/');
            String posStr = temp.substring(posSlash + 1);
            
            String wordStr = temp.substring(0, posSlash);

            this.word = wordStr;
            this.pos = posStr;
            this.dep = depStr;
            this.headIndex = Integer.parseInt(headStr);
            this.originalIndex = index;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 3) return;

        String ngramField = parts[1];
        String countStr = parts[2];

        String[] rawTokens = ngramField.split(" ");
        List<Token> tokens = new ArrayList<>();
        
        Token rootToken = null;

        for (int i = 0; i < rawTokens.length; i++) {
            try {
                Token t = new Token(rawTokens[i], i + 1);
                tokens.add(t);
                if (t.headIndex == 0) rootToken = t;
            } catch (Exception e) {
                return;
            }
        }

        if (rootToken == null) return;
        if (!rootToken.pos.startsWith("VB")) return;
        if (STOP_WORDS.contains(rootToken.word.toLowerCase())) return;

        for (Token t : tokens) {
            if (t == rootToken) continue;
            if (!t.pos.startsWith("NN")) continue;

            String path = extractPath(t, rootToken, tokens);
            
            if (path != null) {
                // ✅ התיקון הקריטי: Stemming גם ל-Slot וגם ל-Head!
                String stemmedSlot = Stemmer.stem(t.word);
                String stemmedHead = Stemmer.stem(rootToken.word); 
                
                // Key: Head (Stemmed) + Path
                // Value: SlotWord (Stemmed) + Count
                context.write(
                    new Text(stemmedHead + "\t" + path), 
                    new Text(stemmedSlot + "\t" + countStr)
                );
            }
        }
    }

    private String extractPath(Token slot, Token root, List<Token> allTokens) {
        if (slot.headIndex == root.originalIndex) {
            return slot.dep;
        }

        Token parent = findTokenByIndex(allTokens, slot.headIndex);
        if (parent != null) {
            if ((parent.pos.equals("IN") || parent.pos.equals("TO")) && 
                parent.headIndex == root.originalIndex) {
                return parent.dep + ":" + parent.word;
            }
        }
        return null;
    }

    private Token findTokenByIndex(List<Token> tokens, int index) {
        if (index <= 0 || index > tokens.size()) return null;
        return tokens.get(index - 1);
    }
}