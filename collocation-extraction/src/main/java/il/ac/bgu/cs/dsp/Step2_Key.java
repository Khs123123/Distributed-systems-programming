package il.ac.bgu.cs.dsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Step2_Key implements WritableComparable<Step2_Key> {
    
    public static final int TYPE_UNIGRAM = 1;
    public static final int TYPE_BIGRAM = 2;

    private String decade;
    private String w1;
    private String w2; // Empty if Unigram
    private int type;

    public Step2_Key() {}

    // Constructor 1: Full (Used by Bigram Mapper)
    public Step2_Key(String decade, String w1, String w2, int type) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
        this.type = type;
    }

    // Constructor 2: 3-Arguments (ADDED THIS to fix the error in Unigram Mapper)
    public Step2_Key(String decade, String w1, int type) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = ""; // Default empty
        this.type = type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(decade);
        out.writeUTF(w1);
        out.writeUTF(w2);
        out.writeInt(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade = in.readUTF();
        w1 = in.readUTF();
        w2 = in.readUTF();
        type = in.readInt();
    }

// In Step2_Key.java

@Override
public int compareTo(Step2_Key other) {
    // 1. Compare Decade
    int d = this.decade.compareTo(other.decade);
    if (d != 0) return d;
    
    // 2. Compare Word1
    int w = this.w1.compareTo(other.w1);
    if (w != 0) return w;
    
    // 3. Compare Type (Unigram < Bigram)
    int t = Integer.compare(this.type, other.type);
    if (t != 0) return t;

    // 4. Compare Word2 (Fix for Aggregation!)
    // If both are Bigrams, we must sort by w2 so duplicates appear together
    if (this.type == TYPE_BIGRAM) {
        return this.w2.compareTo(other.w2);
    }
    
    return 0;
}

    public String getDecade() { return decade; }
    public String getWord1() { return w1; }
    public String getWord2() { return w2; }
    public int getType() { return type; }
}