package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step2_Key implements WritableComparable<Step2_Key> {
    private String decade;
    private String word1;
    private int type; // 1 = Unigram, 2 = Bigram

    public Step2_Key() {}

    public Step2_Key(String decade, String word1, int type) {
        this.decade = decade;
        this.word1 = word1;
        this.type = type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(decade);
        out.writeUTF(word1);
        out.writeInt(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade = in.readUTF();
        word1 = in.readUTF();
        type = in.readInt();
    }

    @Override
    public int compareTo(Step2_Key other) {
        // 1. Compare Decade
        int decadeCmp = this.decade.compareTo(other.decade);
        if (decadeCmp != 0) return decadeCmp;

        // 2. Compare Word1
        int wordCmp = this.word1.compareTo(other.word1);
        if (wordCmp != 0) return wordCmp;

        // 3. Compare Type (Crucial: 1 comes before 2)
        return Integer.compare(this.type, other.type);
    }
    
    // Getters needed for Partitioner/Grouping
    public String getDecade() { return decade; }
    public String getWord1() { return word1; }
}