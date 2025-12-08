package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step3_Key implements WritableComparable<Step3_Key> {
    private String decade;
    private String word; // Represents w2
    private int type;    // 1 = Unigram (c2), 2 = Step2 Data

    public Step3_Key() {}

    public Step3_Key(String decade, String word, int type) {
        this.decade = decade;
        this.word = word;
        this.type = type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(decade);
        out.writeUTF(word);
        out.writeInt(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade = in.readUTF();
        word = in.readUTF();
        type = in.readInt();
    }

    @Override
    public int compareTo(Step3_Key other) {
        // 1. Sort by Decade
        int decadeCmp = this.decade.compareTo(other.decade);
        if (decadeCmp != 0) return decadeCmp;

        // 2. Sort by Word (w2)
        int wordCmp = this.word.compareTo(other.word);
        if (wordCmp != 0) return wordCmp;

        // 3. Sort by Type: Ensure Type 1 comes before Type 2
        return Integer.compare(this.type, other.type);
    }

    public String getDecade() { return decade; }
    public String getWord() { return word; }
}