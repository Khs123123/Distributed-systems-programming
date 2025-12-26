package il.ac.bgu.cs.dsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Step3_Key implements WritableComparable<Step3_Key> {
    private String decade;
    private String word; 
    private int type;    

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
        int decadeCmp = this.decade.compareTo(other.decade);
        if (decadeCmp != 0) return decadeCmp;

        int wordCmp = this.word.compareTo(other.word);
        if (wordCmp != 0) return wordCmp;

        return Integer.compare(this.type, other.type);
    }

    
    @Override
    public String toString() {
        return decade + "\t" + word + "\t" + type;
    }

    public String getDecade() { return decade; }
    public String getWord() { return word; }
}