package il.ac.bgu.cs.dsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Step2_Value implements Writable {

    public static final int TYPE_UNIGRAM = 1;
    public static final int TYPE_BIGRAM = 2;

    private int type;
    private String word2; 
    private long count;   

    public Step2_Value() {}

    public Step2_Value(int type, String word2, long count) {
        this.type = type;
        this.word2 = word2;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeUTF(word2 != null ? word2 : ""); 
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        word2 = in.readUTF();
        count = in.readLong();
    }

    public int getType() { return type; }
    public String getWord2() { return word2; }
    public long getCount() { return count; }
    
    @Override
    public String toString() {
        return "Type: " + type + ", w2: " + word2 + ", count: " + count;
    }
}