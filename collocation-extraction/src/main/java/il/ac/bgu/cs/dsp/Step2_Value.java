package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step2_Value implements Writable {
    // סוגי המידע שאנחנו מעבירים
    public static final int TYPE_UNIGRAM = 1;
    public static final int TYPE_BIGRAM = 2;

    private int type;
    private String word2; // רלוונטי רק אם זה Bigram (שומר את המילה השנייה)
    private long count;   // שומר את c1 (אם זה Unigram) או את c12 (אם זה Bigram)

    // חובה: בנאי ריק ל-Hadoop
    public Step2_Value() {}

    public Step2_Value(int type, String word2, long count) {
        this.type = type;
        this.word2 = word2;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        // טיפול ב-Null כדי למנוע קריסות
        out.writeUTF(word2 != null ? word2 : ""); 
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        word2 = in.readUTF();
        count = in.readLong();
    }

    // Getters
    public int getType() { return type; }
    public String getWord2() { return word2; }
    public long getCount() { return count; }
    
    @Override
    public String toString() {
        return "Type: " + type + ", w2: " + word2 + ", count: " + count;
    }
}