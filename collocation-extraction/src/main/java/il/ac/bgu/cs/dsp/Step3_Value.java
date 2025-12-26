package il.ac.bgu.cs.dsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Step3_Value implements Writable {
    public static final int TYPE_UNIGRAM = 1;
    public static final int TYPE_STEP2_DATA = 2;

    private int type;
    private String w1;  
    private long c1;    
    private long c12;   

    public Step3_Value() {}

    public Step3_Value(long c2) {
        this.type = TYPE_UNIGRAM;
        this.w1 = "";
        this.c1 = 0;
        this.c12 = c2; 
    }

    public Step3_Value(String w1, long c1, long c12) {
        this.type = TYPE_STEP2_DATA;
        this.w1 = w1;
        this.c1 = c1;
        this.c12 = c12;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeUTF(w1 != null ? w1 : "");
        out.writeLong(c1);
        out.writeLong(c12);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        w1 = in.readUTF();
        c1 = in.readLong();
        c12 = in.readLong();
    }

    public int getType() { return type; }
    public String getW1() { return w1; }
    public long getC1() { return c1; }
    public long getC12() { return c12; }
}