package il.ac.bgu.cs.dsp;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Step3_GroupingComparator extends WritableComparator {
    protected Step3_GroupingComparator() { super(Step3_Key.class, true); }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Step3_Key k1 = (Step3_Key) a;
        Step3_Key k2 = (Step3_Key) b;
        
        int d = k1.getDecade().compareTo(k2.getDecade());
        if (d != 0) return d;
        
        return k1.getWord().compareTo(k2.getWord());
    }
}