package il.ac.bgu.cs.dsp;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Step2_GroupingComparator extends WritableComparator {
    protected Step2_GroupingComparator() {
        super(Step2_Key.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Step2_Key key1 = (Step2_Key) a;
        Step2_Key key2 = (Step2_Key) b;

        // Compare ONLY Decade and Word1
        int decadeCmp = key1.getDecade().compareTo(key2.getDecade());
        if (decadeCmp != 0) return decadeCmp;

        return key1.getWord1().compareTo(key2.getWord1());
    }
}