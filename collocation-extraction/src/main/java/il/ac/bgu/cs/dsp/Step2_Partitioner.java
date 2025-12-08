package il.ac.bgu.cs.dsp;

import org.apache.hadoop.mapreduce.Partitioner;

public class Step2_Partitioner extends Partitioner<Step2_Key, Step2_Value> {
    @Override
    public int getPartition(Step2_Key key, Step2_Value value, int numPartitions) {
        // Partition only by Decade + Word1 (Ignore Type)
        // We use hash code to distribute load among reducers
        return Math.abs((key.getDecade() + key.getWord1()).hashCode()) % numPartitions;
    }
}