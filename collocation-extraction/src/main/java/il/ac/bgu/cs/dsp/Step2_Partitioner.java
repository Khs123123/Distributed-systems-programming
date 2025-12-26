package il.ac.bgu.cs.dsp;

import org.apache.hadoop.mapreduce.Partitioner;

public class Step2_Partitioner extends Partitioner<Step2_Key, Step2_Value> {
    @Override
    public int getPartition(Step2_Key key, Step2_Value value, int numPartitions) {
        // Ensure that all keys sharing the same Decade and Word1 go to the same reducer.
        return Math.abs((key.getDecade() + key.getWord1()).hashCode()) % numPartitions;
    }
}