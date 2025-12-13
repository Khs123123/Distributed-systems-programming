package il.ac.bgu.cs.dsp;
import org.apache.hadoop.mapreduce.Partitioner;

public class Step3_Partitioner extends Partitioner<Step3_Key, Step3_Value> {
    @Override
    public int getPartition(Step3_Key key, Step3_Value value, int numPartitions) {
        // Partition by Decade + Word hash
        return Math.abs((key.getDecade() + key.getWord()).hashCode()) % numPartitions;
    }
}