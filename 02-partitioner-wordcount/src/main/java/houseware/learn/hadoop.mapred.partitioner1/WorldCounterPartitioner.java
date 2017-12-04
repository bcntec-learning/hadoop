package houseware.learn.hadoop.mapred.partitioner1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author fphilip@houseware.es
 */

@SuppressWarnings("unused")
public class WorldCounterPartitioner extends Partitioner<Text, LongWritable> {


    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {

        return 0;
    }
}