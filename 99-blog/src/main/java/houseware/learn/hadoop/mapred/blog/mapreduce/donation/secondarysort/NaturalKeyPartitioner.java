package houseware.learn.hadoop.mapred.blog.mapreduce.donation.secondarysort;

import data.writable.DonationWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, DonationWritable> {

    @Override
    public int getPartition(CompositeKey key, DonationWritable value, int numPartitions) {

        // Automatic n-partitioning using hash on the state name
        return Math.abs(key.state.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}
