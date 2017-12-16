package houseware.learn.hadoop.mapred.partitioner1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @author fphilip@houseware.es
 */

@SuppressWarnings("unused")
public class WorldCounterPartitioner extends Partitioner<Text, LongWritable> {


    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        byte b = text.getBytes() [0];

        if(b>='A' && b<='Z'){
            return b-'A'+1;
        }   else {
            if(b>='a' && b<='z'){
                return b-'a'+1;
            }
        }
        
        return 0;
    }
}