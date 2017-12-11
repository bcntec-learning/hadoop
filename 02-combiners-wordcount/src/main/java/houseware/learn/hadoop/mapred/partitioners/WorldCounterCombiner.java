package houseware.learn.hadoop.mapred.partitioners;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author fphilip@houseware.es
 */
public class WorldCounterCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        long wordOccurrences = 0;

        for (LongWritable value : values) {
            wordOccurrences++;
        }

        context.write(key, new LongWritable(wordOccurrences));

    }
}
