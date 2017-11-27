package houseware.learn.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author fphilip@houseware.es
 */

@SuppressWarnings("unused")
public class WorldCounterReduce extends Reducer<Text, LongWritable, Text, LongWritable> {


    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long wordOccurrences = 0;

        for (LongWritable value : values) {
            wordOccurrences++;
        }

        context.write(key, new LongWritable(wordOccurrences));

    }
}