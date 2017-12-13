package houseware.learn.hadoop.mapred.sales;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class SalesReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int frequencyForCountry = 0;
        Iterator<IntWritable> i = values.iterator();
        while (i.hasNext()) {
            IntWritable value = i.next();
            frequencyForCountry += value.get();

        }
        context.write(key, new IntWritable(frequencyForCountry));
    }
}
