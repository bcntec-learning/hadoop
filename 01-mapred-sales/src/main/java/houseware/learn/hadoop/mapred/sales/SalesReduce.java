package houseware.learn.hadoop.mapred.sales;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class SalesReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        int frequencyForCountry = 0;
        while (values.hasNext()) {
            IntWritable value = values.next();
            frequencyForCountry += value.get();

        }
        output.collect(key, new IntWritable(frequencyForCountry));
    }
}
