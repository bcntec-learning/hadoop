package houseware.learn.hadoop.mapred.sales;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class SalesMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        context.write(new Text(SingleCountryData[7]), one);
    }
}
