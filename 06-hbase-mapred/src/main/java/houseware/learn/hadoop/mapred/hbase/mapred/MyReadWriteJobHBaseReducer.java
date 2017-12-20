package houseware.learn.hadoop.mapred.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MyReadWriteJobHBaseReducer {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "MyReadWriteJobSimple Program");
        job.setJarByClass(MyReadWriteJobHBaseReducer.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs
        String sourceTable = args[0];
        String targetTable = args[1];
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,      // input table
                scan,              // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper class
                null,              // mapper output key
                null,              // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                targetTable,        // output table
                MyTableReducer.class,    // reducer class
                job);
        job.setNumReduceTasks(1);
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

    }

    public static class MyMapper extends TableMapper<Text, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String val = new String(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("attr1")));
            text.set(val);     // we can only emit Writables...

            context.write(text, ONE);
        }
    }

    public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(i));

            context.write(null, put);
        }
    }


}
