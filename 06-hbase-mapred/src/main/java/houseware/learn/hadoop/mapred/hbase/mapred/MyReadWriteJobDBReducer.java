package houseware.learn.hadoop.mapred.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Connection;

public class MyReadWriteJobDBReducer {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "MyReadWriteJobDBReducer Program");
        job.setJarByClass(MyReadWriteJobDBReducer.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs
        String sourceTable = args[0];
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,      // input table
                scan,              // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper class
                null,              // mapper output key
                null,              // mapper output value
                job);
        job.setReducerClass(MyRdbmsReducer.class);
        job.setCombinerClass(MyRdbmsReducer.class);
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

    public static class MyRdbmsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Connection c = null;

        public void setup(Context context) {
            // create DB connection...
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // do summarization
            // in this example the keys are Text, but this is just an example
        }

        public void cleanup(Context context) {
            // close db connection
        }

    }

}
