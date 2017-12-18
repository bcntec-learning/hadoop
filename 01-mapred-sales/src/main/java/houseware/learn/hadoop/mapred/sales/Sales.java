package houseware.learn.hadoop.mapred.sales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class Sales {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Use: SalesCountry <SOURCE_PATH> <TARGET_PATH>");
            System.exit(1);
        }

        System.out.println("Start word counter program with these parameters: "
                + Arrays.toString(args));

        String source = args[0];
        String target = args[1];

        boolean success = false;

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "My Sales Program");
            job.setJarByClass(Sales.class);

            FileInputFormat.addInputPath(job, new Path(source));
            FileOutputFormat.setOutputPath(job, new Path(target));

            job.setMapperClass(SalesMap.class);
            job.setReducerClass(SalesReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            success = job.waitForCompletion(true);
        } catch (Exception e) {
            System.err.println("Error counting words.");
            e.printStackTrace(System.err);
        }


        System.exit(success ? 0 : 1);

    }


}