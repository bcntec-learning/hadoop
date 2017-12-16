package houseware.learn.hadoop.mapred.partitioner1;

import java.io.IOException;
import java.util.Arrays;

import houseware.learn.hadoop.mapred.wordcount.WordCounter;
import houseware.learn.hadoop.mapred.wordcount.WordCounterMap;
import houseware.learn.hadoop.mapred.wordcount.WorldCounterReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author fphilip@houseware.es
 */

@SuppressWarnings("unused")
public class WordCounterWithPartitioner {


    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Use: WordCounterWithPartitioner <SOURCE_PATH> <TARGET_PATH>");
            System.exit(1);
        }

        System.out.println("Start word counter program with these parameters: "
                + Arrays.toString(args));

        String source = args[0];
        String target = args[1];

        boolean success = false;

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "My WordCounter Program");
            job.setJarByClass(WordCounter.class);

            FileInputFormat.addInputPath(job, new Path(source));
            FileOutputFormat.setOutputPath(job, new Path(target));

            job.setMapperClass(WordCounterMap.class);
            job.setReducerClass(WorldCounterReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setPartitionerClass(WorldCounterPartitioner.class);
            job.setNumReduceTasks(27);

            success = job.waitForCompletion(true);
        } catch (Exception e) {
            System.err.println("Error counting words.");
            e.printStackTrace(System.err);
        }


        System.exit(success ? 0 : 1);

    }
}