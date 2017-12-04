package houseware.learn.hadoop.mapred.counters;

import houseware.learn.hadoop.mapred.wordcount.WordCounter;
import houseware.learn.hadoop.mapred.wordcount.WordCounterMap;
import houseware.learn.hadoop.mapred.wordcount.WorldCounterReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author fphilip@houseware.es
 */

@SuppressWarnings("unused")
public class WordCounterWithMyCounters {


    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length != 2) {
            System.err.println("Use: WordCounterWithMyCounters <SOURCE_PATH> <TARGET_PATH>");
            System.exit(1);
        }

        System.out.println("Start word counter program with these parameters: "
                + Arrays.toString(args));

        String source = args[0];
        String target = args[1];

        boolean success = false;

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "My WordCounterWithMyCounters Program");
            job.setJarByClass(WordCounter.class);

            FileInputFormat.addInputPath(job, new Path(source));
            FileOutputFormat.setOutputPath(job, new Path(target));

            job.setMapperClass(WordCounterMapCounted.class);
            job.setReducerClass(WorldCounterReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            success = job.waitForCompletion(true);

            Counters counters = job.getCounters();
            //JobCounter.TOTAL_LAUNCHED_MAPS
            Counter cU = counters.findCounter(MY_COUNTERS.UPPERCASE);
            System.out.println(cU.getDisplayName() + ":" + cU.getValue());
            Counter cL = counters.findCounter(MY_COUNTERS.LOWERCASE);
            System.out.println(cL.getDisplayName() + ":" + cL.getValue());


        } catch (Exception e) {
            System.err.println("Error counting words.");
            e.printStackTrace(System.err);
        }

        System.exit(success ? 0 : 1);

    }

    static class WordCounterMapCounted extends WordCounterMap {

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {

            String line = value.toString().replaceAll("[^a-zA-Z 0-9]+", "").replaceAll("\\s+", " ");

            final List<String> words = extractWordsFromLine(line);

            for (String word : words) {
                String w = word.toUpperCase();
                context.getCounter((w.charAt(0) == word.charAt(0)) ? MY_COUNTERS.UPPERCASE : MY_COUNTERS.LOWERCASE).increment(1);
                context.write(new Text(w), new LongWritable(1));
            }

        }

    }
}