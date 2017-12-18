package houseware.learn.hadoop.mapred.mrtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author fphilip@houseware.es
 */

@SuppressWarnings("unused")
public class WordCounterToTestSample {


    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Use: WordCounterToTest <SOURCE_PATH> <TARGET_PATH>");
            System.exit(1);
        }

        System.out.println("Start word counter program with these parameters: "
                + Arrays.toString(args));

        String source = args[0];
        String target = args[1];

        boolean success = false;

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "My WordCounterToTest Program");
            job.setJarByClass(WordCounterToTestSample.class);

            FileInputFormat.addInputPath(job, new Path(source));
            FileOutputFormat.setOutputPath(job, new Path(target));

            job.setMapperClass(WordCounterMap.class);
            job.setReducerClass(WordCounterReduce.class);
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

    public static class WordCounterMap extends Mapper<LongWritable, Text, Text, LongWritable> {


        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            String line = value.toString().toUpperCase().replaceAll("[^a-zA-Z 0-9]+", "").replaceAll("\\s+", " ");


            if (line != null && !"".equals(line)) {
                final String[] words = line.split(" ");

                List<String> finalWords = new ArrayList<>(words.length);
                for (String word : words) {
                    if (!"".equals(word)) {
                        finalWords.add(word);
                    }
                }

                for (String word : finalWords) {

                    String w = word.toUpperCase();
                    context.getCounter((w.charAt(0) == word.charAt(0)) ? MY_COUNTERS.UPPERCASE : MY_COUNTERS.LOWERCASE).increment(1);

                    context.write(new Text(word), new LongWritable(1));
                }

            }

        }


    }

    public static class WordCounterReduce extends Reducer<Text, LongWritable, Text, LongWritable> {


        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            long wordOccurrences = 0;

            for (LongWritable value : values) {
                wordOccurrences++;
            }

            context.write(key, new LongWritable(wordOccurrences));

        }
    }
}