package ejercicios;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class Ejemplo1 {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length != 2) {
            System.err.println("Use: Ejemplo1 <SOURCE_PATH> <TARGET_PATH>");
            System.exit(1);
        }

        System.out.println("Start  program with these parameters: "
                + Arrays.toString(args));

        String source = args[0];
        String target = args[1];

        boolean success = false;

        try {
            Configuration conf = new Configuration();


            conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class.getName());
            Job job = Job.getInstance(conf, "My Sales Program");
            job.setJarByClass(Ejemplo1.class);

            FileInputFormat.addInputPath(job, new Path(source));
            FileOutputFormat.setOutputPath(job, new Path(target));

            job.setMapperClass(Ejemplo1.Map.class);
            job.setReducerClass(Ejemplo1.Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileOutputFormat.setCompressOutput(job, true);

            success =  job.waitForCompletion(true);
        } catch (Exception e) {
            System.err.println("Error counting words.");
            e.printStackTrace(System.err);
        }


        System.exit(success ? 0 : 1);

    }
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] data = valueString.split(",");
            if("EXITO".equals(data[0])) {
                context.write(new Text(data[2]), one);
            }
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int frequencyForIp = 0;
            Iterator<IntWritable> i = values.iterator();
            while (i.hasNext()) {
                IntWritable value = i.next();
                frequencyForIp += value.get();

            }
            context.write(key, new IntWritable(frequencyForIp));
        }
    }

}