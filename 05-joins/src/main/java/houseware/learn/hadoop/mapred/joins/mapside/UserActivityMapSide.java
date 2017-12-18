package houseware.learn.hadoop.mapred.joins.mapside;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class UserActivityMapSide {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Use: UserActivityMapSide <SOURCE_PATH> <TARGET_PATH>");
            System.exit(1);
        }

        System.out.println("Start word counter program with these parameters: "
                + Arrays.toString(args));

        String source = args[0];
        String target = args[1];

        boolean success = false;
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "My UserActivityMapSide Program");
            job.setJarByClass(UserActivityMapSide.class);

            // input path
            FileInputFormat.addInputPath(job, new Path(args[0]));

            // output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(UserActivityMapper.class);
            job.setReducerClass(UserActivityReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(UserActivityVO.class);

            job.addCacheFile(new URI("hdfs://localhost:9000/input/user.log"));
            job.setOutputKeyClass(UserActivityVO.class);
            job.setOutputValueClass(NullWritable.class);

            success = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }


        System.exit(success ? 0 : 1);

    }
    public class UserActivityReducer extends Reducer<IntWritable, UserActivityVO, UserActivityVO, NullWritable> {

        NullWritable value = NullWritable.get();

        @Override
        protected void reduce(IntWritable key, Iterable<UserActivityVO> values,
                              Reducer<IntWritable, UserActivityVO, UserActivityVO, NullWritable>.Context context)
                throws IOException, InterruptedException {
            for (UserActivityVO userActivityVO : values) {
                context.write(userActivityVO, value);
            }
        }
    }
}
