package houseware.learn.hadoop.mapred.blog.mapreduce.donation.secondarysort;

import houseware.learn.hadoop.mapred.blog.data.writable.DonationWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderByCompositeKey {

    public static final Log LOG = LogFactory.getLog(OrderByCompositeKey.class);

    public static class CompositeKeyCreationMapper extends Mapper<Object, DonationWritable, CompositeKey, DonationWritable> {

        private CompositeKey compositeKey = new CompositeKey();

        @Override
        public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {

            // Ignore entries with empty values for better readability of results
            if (StringUtils.isEmpty(donation.donor_state) || StringUtils.isEmpty(donation.donor_city)) {
                return;
            }

            compositeKey.set(donation.donor_state, donation.donor_city, donation.total);
            context.write(compositeKey, donation);

        }

    }


    public static class ValueOutputReducer extends Reducer<CompositeKey, DonationWritable, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void reduce(CompositeKey key, Iterable<DonationWritable> donations, Context context) throws IOException, InterruptedException {

            for (DonationWritable donation : donations) {
                outputKey.set(donation.donation_id);
                outputValue.set(String.format("%s %s %.2f", donation.donor_state, donation.donor_city, donation.total));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Secondary Sorting");
        job.setJarByClass(OrderByCompositeKey.class);

        // Mapper configuration
        job.setMapperClass(CompositeKeyCreationMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(DonationWritable.class);

        // Partitioning/Sorting/Grouping configuration
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setSortComparatorClass(FullKeyComparator.class);
        job.setGroupingComparatorClass(NaturalKeyComparator.class);

        // Reducer configuration
        job.setReducerClass(ValueOutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
