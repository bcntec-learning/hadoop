package houseware.learn.hadoop.mapred.joins.reduceside;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceSideJoinSample2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 4) {
            System.err.println("Usage: join <input-table1><input-table2><jointype:inner|leftouter|rightouter|fullouter><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Reduce Side Join");
        job.setJarByClass(ReduceSideJoinSample2.class);

        job.setReducerClass(DeptJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeptJoinMapper.class);
        job.getConfiguration().set("join.type", args[2]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String attrs[] = value.toString().split(",");
            String deptId = attrs[2];
            // The foreign join key is the dept ID
            outkey.set(deptId);
            // flag this each record with prefixing it with 'A'
            outvalue.set("A" + value.toString());
            context.write(outkey, outvalue);
        }
    }

    public static class DeptJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String attrs[] = value.toString().split(",");
            String deptId = attrs[0];
            // The foreign join key is the dept ID
            outkey.set(deptId);
            // flag this each record with prefixing it with 'B'
            outvalue.set("B" + value.toString());
            context.write(outkey, outvalue);
        }
    }

    public static class DeptJoinReducer extends Reducer<Text, Text, Text, Text> {
        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private List<Text> listA = new ArrayList<>();
        private List<Text> listB = new ArrayList<>();
        private String joinType = null;

        public void setup(Context context) {
            // set up join configuration based on input
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear the lists
            listA.clear();
            listB.clear();
            // Put records from each table into correct lists, remove the prefix
            for (Text t : values) {
                tmp = t;
                if (tmp.charAt(0) == 'A') {
                    listA.add(new Text(tmp.toString().substring(1)));
                } else if (tmp.charAt(0) == 'B') {
                    listB.add(new Text(tmp.toString().substring(1)));
                }
            }
            // Execute joining logic based on its type
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {

                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {

                for (Text A : listA) {

                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        context.write(A, EMPTY_TEXT);
                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {

                for (Text B : listB) {

                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {

                        context.write(EMPTY_TEXT, B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {

                if (!listA.isEmpty()) {

                    for (Text A : listA) {

                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        } else {

                            context.write(A, EMPTY_TEXT);
                        }
                    }
                } else {

                    for (Text B : listB) {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            }
        }
    }


}