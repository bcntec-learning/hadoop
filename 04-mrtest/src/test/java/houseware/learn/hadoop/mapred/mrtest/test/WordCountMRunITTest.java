package houseware.learn.hadoop.mapred.mrtest.test;

import houseware.learn.hadoop.mapred.mrtest.WordCounterToTestSample;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static houseware.learn.hadoop.mapred.mrtest.MY_COUNTERS.UPPERCASE;
import static org.junit.Assert.assertEquals;

public class WordCountMRunITTest {
    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCounterToTestSample.WordCounterMap mapper = new WordCounterToTestSample.WordCounterMap();
        WordCounterToTestSample.WordCounterReduce reducer = new WordCounterToTestSample.WordCounterReduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void test_Mapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("hadoop is bigdata"));
        mapDriver.withInput(new LongWritable(), new Text("hadoop is emerging"));
        mapDriver.withOutput(new Text("hadoop"), new LongWritable(1));
        mapDriver.withOutput(new Text("is"), new LongWritable(1));
        mapDriver.withOutput(new Text("bigdata"), new LongWritable(1));
        mapDriver.withOutput(new Text("hadoop"), new LongWritable(1));
        mapDriver.withOutput(new Text("is"), new LongWritable(1));
        mapDriver.withOutput(new Text("emerging"), new LongWritable(2));
        mapDriver.runTest();
    }

    @Test
    public void test_Reducer() throws IOException {
        List<LongWritable> values1 = new ArrayList<>();
        values1.add(new LongWritable(1));
        reduceDriver.withInput(new Text("bigdata"), values1);

        List<LongWritable> values3 = new ArrayList<>();
        values3.add(new LongWritable(1));
        reduceDriver.withInput(new Text("emerging"), values3);

        List<LongWritable> values = new ArrayList<>();
        values.add(new LongWritable(1));
        values.add(new LongWritable(1));
        reduceDriver.withInput(new Text("hadoop"), values);

        List<LongWritable> values2 = new ArrayList<>();
        values2.add(new LongWritable(1));
        values2.add(new LongWritable(1));
        reduceDriver.withInput(new Text("is"), values2);

        reduceDriver.withOutput(new Text("bigdata"), new LongWritable(1));
        reduceDriver.withOutput(new Text("emerging"), new LongWritable(1));
        reduceDriver.withOutput(new Text("hadoop"), new LongWritable(2));
        reduceDriver.withOutput(new Text("is"), new LongWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void test_MapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "hadoop is bigdata"));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "hadoop is emerging"));

        mapReduceDriver.withOutput(new Text("bigdata"), new LongWritable(1));
        mapReduceDriver.withOutput(new Text("emerging"), new LongWritable(1));
        mapReduceDriver.withOutput(new Text("hadoop"), new LongWritable(2));
        mapReduceDriver.withOutput(new Text("is"), new LongWritable(2));

        mapReduceDriver.runTest();
    }

    @Test
    public void test_Count() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "hadoop is Bigdata"));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "Hadoop is emerging"));
        mapDriver.runTest();
        assertEquals("Uppercase?", 2, mapDriver.getCounters()
                .findCounter(UPPERCASE).getValue());

        mapReduceDriver.runTest();
    }

}