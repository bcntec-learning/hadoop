package houseware.learn.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author fphilip@houseware.es
 */
public class WordCounterMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static List<String> discardWords = new ArrayList<>();
    static {
        discardWords.add("");
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().toUpperCase().replaceAll("[^a-zA-Z 0-9]+","").replaceAll("\\s+", " ");

        final List<String> words = extractWordsFromLine(line);

        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }

    }

    private List<String>  extractWordsFromLine(String line) {

        if (line != null && !"".equals(line)) {
            return getWords(line);
        }
        return Collections.emptyList();
    }

    private List<String>  getWords(String line) {

        final String[] words = line.split(" ");

        List<String>  finalWords = new ArrayList<>(words.length);
        for (String word : words) {
            if (!"".equals(word) && !discardWords.contains(word)) {
                finalWords.add(word);
            }
        }
        return finalWords;
    }
}