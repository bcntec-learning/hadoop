package houseware.learn.hadoop.mapred.blog.data.seqfile;

import data.writable.ProjectWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class ProjectsReader {

    public static void main(final String[] args) throws Exception {

        Path inputFile = new Path(args[0]);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(inputFile))) {
            System.out.println("Compressed ? " + reader.isBlockCompressed());

            Text key = new Text();
            ProjectWritable value = new ProjectWritable();

            while (reader.next(key, value)) {
                System.out.println(value.toString());
            }

        }
    }

}
