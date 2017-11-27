import java.io.IOException;
import java.net.URI;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class HdfsWrite {
    public static void main(String[] args) throws IOException {
        String fromUri = args[0];
        String toUri = args[1];
        int bytesRead;
        byte[] buffer = new byte[256];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(toUri), conf);

        FSDataOutputStream out = null;
        FSDataInputStream in = null;             //If source file is in HDFS
        // if it is from Local FS.
        //InputStream in2 = new BufferedInputStream(new FileInputStream(fromUri));
        try {
            out = fs.create(new Path(toUri));
            in = fs.open(new Path(fromUri));
            // we can copy bytes from input stream to output stream as shown below
             IOUtils.copyBytes(in, out, 4096, false)
            // Read from input stream and write to output stream until EOF.
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            in.close();
            out.close();
        }
    }
}