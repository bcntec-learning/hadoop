import java.net.URI;

/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class HdfsRead {
    public static void main(String[] args) throws IOException {

        String uri = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;

        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}