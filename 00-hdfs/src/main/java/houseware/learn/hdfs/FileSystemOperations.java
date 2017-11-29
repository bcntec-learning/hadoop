package houseware.learn.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.security.PrivilegedExceptionAction;


/**
 * @author fphilip@houseware.es
 */
@SuppressWarnings("unused")
public class FileSystemOperations {

    static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    static final String HADOOP_USER = "hadoop";

    public FileSystemOperations() {
//        System.setProperty(HADOOP_USER_NAME,HADOOP_USER);
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length < 1) {
            System.out.println("Usage: add/read/delete/mkdir"
                    + " [<local_path> <hdfs_path>]");
            System.exit(1);
        }
        UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser(HADOOP_USER);

        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {

            FileSystemOperations client = new FileSystemOperations();
            String hdfsPath = "hdfs://localhost:8020";

            Configuration conf = new Configuration();

            conf.set(HADOOP_USER_NAME, "hadoop");
            conf.set("fs.default.name", hdfsPath);

            if (args[0].equals("add")) {
                if (args.length < 3) {
                    System.out.println("Usage: add <local_path> "
                            + "<hdfs_path>");
                    System.exit(1);
                }

                client.addFile(args[1], args[2], conf);

            } else if (args[0].equals("read")) {
                if (args.length < 2) {
                    System.out.println("Usage: read <hdfs_path>");
                    System.exit(1);
                }

                client.readFile(args[1], conf);

            } else if (args[0].equals("delete")) {
                if (args.length < 2) {
                    System.out.println("Usage: delete <hdfs_path>");
                    System.exit(1);
                }

                client.deleteFile(args[1], conf);

            } else if (args[0].equals("mkdir")) {
                if (args.length < 2) {
                    System.out.println("Usage: mkdir <hdfs_path>");
                    System.exit(1);
                }

                client.mkdir(args[1], conf);

            } else {
                System.out.println("Usage: add/read/delete/mkdir"
                        + " [<local_path> <hdfs_path>]");
                System.exit(1);
            }

            System.out.println("Done!");

            return null;

        });
    }

    /**
     * create a existing file from local filesystem to hdfs
     *
     * @param source
     * @param dest
     * @param conf
     * @throws IOException
     */
    public void addFile(String source, String dest, Configuration conf) throws IOException {


        try (FileSystem fileSystem = FileSystem.get(conf)) {

            // Get the filename out of the file path
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

            // Create the destination path including the filename.
            if (dest.charAt(dest.length() - 1) != '/') {
                dest = dest + "/" + filename;
            } else {
                dest = dest + filename;
            }

            // System.out.println("Adding file to " + destination);

            // Check if the file already exists
            Path path = new Path(dest);
            if (fileSystem.exists(path)) {
                System.out.println("File " + dest + " already exists");
                return;
            }

            try (FSDataOutputStream out = fileSystem.create(path)) {

                try (InputStream in = new BufferedInputStream(new FileInputStream(new File(source)))) {

                    byte[] b = new byte[1024];
                    int numBytes = 0;
                    while ((numBytes = in.read(b)) > 0) {
                        out.write(b, 0, numBytes);
                    }
                }
            }
        }

        // Close all the file descriptors


    }

    /**
     * read a file from hdfs
     *
     * @param file
     * @param conf
     * @throws IOException
     */
    public void readFile(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        try (FSDataInputStream in = fileSystem.open(path)) {

            String filename = file.substring(file.lastIndexOf('/') + 1,
                    file.length());

            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(
                    new File(filename)))) {

                byte[] b = new byte[1024];
                int numBytes = 0;
                while ((numBytes = in.read(b)) > 0) {
                    out.write(b, 0, numBytes);
                }
            }
        }

    }

    /**
     * delete a directory in hdfs
     *
     * @param file
     * @throws IOException
     */
    public void deleteFile(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    /**
     * create directory in hdfs
     *
     * @param dir
     * @throws IOException
     */
    public void mkdir(String dir, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already not exists");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }
}