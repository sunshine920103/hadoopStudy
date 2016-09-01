import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.BasicConfigurator;

import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by cc on 2016/7/20.
 */
public class HadoopStudy {
    private static String uri = "hdfs://192.168.19.130:9000";
    private static Configuration config = new Configuration();

    static {
        BasicConfigurator.configure();
        System.setProperty("hadoop.home.dir", "E:\\hadoop2.6bin\\hadoop-2.6.2");
    }

    public static void main(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(URI.create(uri), config);
        FSDataInputStream inputStream= fs.open(new Path("/c.log"));
        FileOutputStream outputStream=new FileOutputStream("e://cc.log");
        IOUtils.copyBytes(inputStream,outputStream,4096,true);
    }
}
