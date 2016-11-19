import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by zhufeng on 2016/11/19.
 */
public class hdfs2Hive {
    public static class hdfs2HiveMap extends Mapper<LongWritable,Text,NullWritable,Text>
    {
        private MultipleOutputs<NullWritable,Text> multipleOutputs;
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
           multipleOutputs.close();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs=new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String vals_array=value.toString().split(" ")[0];
            String dateHour=vals_array.substring(0,10);
            String basePath=String.format("%s/part",dateHour);

            multipleOutputs.write(NullWritable.get(), value,basePath);
        }
    }



    public static void main(String[] args) throws Exception {
        Job job=Job.getInstance();
        job.setJarByClass(hdfs2Hive.class);
        job.setMapperClass(hdfs2HiveMap.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path("/test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/cctest4/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }






}
