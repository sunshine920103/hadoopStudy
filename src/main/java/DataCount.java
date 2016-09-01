import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/21.
 */
public class DataCount {

    public static class DataMap extends Mapper<LongWritable,Text,Text,DataBean>{

        Text k=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values=value.toString().split("\t");
            k.set(values[1]);
            context.write(k,new DataBean("",Long.parseLong(values[8]),Long.parseLong(values[9])));
        }

    }


    public static class DataPartitioner extends Partitioner<Text,DataBean>{

        private static Map<String,Integer> accountMap=new HashMap<String, Integer>();
        static{
            accountMap.put("135",1);
            accountMap.put("136",1);
            accountMap.put("137",1);
            accountMap.put("138",1);
            accountMap.put("150",2);
            accountMap.put("159",2);
            accountMap.put("183",3);
            accountMap.put("182",3);
        }
        @Override
        public int getPartition(Text text, DataBean dataBean, int i) {
            String account=text.toString();
            Integer num=accountMap.get(account.substring(0,3));
            if (null==num)
            {
                num=0;
            }
            return num;
        }
    }

    public static class DataReduce extends Reducer<Text,DataBean,Text,DataBean>
    {
        @Override
        protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
            long upload=0;
            long download=0;
            for(DataBean dataBean:values)
            {
                upload+=dataBean.getUpload();
                download+=dataBean.getDownload();
            }

            context.write(key,new DataBean("",upload,download));
        }
    }


    public static void main (String[] args) throws Exception
    {
        Job job=Job.getInstance();
        job.setJarByClass(DataCount.class);

        job.setMapperClass(DataMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);

        job.setPartitionerClass(DataPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.setReducerClass(DataReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
