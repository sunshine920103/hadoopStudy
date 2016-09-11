import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/21.
 */
public class WordCount {

    private static final Logger LOGGER= LoggerFactory.getLogger(WordCount.class);

    static class HbaseMapper extends TableMapper<ImmutableBytesWritable,Put>{

        @Override
        protected void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException {

            byte[] b = row.get();
            String keys = Bytes.toString(b);
            byte[] value=result.getValue(Bytes.toBytes(Constants.COLUMN_FAMILY),Bytes.toBytes("f1"));

            LOGGER.info("------keys---::::"+keys);
            LOGGER.info("------values---::::"+Bytes.toString(value));

            context.write(row, resultToPut(row,result));

        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (KeyValue kv : result.raw()) {
                put.add(kv);
            }
            return put;
        }
    }





    public static void main(String[] args) throws Exception
    {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config,"mytest");
        job.setJarByClass(WordCount.class);     // class that contains mapper

        Scan scan = new Scan();

        //rpc请求的记录数
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                Constants.TableName.TEST,        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                HbaseMapper.class,   // mapper
                null,             // mapper output key
                null,             // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                Constants.TableName.RECEIVE,      // output table
                null,             // reducer class
                job);
        job.setNumReduceTasks(0);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
