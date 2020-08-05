package hbase.mapreducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * hbase====>hdfs
 */
public class ReadHbaseToHdfs {

    static class MyMapper extends TableMapper<Text, IntWritable>{

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String word = Bytes.toString(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
            context.write(new Text(word),new IntWritable(1));

        }
    }

    static class  MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(IntWritable i:values){
                count=count+i.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /*if(args.length!=2){
            System.err.println("Usage:MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Job job=Job.getInstance();
        Configuration conf=job.getConfiguration();
        FileSystem fs=FileSystem.get(conf);
        fs.delete(new Path(args[1]),true);*/

        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","hadoop");
        conf.set("fs.defaultFS","hdfs://172.16.98.186:9000");
        conf.set("hbase.zookeeper.quorum","172.16.98.186:2181,172.16.98.185:2181,172.16.98.184:2181");
        Job job=Job.getInstance(conf);
        //Configuration conf=job.getConfiguration();
        //FileSystem fs=FileSystem.get(conf);

        job.setJarByClass(ReadHbaseToHdfs.class);
        job.setJobName("word count");


        Scan scan=new Scan();
        /**
         * 1:表名，相当于输入
         * 2：scan对象
         * 3：mpper类
         * 4：mapper输出的key的类型
         * 5：mapper输出的value的类型
         * 6：job
         */
        TableMapReduceUtil.initTableMapperJob("barry:t1",
                scan,MyMapper.class,Text.class,IntWritable.class,job);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.getConfiguration().set(TableInputFormat.INPUT_TABLE,"barry:t1");
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/hbase01/"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);

        //System.exit(job.waitForCompletion(true)?0:1);
    }
}
