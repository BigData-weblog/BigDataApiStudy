package hbase.mapreducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class ReaderHdfsToHbase {

    static class Mymapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        Text k=new Text();
        IntWritable v=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            for(String i:split){
                k.set(i);
                v.set(1);
                context.write(k,v);
            }
        }
    }

    /**
     * 1:map端输出的key的类型
     * 2：map端输出的value类型
     * 3：reduce端输出的key的类型
     * reducer端输出的value的默认Mutation
     */
    static class MyReducer extends TableReducer<Text,IntWritable, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }

            Put p=new Put(key.getBytes());
            p.add("cf1".getBytes(),"name".getBytes(), Bytes.toBytes(sum));

            context.write(NullWritable.get(),p);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","hadoop");
        conf.set("fs.defaultFS","hdfs://172.16.98.186:9000");
        conf.set("hbase.zookeeper.quorum","172.16.98.186:2181,172.16.98.185:2181,172.16.98.184:2181");
        Job job=Job.getInstance(conf);

        job.setJarByClass(ReaderHdfsToHbase.class);
        job.setMapperClass(Mymapper.class);
        TableMapReduceUtil.initTableReducerJob("barry:t1",MyReducer.class,job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Mutation.class);

        FileInputFormat.addInputPath(job,new Path("/barry-test/in/in01/word01"));

        job.waitForCompletion(true);
    }
}
