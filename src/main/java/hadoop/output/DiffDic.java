package hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DiffDic {

    static class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable>{

        Text mapkey=new Text();
        IntWritable mapvalue=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            String k=datas[0]+"\t"+datas[1];
            mapkey.set(k);
            for(int i=2;i<datas.length;i++){
                mapvalue.set(Integer.parseInt(datas[i]));
                context.write(mapkey,mapvalue);
            }
        }
    }

    static class MyReducer extends Reducer<Text,IntWritable,Text, DoubleWritable>{

        DoubleWritable value=new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            int count=0;
            for(IntWritable v:values){
                sum+=v.get();
                count++;
            }
            double avg=(double)sum/count;
            value.set(avg);
            context.write(key,value);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置文件
        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","hadoop");
        conf.set("fs.defaultFS","hdfs://172.16.98.186:9000");
        //启动一个Job 封装计算程序的mapper和reducer 输入和输出
        Job job=Job.getInstance(conf);

        //设置的是计算程序的主驱动类 运行的时候打成jar包运行
        job.setJarByClass(DiffDic.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //指定自定义输出
        job.setOutputFormatClass(DiffDicOutputFormat.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/in/in03/course"));

        //目的：存放运行的标志文件
        FileOutputFormat.setOutputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/out/output_score_02"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);
    }
}
