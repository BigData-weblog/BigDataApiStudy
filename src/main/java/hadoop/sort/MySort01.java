package hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MySort01 {

    static class MyMapper extends Mapper<LongWritable,Text, IntWritable,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            String word=datas[0];
            int count=Integer.parseInt(datas[1]);
            context.write(new IntWritable(count),new Text(word));
        }
    }

    /**
     * 将map输入的结果反转，输出最终结果
     */
    static class MyReducer extends Reducer<IntWritable,Text,Text,IntWritable>{

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t:values){
                context.write(t,key);
            }
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
        job.setJarByClass(MySort01.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reducer的输出类型 写代码的时候已经制定了 ,这里为什么要指定
        //jdk的泛型jdk 1.5开始出现的  泛型是指在代码编译时候进行类型检查
        //在代码运行的时候泛型会被自动擦除，所以我们这里需要指定
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("/barry-test/out/out01"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/sort01"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);


    }
}
