package hadoop.partition;

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

public class WordCount {
    /**
     * Mapper类中的四个泛型
     * 你的输入就是需要统计的文件，这个文件应该以那种方式给你，最好的方式是一行给你一次
     * 输入：一行的内容
     * KEYIN： 输入的key的泛型，这里指的是每一行的偏移量，mapreduce底层文件输入依赖流的方式
     * 字节流方式 记录是每一行的起始偏移量，一般情况下没啥用
     * VALURIN：输入的值的类型，这里指的是一行的内容
     *
     * KEYOUT：输出的key类型
     * VALUEOUT：输出的value类型
     *
     * 当数据需要持久化到磁盘或者进行网路传输的时候，必须进行序列化和反序列化
     * 序列化：原始数据----二进制
     * 反序列化：二进制----原始数据
     *
     * mapreduce处理数据的时候必然经过持久化磁盘或者网络传输，那么数据必须序列化，反序列化
     * java中序列化的接口serializable，连同类结构一起进行序列化和反系列化
     * java中的序列化和反序列化过于累赘，所以hadoop弃用java中的这一套序列化，反序列化的东西
     * hadoop为我们提供了一套自己的序列化和反序列化的接口 Writeable  优点：轻便
     * 对应的8个基本数据类型和String类都帮我们实现好了 Writable接口
     * int-----IntWritable
     * long----LongWritable
     * double----DoubleWritable
     * String----Text
     * null----NullWritable
     */
    static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

        /*
        这个方法调用频率：一行调用一次
        LongWritable key:每一行的启始偏移量
        Text value：每一行的内容 每次读取到的哪一行的内容
        Context context：上下文对象 用于传输的
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取到每一行的内容，进行每个单词的切分，将每个单词加标签1
            String line=value.toString();
            //对每一行内容切分
            String[] words = line.split("\t");
            //循环遍历打标志
            for(String w:words){
                context.write(new Text(w),new IntWritable(1));
            }
        }
    }

    /**
     * reduce处理的是map的结果 reduce的输入是map的输出
     * KEYIN：reducer输入key的类型----Mapper输出的key类型 Text
     * VALUE：reducer输入的value的类型----Mapper输出的value的类型 IntWritable
     * 输出应该是reduce最终处理完的业务逻辑的输出
     *
     * KEYOUT:reducer统计结果的key的类型  这里指的是最终统计完成的单词 Text
     * VALUEOUT:reducer统计结果的value的类型，这里指的是单词出现的总次数 IntWritable
     */
    static class WordCountReducer extends Reducer <Text,IntWritable,Text,IntWritable>{

        /*
        reduce端想要对这种结果进行统计，最好相同的单词在一起，事实上确实是在一起
        map端输出的数据到达reduce端之前就会对数据进行一个整理，这个整理是框架帮你做的，这个整理是分组
        框架内部会进行一个分组，安装map输出的key进行分组，key想要的为一组，map端输出的数据中有多少个不同的key就有多少个组

        Text key 每一组中的那个形态的key
        Iterable<IntWritable> values，每一组中相同的key对应的所有value的值
        Context context 上下文对象，用于传输 写出到hdfs

        这个方法的调用频率
        每一组调用一次
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置文件
        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","hadoop");

        //启动一个Job 封装计算程序的mapper和reducer 输入和输出
        Job job=Job.getInstance(conf);

        //设置的是计算程序的主驱动类 运行的时候打成jar包运行
        job.setJarByClass(WordCount.class);

        //设置mapper和reducer的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducetask个数，不设置，默认是1个
        job.setNumReduceTasks(3);
        //设置自定义分区类
        job.setPartitionerClass(MyPartition.class);
        //设置reducer的输出类型 写代码的时候已经制定了 ,这里为什么要指定
        //jdk的泛型jdk 1.5开始出现的  泛型是指在代码编译时候进行类型检查
        //在代码运行的时候泛型会被自动擦除，所以我们这里需要指定
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/in/in01/word01"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/out/out09"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);
    }
}
