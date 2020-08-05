package hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyDefineSort {

    static class MyMapper extends Mapper<LongWritable, Text,FlowBean, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] infos = value.toString().split("\t");
            FlowBean fb=new FlowBean(infos[0],Integer.parseInt(infos[1].trim()),Integer.parseInt(infos[2].trim()),Integer.parseInt(infos[3].trim()));
            context.write(fb,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable>{

        /**
         *  到这里，分组是怎么分的，这里分组怎么比较的
         *  key 为自定义类型，在进行分组比较的时候，是按照地址进行比较的吗？
         *  如果按照地址进行分组，Iterable<NullWritable> values 封装的只有一个值
         *  证明分组的时候不是按照地址进行分组的
         *  那是按照什么呢？
         *  经过观察，按照我们自定义的CompareTo()方法进行分组的
         *  没有定义分组的时候，如果使用的是自带的类型，则调用的是自带类型的compareTo()，根据这个方法判断时候为一组
         *  自定义类型，根据自定义类中compareTo方法
         *
         */
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            for(NullWritable nl:values){
                context.write(key,NullWritable.get());
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
        job.setJarByClass(MyDefineSort.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reducer的输出类型 写代码的时候已经制定了 ,这里为什么要指定
        //jdk的泛型jdk 1.5开始出现的  泛型是指在代码编译时候进行类型检查
        //在代码运行的时候泛型会被自动擦除，所以我们这里需要指定
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("/barry-test/out/out09/part-r-00000"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/sort02"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);
    }
}
