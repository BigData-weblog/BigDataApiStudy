package hadoop.mergefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MergeFile {

    // 参数1：自定义输入的key
    static class MyMapper extends Mapper<Text, NullWritable,Text,NullWritable>{
        /**
         *
         * @param key  整个文件的内容
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{

        //每组调用一次
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
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
        job.setJarByClass(MergeFile.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指定自定义输入
        job.setInputFormatClass(MergeFileInputFormat.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/in/in01"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/out/merge_file_02"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);
    }
}
