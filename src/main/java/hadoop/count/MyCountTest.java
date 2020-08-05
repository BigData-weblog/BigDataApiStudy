package hadoop.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 在生产中，全局计数器（自定义）一般用来统计不规则的数据的数据量
 */
public class MyCountTest {

    //统计总记录条数和总字段数
    static class MyMapper extends Mapper<LongWritable, Text, NullWritable,NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取计数器
            Counter lines_counter = context.getCounter(MyCounter.LINES);

            //对计数器进行操作，进行总行数统计 increment(1L)
            lines_counter.increment(1L);

            //获取下一个计数 总计总的字段数
            Counter counts = context.getCounter(MyCounter.COUNT);
            String[] datas = value.toString().split("\t");
            counts.increment(datas.length);
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
        job.setJarByClass(MyCountTest.class);

        //没有设置reduce的个数默认1个，如果没有reducetask的时候一定要加这句话
        job.setNumReduceTasks(0);
        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("/barry-test/out/out09/part-r-00000"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/count02"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);

    }
}
