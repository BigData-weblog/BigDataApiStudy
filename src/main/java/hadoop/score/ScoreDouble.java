package hadoop.score;

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

/**
 * 分组
 * 当即有分组又有排序的时候，排序在前分组在后
 * 排序字段一定要包含分组字段，实际上的分组，仅仅是将map输出的结果，相邻的进行比较，仅仅会比较前一条数据和后一条数据
 *
 * 如果相同返回为1组，如果不相同，则重新划分组
 * 如果我们想要分组一定要能保证分组字段的数据在相邻的位置
 */
public class ScoreDouble {

    static class MyMapper extends Mapper<LongWritable, Text,ScoreBean,Text>{

        ScoreBean sb=new ScoreBean();
        Text v=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            String course=datas[0];
            String name=datas[1];

            int sum=0;
            int count=0;

            for(int i=2;i<datas.length;i++){
                count++;
                sum+=Integer.parseInt(datas[i]);
            }

            double avg=(double)sum/count;
            sb.setCourse(course);
            sb.setScore(avg);
            v.set(name);
            context.write(sb,v);
        }
    }

    static class MyReducer extends Reducer<ScoreBean,Text,Text, NullWritable>{

        Text k=new Text();
        @Override
        protected void reduce(ScoreBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //取第一个学生信息
            Text name = values.iterator().next();
            k.set(key.getCourse()+"\t"+name.toString()+"\t"+key.getScore());
            context.write(k,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        //加载配置文件
        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","hadoop");

        //启动一个Job 封装计算程序的mapper和reducer 输入和输出
        Job job=Job.getInstance(conf);

        //设置的是计算程序的主驱动类 运行的时候打成jar包运行
        job.setJarByClass(ScoreDouble.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //指定分组组件
        job.setGroupingComparatorClass(MyGroup.class);
        //设置mapper的输出类型
        job.setMapOutputKeyClass(ScoreBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/in/in03/course"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("hdfs://172.16.98.186:9000/barry-test/out/course04"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);

    }
}
