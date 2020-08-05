package hadoop.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 多job的串联
 */
public class CommonFriend {

    /**
     * 每当启动一个maptask任务，会执行一个程序，就会找到这个类
     */
    static class MyMapper_step01 extends Mapper<LongWritable, Text,Text,Text>{

        Text k=new Text();
        Text v=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] user_friends = value.toString().split(":");
            String[] friends = user_friends[1].split(",");
            for(String f:friends){
                k.set(f);
                v.set(user_friends[0]);
                context.write(k,v);
            }
        }
    }

    static class MyReducer_step01 extends Reducer<Text,Text,Text,Text>{

        Text t=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer();
            for(Text v:values){
                sb.append(v.toString()).append(",");
            }
            t.set(sb.substring(0,sb.length()-1));
            context.write(key,t);
        }
    }

    static class MyMapper_step02 extends Mapper<LongWritable,Text,Text,Text>{

        Text keyout=new Text();
        Text valueout=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] friend_user = value.toString().split("\t");
            String[] users = friend_user[1].split(",");
            for(String u1:users){
                for(String u2:users){

                    if(u1.compareTo(u2)<0){
                        String k=u1+"-"+u2;
                        keyout.set(k);
                        valueout.set(friend_user[0]);
                        context.write(keyout,valueout);
                    }
                }
            }
        }
    }

    static class MyReducer_step2 extends Reducer<Text,Text,Text,Text>{

        Text valueout=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer();
            for(Text v:values){
                sb.append(v).append(",");
            }
            valueout.set(sb.substring(0,sb.length()-1));
            context.write(key,valueout);

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
        job.setJarByClass(CommonFriend.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper_step01.class);
        job.setReducerClass(MyReducer_step01.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("/barry-test/in/in04/friends"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/friend_step_003"));


        //启动一个Job 封装计算程序的mapper和reducer 输入和输出
        Job job2=Job.getInstance(conf);

        //设置的是计算程序的主驱动类 运行的时候打成jar包运行
        job2.setJarByClass(CommonFriend.class);

        //设置mapper和reducer的类
        job2.setMapperClass(MyMapper_step02.class);
        job2.setReducerClass(MyReducer_step2.class);

        //设置mapper的输出类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job2,new Path("/barry-test/out/friend_step_003"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job2,new Path("/barry-test/out/friend_step_004"));


        //会将多个job当做一个组中的job提交 参数指的是组名，随意
        JobControl jc=new JobControl("common_frend");
        //将原声的job转换为可控制的job
        ControlledJob ajob=new ControlledJob(job.getConfiguration());
        ControlledJob bjob=new ControlledJob(job2.getConfiguration());

        //添加依赖关系
        bjob.addDependingJob(ajob);

        jc.addJob(ajob);
        jc.addJob(bjob);

        Thread t=new Thread(jc);
        t.start();

        while (!jc.allFinished()){
            t.sleep(500);
        }
        t.stop();

    }
}
