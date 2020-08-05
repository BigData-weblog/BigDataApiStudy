package hadoop.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * reducer join 缺陷
 * 1。reducetask 的并行度问题  0.95*datanode 节点的个数，并行度不高，性能不高
 * 2。reducertask容易产生数据倾斜
 *
 */
public class ReduceJoin {
    static class MyMapper extends Mapper<LongWritable,Text,Text, Text>{

        String filename="";
        //context 上下文对象
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //获取文件名
            //获取文件切片相关的信息，一个切片对应一个maptask
            InputSplit inputSplit = context.getInputSplit();
            //转换为文件切片
            FileSplit fs=(FileSplit)inputSplit;
            //获取文件名
            filename=fs.getPath().getName();
        }

        /**
         * 由于map中需要知道数据来源，所以最好在进入map函数之前可以获取文件的名字，这个事情给setup做
         * map端做的事情：发送数据的时候需要打标志
         * 读取两个表中的数据 进行切分发送
         * key：公共字段 关联字段pid
         * value：剩下的 需要有标志，标记数据的来源
         * reduce端：
         * 接收过来，判断是来自于哪个表的数据进行拼接
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        Text k=new Text();
        Text v=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //解析出来每一行内容，打标记发送
            String[] infos = value.toString().split("\t");
            if(filename.equals("t_order")){
                k.set(infos[2]);
                v.set("OR"+infos[0]+"\t"+infos[1]+"\t"+infos[3]);
                context.write(k,v);
            }else{
                k.set(infos[0]);
                v.set("PR"+infos[1]+"\t"+infos[2]+"\t"+infos[3]);
                context.write(k,v);
            }
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text, NullWritable>{
        Text k=new Text();
        // 按pid进行分组的
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            /**
             * 关联关系 一对多，一个商品对应多条订单
             * 接收到的数据，一的表的数据只有一个，多的表的数据，可能有多个
             * 将多的表数据拿过来和一的表中的数据分别进行拼接
             */
            //两个数据需要封装到两个容器中
            List<String> orderList=new ArrayList<String>();
            List<String> proList=new ArrayList<String>();
            for (Text v:values){
                String s = v.toString();
                if(s.startsWith("OR")){
                    orderList.add(s.substring(2));
                }else{
                    proList.add(s.substring(2));
                }
            }
            //拼接的时候 什么时候才可以进行拼接
            if(orderList.size()>0 && proList.size()>0){
                //循环遍历多的 拼接1的
                for(String ol:orderList){
                    String res=key.toString()+"\t"+ol+"\t"+proList.get(0);
                    k.set(res);
                    context.write(k,NullWritable.get());
                }
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
        job.setJarByClass(ReduceJoin.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reducer的输出类型 写代码的时候已经制定了 ,这里为什么要指定
        //jdk的泛型jdk 1.5开始出现的  泛型是指在代码编译时候进行类型检查
        //在代码运行的时候泛型会被自动擦除，所以我们这里需要指定
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("/barry-test/in/in05"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/reduce_join_01"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);


    }
}
