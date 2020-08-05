package hadoop.mapjoin;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**mapjoin 大小表
 * 因为有一个表需要加载到内存中，注定加载到内存中的表不能过大 256M
 *
 * 优点：并行度高，不存在数据倾斜的问题 运行效率高 优先选择map端join
 *
 */
public class MapJoin {
    static class MyMapper extends Mapper<LongWritable,Text,Text, NullWritable>{

        Map<String ,String> map=new HashMap<String,String>();
        /**
         * 读取缓存中的数据，封装到容器中
         * 读：流
         * 容器：
         *  因为等会要进行匹配，匹配的时候，pid匹配，最好pid先抽出来
         *  Map
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取缓存中的数据路径
            Path path = context.getLocalCacheFiles()[0];
            String p=path.toString();
            BufferedReader br=new BufferedReader(new FileReader(p));
            String line=null;

            while((line=br.readLine())!=null){
                String[] infos=line.split("\t");
                map.put(infos[0],infos[1]+"\t"+infos[2]+"\t"+infos[3]);
            }

        }

        Text k=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");

            String pid=infos[2];
            if(map.containsKey(pid)){
                String res=value.toString()+map.get(pid);
                k.set(res);
                context.write(k,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //加载配置文件
        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","hadoop");
        conf.set("fs.defaultFS","hdfs://172.16.98.186:9000");
        //启动一个Job 封装计算程序的mapper和reducer 输入和输出
        Job job=Job.getInstance(conf);

        //设置的是计算程序的主驱动类 运行的时候打成jar包运行
        job.setJarByClass(MapJoin.class);

        //设置mapper和reducer的类
        job.setMapperClass(MyMapper.class);

        //这里指的是最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        //将指定文件的url加载到内存中,参数是加载文件地址，这种方式只能打jar包运行，需要的集群中找缓存文件
        //会先把文件拉到本地
        job.addCacheFile(new URI("/barry-test/in/in05/t_product"));

        //设置输入路径和输出路径   运行代码的时候控制台手动输入的参数，第一个参数
        //输入路径：需要统计的单词的路径
        FileInputFormat.addInputPath(job,new Path("/barry-test/in/in05/t_order"));

        //输出路径：最终结果输出的路径  注意：输出路径一定不能存在 hdfs怕把原来的文件覆盖了，所以要求传入的路径不能存在
        FileOutputFormat.setOutputPath(job,new Path("/barry-test/out/map_join_01"));

        //job提交 传入boolean类型的参数 是否打印日记
        job.waitForCompletion(true);


    }
}
