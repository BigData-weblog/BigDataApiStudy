package hadoop.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, IntWritable, Text,IntWritable> {

    //重写reduce方法，这个reduce方法对应的是一个切片（maptask）的数据进行统计

    /**
     * key:map输出的key
     * values:map输出的相同key的所有value
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable v:values){
            sum+=v.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
