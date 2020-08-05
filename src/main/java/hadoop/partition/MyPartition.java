package hadoop.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区规则
 * key:map输出的key的类型
 * value:map输出的value的类型
 */
public class MyPartition extends Partitioner<Text, IntWritable> {

    /**
     * Text key , map的输出的key
     * IntWritable value,map输出的value
     * int numPartitions,分区个数  job.setNumReduceTasks();
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String k=key.toString();
        char kc=k.charAt(0);
        if(kc>='a' && kc <'j'){
            return 0;
        }else if(kc>='j' && kc<'o'){
            return 1;
        }else{
            return 2;
        }
    }
}
