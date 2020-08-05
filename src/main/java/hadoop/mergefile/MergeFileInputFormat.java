package hadoop.mergefile;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * k:mapper 输入的key的类型，默认指的偏移量
 * v：mapper输入的value的类型，默认指的一行的内容
 *
 * 每次读取一个文件的内容
 * key：整个文件的内容
 * value：NullWritable
 */
public class MergeFileInputFormat  extends FileInputFormat<Text, NullWritable> {
    //构建recordreader
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MergeFileRecordReader record = new MergeFileRecordReader();
        record.initialize(inputSplit,taskAttemptContext);
        return record;
    }
}
