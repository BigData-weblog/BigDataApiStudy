package hadoop.output;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//这两个泛型，对应reducer两个输出的泛型
public class DiffDicOutputFormat extends FileOutputFormat<Text, DoubleWritable> {

    //这个方法 获取RecordWriter 对象
    public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fs=FileSystem.get(taskAttemptContext.getConfiguration());
        return new DiffRecordWriter(fs);
    }
}
