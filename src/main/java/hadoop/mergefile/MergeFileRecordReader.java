package hadoop.mergefile;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MergeFileRecordReader extends RecordReader<Text, NullWritable> {

    FSDataInputStream open=null;
    //用于表识文件是否读取完成
    boolean isReader=false;
    long len=0;
    Text k=new Text();
    //主要就是进行文件读取，整个文件进行读取 创建一个流，要创建流必须待优路径 怎么获取文件路径
    //这个方法是初始化的方法 类似setup方法，用于对属性或者链接或流进行初始化
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit sl=(FileSplit)inputSplit;
        Path path=sl.getPath();
        FileSystem fs=FileSystem.get(taskAttemptContext.getConfiguration());
        open=fs.open(path);
        len=sl.getLength();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isReader){
            byte[] b=new byte[(int)len];
            open.readFully(0,b);
            k.set(b);
            isReader=true;
            return isReader;
        }else {
            return false;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.k;
    }

    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    //获取执行进度的方法
    public float getProgress() throws IOException, InterruptedException {
        return isReader?1.0F:0.0F;
    }

    //进行一个链接或流的关闭
    public void close() throws IOException {
        open.close();
    }
}
