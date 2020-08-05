package hadoop.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class DiffRecordWriter extends RecordWriter<Text, DoubleWritable> {

    FSDataOutputStream out01=null;
    FSDataOutputStream out02=null;
    FileSystem fs;
    public DiffRecordWriter(FileSystem fs) throws IOException {
        this.fs=fs;
        out01 = fs.create(new Path("/barry-test/out/kaoshi01/jige"));
        out02 = fs.create(new Path("/barry-test/out/kaoshi01/bujige"));
    }

    //真正向外写出的方法，不同的成绩写出到不同的文件夹，流，需要创建两个流  输出流 怎么获取流：filesystem对象，获取的时候需要配置文件
    public void write(Text text, DoubleWritable doubleWritable) throws IOException, InterruptedException {
        double score=doubleWritable.get();
        if(score>=60){
            //输出的时候不仅仅要输出分数，还要输出 课程和名字
            out01.write((text.toString()+"\t"+score+"\n").getBytes());
        }else{
            out02.write((text.toString()+"\t"+score+"\n").getBytes());
        }
    }

    //关闭资源的方法
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        out01.close();
        out02.close();
    }
}
