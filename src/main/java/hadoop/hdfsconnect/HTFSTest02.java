package hadoop.hdfsconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HTFSTest02 {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://172.16.98.186:9000"),conf,"hadoop");

        //这种方式必须指定一个文件名
        Path p=new Path("/barry-test/test");

        /*FileInputStream fileInputStream=new FileInputStream(new File("/Users/barry.cao/Desktop/test3.txt"));

        //文件上传
        //创建hdfs输入流
        FSDataOutputStream out = fs.create(p);
        IOUtils.copyBytes(fileInputStream,out,4096);*/



        //文件下载
        FSDataInputStream in = fs.open(p);

        FileOutputStream out=new FileOutputStream("/Users/barry.cao/Desktop/testcc");

        IOUtils.copyBytes(in,out,4096);

    }
}
