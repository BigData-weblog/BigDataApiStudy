package hadoop.hdfsconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HTFSTest {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        //System.setProperty("HADOOP_USER_NAME","hadoop");
        Configuration conf=new Configuration();
        //hdfs-default.xml  hdfs-site.xml
        //手动加载配置文件
        //conf.addResource("hdfs-default.xml");
        conf.set("dfs.replication","4");
        //conf.set("fs.default.name","hdfs://localhost:9000");
        //FileSystem fs=FileSystem.get(new URI("hdfs://172.16.98.186:9000"),conf,"hadoop");
        FileSystem fs=FileSystem.get(new URI("hdfs://172.16.98.186:9000"),conf,"hadoop");
        //FileSystem fs=FileSystem.get(conf);
        //-DHADOOP_USER_NAME=hadoop

        System.out.println(fs);

        Path dst=new Path("/barry-test");
        Path src=new Path("file:/Users/barry.cao/Desktop/test3.txt");
        //Path src2=new Path("/Users/barry.cao/Documents/Test.java");

        fs.copyFromLocalFile(src,dst);

        fs.close();

    }
}
