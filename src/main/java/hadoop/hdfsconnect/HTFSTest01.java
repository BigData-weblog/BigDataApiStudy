package hadoop.hdfsconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HTFSTest01 {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://172.16.98.186:9000"),conf);

        Path p=new Path("/barry-test/test");

        //fs.mkdirs(p);
        //默认递归删除
        //fs.delete(p,false);//可以设定是否递归删除

        /*boolean ss=fs.exists(new Path("/barry-test"));
        System.out.println(ss);

        fs.rename(new Path("/barry-test/test.txt"),new Path("/barry-test/test00.txt"));*/


        //获取指定目录下的文件列表,只能获取文件
        /*RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/barry-test/ss"), false);

        while(listFiles.hasNext()){

            LocatedFileStatus next = listFiles.next();
            *//*System.out.println(next.getPath());
            System.out.println(next.getLen());
            System.out.println(next.getBlockSize());*//*
            BlockLocation[] blockLocations = next.getBlockLocations();
            for(BlockLocation bl:blockLocations){
                System.out.println(bl);
            }
            System.out.println(blockLocations.length);
        }*/

        FileStatus[] fileStatuses = fs.listStatus(new Path("/barry-test"));
        for(FileStatus f:fileStatuses){
            System.out.println(f);
            System.out.println(f.isDirectory());
            System.out.println(f.isFile());



        }
        fs.close();

    }
}
