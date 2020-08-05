package hadoop.hdfsconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 删除hdfs指定类型的文件
 *
 * @param null
 * @author barry.cao
 * @date 2020-07-23 16:15:21
 * @return
 **/
public class HTFSTest04 {

    public static final String FILE_TYPE = "txt";

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://172.16.98.186:9000"), conf, "hadoop");

        //这种方式必须指定一个文件名
        Path p = new Path("/barry-test");

        FileStatus fileStatus = fs.getFileStatus(p);
        if (fileStatus.isFile()) {
            deleteTypefile(fs, p);
        } else {
            deleteDirFile(fs, p);
        }
    }

    private static void deleteDirFile(FileSystem fs, Path p) throws IOException {
        //删除目录
        FileStatus[] fileStatuses = fs.listStatus(p);

        for(FileStatus fss:fileStatuses){
            Path path=fss.getPath();

            if(fss.isDir()){
                deleteDirFile(fs,path);
            }else{
                deleteTypefile(fs,path);
            }



        }
    }

    private static void deleteTypefile(FileSystem fs, Path p) throws IOException {
        //删除文件
        FileStatus fileStatus = fs.getFileStatus(p);

        String name = fileStatus.getPath().getName();
        if(name.endsWith(FILE_TYPE)){
            fs.delete(p,false);
        }

    }


}
