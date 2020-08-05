package hadoop.hdfsconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 级联删除hdfs上某个文件夹
 * @author barry.cao
 * @date 2020-07-23 16:15:21
 * @param null
 * @return
 **/
public class HTFSTest03 {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://172.16.98.186:9000"), conf, "hadoop");

        //这种方式必须指定一个文件名
        Path p = new Path("/barry-test");
        deleteEmptyAll(fs, p);
    }

    private static void deleteEmptyAll(FileSystem fs, Path p) throws IOException {
        //首先判断给定的目录是否是空目录
        FileStatus[] fileStatuses = fs.listStatus(p);

        if (fileStatuses.length == 0) {
            fs.delete(p,false);
        } else {
            //非空目录
            for (FileStatus fss : fileStatuses) {
                Path path = fss.getPath();
                if (fss.isFile()) {
                    //如果是文件，判断长度是否为0
                    if (fss.getLen() == 0) {
                        fs.delete(path, false);
                    }
                } else {
                    deleteEmptyAll(fs, path);
                }
            }
            //需要判断最终删除完子文件之后的文件夹是否为空
            FileStatus[] fileStatuses1 = fs.listStatus(p);
            if(fileStatuses1.length==0){
                fs.delete(p,false);
            }
        }
    }


}
