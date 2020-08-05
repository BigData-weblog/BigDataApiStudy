package hbase.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class TestPageFilter {

    public static void main(String[] args) throws IOException {
        ResultScanner pageDate = getDate(3, 5);
        Iterator<Result> iterator = pageDate.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            System.out.println(new String(next.getRow()));
        }
    }
    //需要做的工作就是将用户传入的页码的参数 转化为 startrow
    public static ResultScanner getDate(int pageIndex, int pagesize) throws IOException {
        String startrow = getCurrentStart(pageIndex, pagesize);
        return getPageDate(startrow,pagesize);

    }

    private static String getCurrentStart(int pageIndex,int pageSize) throws IOException {

        //判断pageIndex是否大于1
        if(pageIndex<=1){
            return null;
        }else{
            String statRow="";
            for(int i=1;i<=pageIndex-1;i++){
                ResultScanner pageDate = getPageDate(statRow, pageSize);
                Iterator<Result> iterator = pageDate.iterator();
                Result res=null;
                while(iterator.hasNext()){
                    res=iterator.next();
                }
                String lastRow = new String(res.getRow());
                String newrow = new String(Bytes.add(lastRow.getBytes(), "0x00".getBytes()));
                statRow=newrow;
            }
            return statRow;
        }

    }
    public static ResultScanner getPageDate(String startrow, int pagesize) throws IOException {

        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //添加配置文件zookeeper的路径
        conf.set("hbase.zookeeper.quorum","172.16.98.186:2181,172.16.98.185:2181,172.16.98.184:2181");
        //通过连接工厂创建连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table=connection.getTable(TableName.valueOf("barry:t1"));

        Scan scan=new Scan();
        if(!StringUtils.isBlank(startrow)){
            scan.setStartRow(startrow.getBytes());
        }

        PageFilter pageFilter = new PageFilter(pagesize);
        scan.setFilter(pageFilter);

        ResultScanner scanner = table.getScanner(scan);
        return scanner;
    }
}
