package hbase.testconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestConnect {

    public static void main(String[] args) throws IOException {
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //添加配置文件zookeeper的路径
        conf.set("hbase.zookeeper.quorum","172.16.98.186:2181,172.16.98.185:2181,172.16.98.184:2181");
        //通过连接工厂创建连接
        Connection connect = ConnectionFactory.createConnection(conf);
        //通过连接获取table信息
        Table table = connect.getTable(TableName.valueOf("barry_test"));

        //设定行键
        Put put = new Put(Bytes.toBytes("rk003"));
        //设置字段值
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("xxx"));

        table.put(put);
        table.close();
        connect.close();

    }
}