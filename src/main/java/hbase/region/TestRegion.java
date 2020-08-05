package hbase.region;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 *  建表的时候指定预分区
 *  预分区的目的  将表划分到多个regionserver  避免数据插入或读取的 数据热点的问题
 *
 *
 */
public class TestRegion {
    public static void main(String[] args) throws IOException {

        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //添加配置文件zookeeper的路径
        conf.set("hbase.zookeeper.quorum","172.16.98.186:2181,172.16.98.185:2181,172.16.98.184:2181");
        //通过连接工厂创建连接
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //建表
        HTableDescriptor test_region_barry = new HTableDescriptor(TableName.valueOf("test_region_barry"));
        HColumnDescriptor info01 = new HColumnDescriptor("info01");
        HColumnDescriptor info02 = new HColumnDescriptor("info02");
        test_region_barry.addFamily(info01);
        test_region_barry.addFamily(info02);
        /**
         * 1.表描述器
         * 2.起始key     第一个分区的结束rowkey
         * 3.结束key     最后一个分区的起始rowkey   中间的分区  平均比分的
         * 4.分区数量
         */
        admin.createTable(test_region_barry,"100".getBytes(),"600".getBytes(),3);


        //这种方式定义的分界线 region之间的分界线  给的就是rowkey的值  分区就是按照rowkey 字典顺序
        /*byte[][] split={"100".getBytes(),"300".getBytes(),"500".getBytes()};
        admin.createTable(test_region_barry,split);*/
    }
}
