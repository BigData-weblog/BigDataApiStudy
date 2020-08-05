package hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class TestFilter {

    static Connection conn;
    static Admin admin;
    public static void init() throws IOException {
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //添加配置文件zookeeper的路径
        conf.set("hbase.zookeeper.quorum","172.16.98.186:2181,172.16.98.185:2181,172.16.98.184:2181");
        //通过连接工厂创建连接
        conn = ConnectionFactory.createConnection(conf);
        //通过conn获取admin对象
        admin = conn.getAdmin();
    }
    //过滤器
    public static void testFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("barry:t1"));
        Scan scan=new Scan();
        //列族过滤器
        FamilyFilter filter01 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("cf1".getBytes()));
        //列名过滤器
        QualifierFilter filter02 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
        //FilterList listF=new FilterList(filter01,filter02);// &&的关系

        FilterList listF=new FilterList(FilterList.Operator.MUST_PASS_ALL);//MUST_PASS_ONE ||的关系
        listF.addFilter(filter01);
        listF.addFilter(filter02);

        //FilterList listF=new FilterList(FilterList.Operator.MUST_PASS_ALL,filter01,filter02);一步到位
        scan.setFilter(listF);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            List<KeyValue> list =next.list();
            for(KeyValue kv:list){
                System.out.print(new String(kv.getRow())+"\t");
                System.out.print(new String(kv.getFamily())+"\t");
                System.out.print(new String(kv.getQualifier())+"\t");
                if("age".equals(Bytes.toString(kv.getQualifier()))){
                    System.out.println(Bytes.toInt(kv.getValue()));
                }else{
                    System.out.println(new String(kv.getValue()));
                }
            }
        }
    }

    //单列值过滤器
    public static void testFilter02() throws IOException {
        Table table = conn.getTable(TableName.valueOf("barry:t1"));
        Scan scan=new Scan();
        //单列值过滤器 遇到列名和列的值进行过滤，如果这一行存在过滤的列名，对应的值不是想要 会过滤掉 不要
        //如果这一行中不含有需要过滤的列名 默认是返回的 filter01.setFilterIfMissing(false);
        //false:代表如果不包含 不过滤
        //true:如果不包含，过滤
        SingleColumnValueFilter filter01 = new SingleColumnValueFilter("cf1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "no007".getBytes());
        filter01.setFilterIfMissing(true);
        scan.setFilter(filter01);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            List<KeyValue> list =next.list();
            for(KeyValue kv:list){
                System.out.print(new String(kv.getRow())+"\t");
                System.out.print(new String(kv.getFamily())+"\t");
                System.out.print(new String(kv.getQualifier())+"\t");
                if("age".equals(Bytes.toString(kv.getQualifier()))){
                    System.out.println(Bytes.toInt(kv.getValue()));
                }else{
                    System.out.println(new String(kv.getValue()));
                }
            }
        }
    }

    public static void testFilter03() throws IOException {
        Table table = conn.getTable(TableName.valueOf("barry:t1"));
        Scan scan=new Scan();
        scan.setStartRow("row12".getBytes());
        //前缀过滤器 针对行键,过滤行键前缀的
        Filter filter01 = new PrefixFilter("row".getBytes());
        //列前缀过滤器
        Filter filter02 = new ColumnPrefixFilter("na".getBytes());
        //分页过滤器 pageindex,pagesize
        Filter filter03 = new PageFilter(3);
        FilterList filterList=new FilterList(filter01,filter02,filter03);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            List<KeyValue> list =next.list();
            for(KeyValue kv:list){
                System.out.print(new String(kv.getRow())+"\t");
                System.out.print(new String(kv.getFamily())+"\t");
                System.out.print(new String(kv.getQualifier())+"\t");
                if("age".equals(Bytes.toString(kv.getQualifier()))){
                    System.out.println(Bytes.toInt(kv.getValue()));
                }else{
                    System.out.println(new String(kv.getValue()));
                }
            }
        }


    }
    public static void main(String[] args) throws IOException {
        init();
        //testFilter();
        //testFilter02();
        testFilter03();
    }
}
