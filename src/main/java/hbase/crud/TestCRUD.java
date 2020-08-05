package hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestCRUD {

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
    // 创建命名空间
    public static void createNamespace() throws IOException {
        NamespaceDescriptor.Builder builder=NamespaceDescriptor.create("barry");
        NamespaceDescriptor nsd=builder.build();
        admin.createNamespace(nsd);
        admin.close();
    }

    //删除命名空间
    public static void deleteNamespace() throws IOException {

        admin.deleteNamespace("barry");
        admin.close();
    }

    //创建表
    public static void createtable() throws IOException {

        //创建表描述符
        HTableDescriptor desc=new HTableDescriptor(TableName.valueOf("barry:t1"));
        //创建列族描述符
        HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes("cf1"));
        //添加列族描述符
        desc.addFamily(colDesc);
        //创建表
        admin.createTable(desc);
    }

    //drop 表
    public static void dropTable() throws IOException {
        //禁用表
        admin.disableTable(TableName.valueOf("t1"));
        // 删除表
        admin.deleteTable(TableName.valueOf("t1"));
        admin.close();

    }

    //put
    public static void put() throws IOException {
        Table t=conn.getTable(TableName.valueOf("barry:t1"));
        Put put=new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("no001"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("no"),Bytes.toBytes("tom"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(12));

        t.put(put);
        t.close();
    }

    public static void update() throws IOException {
        Table t=conn.getTable(TableName.valueOf("barry:t1"));
        Put put=new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("no"),Bytes.toBytes("tomos"));
        t.put(put);
        t.close();
    }

    //put list
    public static void putList() throws IOException {
        Table t=conn.getTable(TableName.valueOf("barry:t1"));
        Put put=null;
        List<Put> list=new ArrayList<Put>();
        for(int i=2;i<20;i++){
            put=new Put(Bytes.toBytes("row"+i));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("no00"+i));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("no"),Bytes.toBytes("tom"+i));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(i));
            list.add(put);
        }
        t.put(list);
        t.close();
    }

    //get
    public static void getValue() throws IOException {
        Table table=conn.getTable(TableName.valueOf("barry:t1"));
        Get get=new Get(Bytes.toBytes("row10"));
        //get.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"));
        get.addFamily(Bytes.toBytes("cf1"));
        Result r = table.get(get);
        String no = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no")));
        String name = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
        int age = Bytes.toInt(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")));
        System.out.println(name+" , "+age+" , "+no);
    }

    public static void findAll() throws IOException {
        Table table = conn.getTable(TableName.valueOf("barry:t1"));
        Scan scan=new Scan(Bytes.toBytes("row10"),Bytes.toBytes("row20"));
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()){
            Result r = it.next();
            String no = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no")));
            String name = Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
            int age = Bytes.toInt(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")));
            System.out.println(no+" , "+name+" , "+age);
        }
    }

    public static void delete() throws IOException {
        Table table = conn.getTable(TableName.valueOf("barry:t1"));
        Delete del=new Delete(Bytes.toBytes("row19"));
        del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
        table.delete(del);
        table.close();
    }

    //scan 和 过滤查询  含头不含尾
    public static void scanDate() throws IOException {
        Table table = conn.getTable(TableName.valueOf("barry:t1"));
        Scan scan=new Scan();

        scan.setStartRow("row3".getBytes());
        scan.setStopRow("row7".getBytes());
        //Scan scan=new Scan("row1".getBytes());
        //scan.addColumn("cf1".getBytes(),"name".getBytes());
        //scan.addColumn("cf1".getBytes(),"no".getBytes());
        scan.addColumn("cf1".getBytes(),"age".getBytes());

        //过滤器
        Filter filter1 = new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(10)));
        Filter filter2 = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(3)));

        //多过滤器的联合使用
        List<Filter> list01=new ArrayList<Filter>();
        list01.add(filter1);
        list01.add(filter2);

        Filter f=new FilterList(list01);
        scan.setFilter(f);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            List<KeyValue> list = next.list();
            for (KeyValue kv:list){

                System.out.print(new String(kv.getRow())+"\t");
                System.out.print(new String(kv.getFamily())+"\t");
                System.out.print(new String(kv.getQualifier())+"\t");
                if("age".equals(Bytes.toString(kv.getQualifier()))){
                    System.out.println(Bytes.toInt(kv.getValue()));
                }else{
                    System.out.println(new String(kv.getValue()));
                }


            }
            //System.out.println(next);
        }

    }
    public static void main(String[] args) throws IOException {
        init();
        //dropTable();
        //createNamespace();
        //createtable();
        //put();
        //update();
        //putList();
        //getValue();
        //findAll();
        //delete();
        scanDate();
    }

}
