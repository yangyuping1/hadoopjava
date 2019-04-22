package day5;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient {
    public static Connection connection;;
    public static void main(String[] args) {

    }
    //静态代码块
    static {
        //获取连接conf
        HBaseConfiguration conf = new HBaseConfiguration();

        //设置参数
        conf.set("hbase.zookeeper.quorum","192.168.10.111");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //判断表是否存在
    public static boolean exits(String table) throws Exception{
        //拿到admin'对象
        Admin admin = connection.getAdmin();
        //判断是否存在
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }
    //创建表
    ///String...cf1默认有多个.传参
    public static void createTable(String table,String...cf1) throws Exception {
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        //遍历cf
        for (String family : cf1) {
            hTableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        //创建
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功");
    }
    //删除
    public static void delete(String table) throws Exception{
        Admin admin = connection.getAdmin();
        //下线
        admin.disableTable(TableName.valueOf(table));
        //删除
        admin.deleteTables(table);
        System.out.println("删除成功");
    }
    //添加数据
    public static void addTable(String table,String rowkey,String columnfamily,String column,String value) throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        // new Put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //添加
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
        //告知上边,有值了
        table1.put(put);
        System.out.println("添加成功");
    }
    //scan方法
    public static void showTable(String table) throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        //拿到所有的rowkey
        ResultScanner results = table1.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(cell.getFamilyArray().toString());
                System.out.println(cell.getQualifierArray().toString());
                System.out.println(cell.getValueArray().toString());
                System.out.println(cell.getRow().toString());
            }
        }
    }
    //get方法
    public static void getTable(String table) throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(cell.getFamilyArray().toString());
            System.out.println(cell.getQualifierArray().toString());
            System.out.println(cell.getValueArray().toString());
            System.out.println(cell.getRow().toString());
        }
    }
}
