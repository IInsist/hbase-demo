package com.hadoop.hbase_demo.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class RunJob
{
    HBaseAdmin admin = null;
    Configuration config = null;

    @Before
    public void init(){
        try {
            config = HBaseConfiguration.create();
            admin = new HBaseAdmin(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws IOException {
        /*
        * 1，表名
        * 2，列蔟
        * */
        TableName tableName = TableName.valueOf("test");//1，表名
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); //表结构
        HColumnDescriptor columnFamily1 = new HColumnDescriptor("baseInfo"); //2，列蔟
        tableDescriptor.addFamily(columnFamily1);
        admin.createTable(tableDescriptor);
    }

    /**
     * 新增数据
     */
    @Test
    public void putVal(){
        TableName tableName = TableName.valueOf("test");
        try {
            HTable table = new HTable(config,tableName);
            /*
            * 1表名
            * 2，ROW KEY
            * 3，ColumnFamily
            * 4，Value
            * */
            byte[] rowKey = Bytes.toBytes("0001");
            Put put = new Put(rowKey);

            byte[] columnFamily = Bytes.toBytes("baseInfo"); //列蔟
            byte[] qualifier = Bytes.toBytes("1"); //分隔符
            byte[] values = Bytes.toBytes("value"); //
            put.add(columnFamily,qualifier,values);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过rowKey查找数据
     */
    @Test
    public void getVal(){
        TableName tableName = TableName.valueOf("test");
        try {
            HTable table = new HTable(config, tableName);
            Get get = new Get(Bytes.toBytes("0001"));
            Result result = table.get(get);
            System.out.println(result);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
