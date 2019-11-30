package com.hadoop.hbase_demo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 获取Hbase连接对象工具类
 */
public class HBaseConn {

    public static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration;
    private static Connection connection;

    public HBaseConn(){
        try {
            if(configuration==null){
                //配置Configuration对象
                configuration = HBaseConfiguration.create();
                configuration.set(Constants.HBASE_ROOTDIR,PropertiesUtil.getPropertyVal(Constants.HBASE_ROOTDIR));
                configuration.set(Constants.HBASE_ZOOKEEPER_QUORUM,PropertiesUtil.getPropertyVal(Constants.HBASE_ZOOKEEPER_QUORUM));
                configuration.set(Constants.HBASE_CLUSTER_DISTRIBUTED,PropertiesUtil.getPropertyVal(Constants.HBASE_CLUSTER_DISTRIBUTED));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取的HBase连接对象
     * @return
     */
    private Connection getConnection(){
        if (connection==null){
            try {
                //通过工厂类来创建Connection方法
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获得HBase连接对象
     * @return
     */
    public static Connection getHBaseCon(){
        return INSTANCE.getConnection();
    }

    /**
     * 通过表名获得HBase表对象
     * @param tableName
     * @return
     * @throws Exception
     */
    public static Table getTable(String tableName) throws Exception{
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭Hbase连接对象
     */
    public static void closeConn(){
        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
