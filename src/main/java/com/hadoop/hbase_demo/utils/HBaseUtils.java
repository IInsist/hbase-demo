package com.hadoop.hbase_demo.utils;

import org.apache.hadoop.hbase.client.Admin;

/**
 * 操作HBase数据库工具类
 */
public class HBaseUtils {

    /**
     * 创建表格
     * @param tableName
     * @param columnFamily
     * @return
     */
    public boolean createTable(String tableName,String[] columnFamily){
        //Admin admin = HBaseConn.getHBaseCon().getAdmin();
        return true;
    }
}
