package com.hadoop.hbase_demo.utils;

import java.io.IOException;
import java.util.Properties;

/**
 *  处理properties文件类
 */
public class PropertiesUtil {

    public static Properties properties = new Properties();

    static {
        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(Constants.CONFIG_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getPropertyVal(String propertyKey){
        return properties.getProperty(propertyKey);
    }

}
