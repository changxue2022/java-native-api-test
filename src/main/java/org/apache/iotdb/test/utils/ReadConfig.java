package org.apache.iotdb.test.utils;



import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.System.out;

public class ReadConfig {
    private final String configPath = "resources/config.properties";
//    private final String configPath = System.getProperty("user.dir") + "/conf/application.properties";
    private static ReadConfig config;

    private Properties properties = null;

    /*
     * 单例获取实例
     */
    public static ReadConfig getInstance() throws IOException {
        if (null == config) {
            config = new ReadConfig();
        }
        return config;
    }

    private ReadConfig() throws IOException {
        properties = new Properties();
        InputStream in = ReadConfig.class.getClassLoader().getResourceAsStream("config.properties");
        properties.load(in);
    }
    public String getValue(String key) {
        return properties.getProperty(key);
    }

    public static void main(String[] args) throws IOException {
        out.println(ReadConfig.getInstance().getValue("host"));
    }
}
