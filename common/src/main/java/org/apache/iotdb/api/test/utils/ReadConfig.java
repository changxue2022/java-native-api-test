package org.apache.iotdb.api.test.utils;


// 导入Java IO相关的类，用于处理可能发生的IO异常。

import java.io.IOException;
// 导入Java IO中的InputStream类，用于读取输入流。
import java.io.InputStream;
// 导入Java的Properties类，用于处理配置文件。
import java.util.Properties;

// 导入java.lang.System的out对象，用于打印日志或信息。
import static java.lang.System.out;

/**
 * 这个类用于读取配置文件，例如properties文件，并提供一个全局访问点。
 */
public class ReadConfig {
    // 以下两行是配置文件路径的注释示例，实际使用时应该取消注释其中一个。
    // private static final String configPath = "resources/config.properties"; // 类路径下的资源文件
    // private final String configPath = System.getProperty("user.dir") + "/conf/application.properties"; // 绝对路径下的文件

    // 声明单例对象的静态引用。
    private static ReadConfig config;
    // 声明InputStream对象，用于读取配置文件。
    private InputStream in;

    // 声明Properties对象，用于存储配置文件中的键值对。
    private Properties properties = null;

    /**
     * 单例模式获取实例的方法。
     * 如果config对象为null，则创建一个新的ReadConfig实例。
     *
     * @return ReadConfig的单例实例。
     * @throws IOException 如果读取配置文件时发生IO异常。
     */
    public static ReadConfig getInstance() throws IOException {
        if (null == config) {
            config = new ReadConfig();
        }
        // 以下行为注释，用于示例输出配置文件的绝对路径。
        // out.println(new File(configPath).getAbsolutePath());
        return config;
    }

    /**
     * 私有构造方法，确保外部不能直接创建ReadConfig实例。
     * 构造方法中加载配置文件。
     *
     * @throws IOException 如果加载配置文件时发生IO异常。
     */
    private ReadConfig() throws IOException {
        properties = new Properties();
        // 通过类加载器获取配置文件的输入流。
        in = ReadConfig.class.getClassLoader().getResourceAsStream("config.properties");
        // 加载配置文件到Properties对象中。
        properties.load(in);
    }

    /**
     * 根据给定的键名返回配置项的值。
     *
     * @param key 配置项的键名。
     * @return 键名对应的值。
     */
    public String getValue(String key) {
        return properties.getProperty(key);
    }

    /**
     * 关闭配置文件的输入流。
     *
     * @throws IOException 如果关闭输入流时发生IO异常。
     */
    public void close() throws IOException {
        in.close();
    }

    /**
     * 主方法，用于演示如何使用ReadConfig类获取配置项的值。
     * 打印出配置文件中键名为"host"的配置项的值。
     *
     * @param args 命令行参数。
     * @throws IOException 如果在获取配置实例或读取配置项时发生IO异常。
     */
    public static void main(String[] args) throws IOException {
        out.println(ReadConfig.getInstance().getValue("host"));
    }
}
