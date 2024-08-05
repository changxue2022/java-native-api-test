package org.apache.iotdb.api.test.utils;

// 导入IoTDB的RPC异常类，用于处理与IoTDB服务器连接时可能发生的异常。
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
// 导入IoTDB的执行语句异常类，用于处理执行SQL语句时可能发生的异常。
import org.apache.iotdb.rpc.StatementExecutionException;
// 导入IoTDB的会话接口Session，用于执行SQL语句和管理会话。
import org.apache.iotdb.session.Session;
// 导入IoTDB的会话池接口SessionPool，用于管理一组Session。
import org.apache.iotdb.session.pool.SessionPool;

// 导入Java IO相关的类，用于处理可能发生的IO异常。
import java.io.IOException;
// 导入Java的Arrays工具类，用于处理数组和集合。
import java.util.Arrays;

// 定义公共类PrepareConnection，用于准备与IoTDB服务器的连接。
public class PrepareConnection {
    // 定义配置对象，用于读取和存储配置信息。
    private static ReadConfig config;
    // 定义会话对象，用于与IoTDB服务器建立连接。
    private static Session session;

    // 静态初始化块，用于在类加载时初始化config对象。
    static {
        try {
            config = ReadConfig.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 公共静态方法，用于获取Session对象。
    public static Session getSession() throws IoTDBConnectionException {
        Session session = null;
        // 根据配置判断是否为集群模式，并创建对应的Session对象。
        if (config.getValue("is_cluster").equals("true")) {
            // 集群模式下，使用多个节点的URL创建Session。
            String host_nodes_str = config.getValue("host_nodes");
            session = new Session.Builder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(","))) // 设置节点URL列表
                    .username(config.getValue("user")) // 设置用户名
                    .password(config.getValue("password")) // 设置密码
                    .enableRedirection(false) // 设置是否启用重定向
                    .maxRetryCount(0) // 设置最大重试次数
                    .build();
        } else {
            // 非集群模式下，使用单个节点的URL和端口创建Session。
            session = new Session.Builder()
                    .host(config.getValue("host")) // 设置主机地址
                    .port(Integer.parseInt(config.getValue("port"))) // 设置端口号
                    .username(config.getValue("user")) // 设置用户名
                    .password(config.getValue("password")) // 设置密码
                    .enableRedirection(false) // 设置是否启用重定向
                    .maxRetryCount(0) // 设置最大重试次数
                    .build();
        }
        // 打开Session，并设置获取数据时的批量大小。
        session.open(false);
        session.setFetchSize(10000);
        return session;
    }

    // 公共静态方法，用于获取SessionPool对象。
    public static SessionPool getSessionPool() {
        // 根据配置判断是否为集群模式，并创建对应的SessionPool对象。
        // 代码逻辑与getSession类似，但用于创建会话池。
        SessionPool sessionPool = null;
        if (config.getValue("is_cluster").equals("true")) {
            String host_nodes_str = config.getValue("host_nodes");
            sessionPool = new SessionPool.Builder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                    .user(config.getValue("user"))
                    .password(config.getValue("password"))
                    .maxSize(10)
                    .maxRetryCount(0)
//                    .timeOut(Long.parseLong(config.getValue("session_timeout")))
                    .build();
        } else {
            sessionPool = new SessionPool.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .user(config.getValue("user"))
                    .password(config.getValue("password"))
                    .maxSize(10)
                    .maxRetryCount(0)
//                    .timeOut(Long.parseLong(config.getValue("session_timeout")))
                    .build();
        }
        // 设置会话池的批量大小。
        sessionPool.setFetchSize(10000);
        return sessionPool;
    }

    // 主方法，程序的入口点。
    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 定义一些测试用的变量，例如时间序列的路径、主机地址和时间戳。
        String ROOT_SG1_D1 = "root.multi.d1";
        String host = "172.20.70.45";
        long timestamp = 601L;
        // 这里可以添加代码来使用getSession或getSessionPool方法，并执行一些操作。
    }
}



