package org.apache.iotdb.api.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;

/**
 * 多线程执行创建 pipe 的 sql,可以独立运行，用于执行创建管道的并发测试。
 */
public class CreatePipeConcurrent {
    // 定义客户端的数量。
    private static int clientCount = 100;
    // 定义循环次数。
    private static int loop = 1000;

    // 主方法，程序的入口点。
    public static void main(String[] args) {
        // 定义设备路径的前缀。
        String pattern = "root.test.g_0.d_";
        // 定义实时模式。
        String mode = "hybrid";
        // 定义连接器的名称。
        String connector = "iotdb-thrift-connector";
        // 定义目标主机的地址和端口。
        String targetHost = "172.20.70.32:6667";
        // 创建一个列表，存储IoTDB节点的URL。
        List<String> nodeUrls = new ArrayList<>(3);
        nodeUrls.add("iotdb-33:6667");
        // 注释掉的代码，可能是用于添加更多的IoTDB节点URL。
        // nodeUrls.add("iotdb-19:6667");
        // nodeUrls.add("iotdb-21:6667");
        // 使用SessionPool.Builder构建会话池。
        SessionPool session = new SessionPool.Builder()
                .nodeUrls(nodeUrls) // 设置IoTDB节点的URL列表。
                .maxSize(clientCount) // 设置会话池的最大大小。
                .user("root") // 设置用户名。
                .password("root") // 设置密码。
                .build(); // 构建会话池。
        // 记录开始时间。
        long startTime = System.currentTimeMillis();
        // 创建固定大小的线程池。
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);

        // 循环执行SQL语句。
        for (int i = 0; i < loop; i++) {
            // 注释掉的代码，可能是用于创建管道的SQL语句模板。
            // String sql = "create pipe test with extractor (...";
            // 使用"start pipe"语句启动已定义的管道。
            String sql = "start pipe pipe_"+i;
            // 注释掉的代码，可能是用于测试不同管道的启动。
            // String sql = "start pipe pipe_s_0_d_"+i;
            // 注释掉的代码，可能是用于删除管道的SQL语句。
            // String sql = "drop pipe pipe_s_0_d_"+i;
            // 注释掉的代码，可能是用于测试不同管道的删除。
            // String sql = "drop pipe pipe_"+i+";";
            // 打印SQL语句，已注释。
            // out.println(sql);
            // 将ExecuteSQL任务和会话池作为参数，提交给线程池执行。
            pool.execute(new ExecuteSQL(session, sql));
            // 注释掉的代码，可能是用于测试同时启动多个管道。
            // pool.execute(new ExecuteSQL(session, "start pipe pipe_"+i+";"));
        }
        // 关闭线程池。
        pool.shutdown();
        // 循环等待所有任务执行结束。
        while (true) {
            if (pool.isTerminated()) {
                // 如果所有子线程都结束了，打印总耗时，然后退出循环。
                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
                break;
            }
        }
        // 关闭会话池。
        session.close();
    }
}

// 定义一个名为ExecuteSQL的类，它实现了Runnable接口，可以在线程中执行。
class ExecuteSQL implements Runnable {
    // 成员变量，用于存储会话池对象，通过它可以获取会话来执行SQL语句。
    private SessionPool session;
    // 成员变量，用于存储需要执行的SQL语句。
    private String sql;

    // ExecuteSQL类的构造方法，接收一个SessionPool对象和SQL语句，然后初始化类的成员变量。
    public ExecuteSQL(SessionPool session, String sql) {
        this.session = session;
        this.sql = sql;
    }

    // 实现Runnable接口的run方法，定义执行SQL语句的逻辑。
    @Override
    public void run() {
        try {
            // 尝试执行非查询类型的SQL语句（如DDL或DML），该方法会返回一个影响的行数。
            session.executeNonQueryStatement(sql);
        } catch (StatementExecutionException e) {
            // 如果执行SQL语句时发生语句执行异常，打印出SQL语句，并将异常包装为运行时异常抛出。
            out.println(sql);
            throw new RuntimeException(e);
        } catch (IoTDBConnectionException e) {
            // 如果执行SQL语句时发生连接异常，打印出SQL语句，并将异常包装为运行时异常抛出。
            out.println(sql);
            throw new RuntimeException(e);
        }
    }
}