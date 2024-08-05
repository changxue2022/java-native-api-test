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
 * 多线程执行创建 pipe 的 sql,可以独立运行，用于执行多SQL语句的并发测试
 */
public class MultiSQLConcurrent {
    // 定义客户端的数量。
    private static int clientCount = 100;
    // 定义循环的次数。
    private static int loop = 10000;

    // 主方法，程序的入口点。
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        // 定义设备路径的前缀。
        String pattern = "root";
        // 定义实时模式。
        String mode = "hybrid";
        // 定义连接器的名称。
        String connector = "iotdb-thrift-connector";
        // 定义目标主机的地址和端口。
        String targetHost = "172.20.70.15:6667";

        // 创建SessionPool对象，用于管理会话。
        SessionPool session = new SessionPool.Builder()
                .host("iotdb-16") // 设置IoTDB服务的主机名。
                .user("root")     // 设置用户名。
                .password("root")  // 设置密码。
                .build();         // 构建会话池。

        // 记录操作开始的时间。
        long startTime = System.currentTimeMillis();
        // 注释掉的线程池创建代码。
        // ExecutorService pool = Executors.newFixedThreadPool(clientCount);

        // 创建一个列表，用于存储需要执行的SQL语句。
        List<String> sqlList = new ArrayList<>();
        // 添加创建管道的SQL语句到列表。
        sqlList.add("create pipe test with extractor ('extractor'='iotdb-extractor','extractor.pattern'='"+pattern+"', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');");
        // 添加启动管道的SQL语句到列表。
        sqlList.add("start pipe test;");
        // 添加删除管道的SQL语句到列表。
        sqlList.add("drop pipe test;");

        // 外层循环，用于控制执行SQL语句的次数。
        for (int i=0; i<loop; i++) {
            // 打印当前循环的索引。
            out.println(i);
            // 内层循环，用于执行所有的SQL语句。
            for (String sql: sqlList) {
                try {
                    // 执行查询类型的SQL语句。
                    session.executeQueryStatement(sql);
                } catch (Exception e) {
                    // 如果执行过程中发生异常，这里选择不处理（捕捉但什么也不做）。
                }
            }
        }

//        for (int i = 0; i < loop; i++) {
////            String sql = "create pipe pipe_"+i+" with extractor ('extractor'='iotdb-extractor','extractor.pattern'='"+pattern+i+"', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');";
////          String sql = "start pipe pipe_"+i+";";
////          String sql = "drop pipe pipe_"+i+";";
//            //            out.println(sql);
//            pool.execute(new ExecuteSQL(session, sql));
//        }
//        pool.shutdown();
//        while (true) {//等待所有任务都执行结束
//            if (pool.isTerminated()) {//所有的子线程都结束了
//                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
//                break;
//            }
//        }
        session.close();
    }

}

// 实现了Runnable接口，意味着可以在一个线程中执行。
class ExecuteSQL1 implements Runnable {
    // 成员变量，用于存储SessionPool对象，该对象管理着与IoTDB服务器的会话。
    private SessionPool session;
    // 成员变量，用于存储SQL语句的列表。
    private List<String> sqlList;

    // ExecuteSQL1类的构造方法，接收一个SessionPool对象和包含SQL语句的列表，然后初始化类的成员变量。
    public ExecuteSQL1(SessionPool session, List<String> sqlList) {
        this.session = session;
        this.sqlList = sqlList;
    }

    // 实现Runnable接口的run方法，定义了执行SQL语句的逻辑。
    @Override
    public void run() {
        try {
            // 遍历sqlList中的每个SQL语句。
            for (String sql: sqlList) {
                // 执行非查询类型的SQL语句，如插入或更新操作。
                session.executeNonQueryStatement(sql);
            }
        } catch (StatementExecutionException e) {
            // 如果在执行SQL语句时发生语句执行异常，将异常包装为运行时异常并抛出。
            throw new RuntimeException(e);
        } catch (IoTDBConnectionException e) {
            // 如果在执行SQL语句时发生连接异常，将异常包装为运行时异常并抛出。
            throw new RuntimeException(e);
        }
    }
}