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
 * 多线程执行创建 pipe 的 sql,可以独立运行
 */
public class MultiSQLConcurrent {
    private static int clientCount = 100;
    private static int loop = 10000;

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        String pattern = "root";
        String mode = "hybrid";
        String connector = "iotdb-thrift-connector";
        String targetHost = "172.20.70.15:6667";

        SessionPool session = new SessionPool.Builder()
                .host("iotdb-16")
                .user("root")
                .password("root")
                .build();
        long startTime = System.currentTimeMillis();
//        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        List<String> sqlList = new ArrayList<>();
        sqlList.add("create pipe test with extractor ('extractor'='iotdb-extractor','extractor.pattern'='"+pattern+"', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');");
        sqlList.add("start pipe test;");
        sqlList.add("drop pipe test;");

        for (int i=0; i<loop; i++) {
            out.println(i);
            for (String sql: sqlList) {
                try {
                    session.executeQueryStatement(sql);
                } catch (Exception e) {

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

class ExecuteSQL1 implements Runnable {
    private SessionPool session;
    private List<String> sqlList;
    public ExecuteSQL1(SessionPool session, List<String> sqlList) {
        this.session = session;
        this.sqlList = sqlList;
    }

    @Override
    public void run() {
        try {
            for (String sql: sqlList) {
                session.executeNonQueryStatement(sql);
            }
        } catch (StatementExecutionException e) {
            throw new RuntimeException(e);
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }
}