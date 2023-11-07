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
public class CreatePipeConcurrent {
    private static int clientCount = 100;
    private static int loop = 1000;

    public static void main(String[] args) {
        String pattern = "root.test.g_0.d_";
        String mode = "hybrid";
        String connector = "iotdb-thrift-connector";
        String targetHost = "172.20.70.32:6667";
        List<String> nodeUrls = new ArrayList<>(3);
        nodeUrls.add("iotdb-33:6667");
//        nodeUrls.add("iotdb-19:6667");
//        nodeUrls.add("iotdb-21:6667");
        SessionPool session = new SessionPool.Builder()
                .nodeUrls(nodeUrls)
                .maxSize(clientCount)
                .user("root")
                .password("root")
                .build();
        long startTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);

        for (int i = 0; i < loop; i++) {
//            String sql = "create pipe test with extractor ('extractor'='iotdb-extractor','extractor.pattern'='"+pattern+"', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');";
//            String sql = "create pipe pipe_s_0_d_"+i+" with extractor ('extractor'='iotdb-extractor','extractor.pattern'='"+pattern+i+".s_0', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');";
//            String sql = "create pipe pipe_"+i+" with extractor ('extractor'='iotdb-extractor','extractor.pattern'='"+pattern+i+"', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector.batch.enable'='false','connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');";
// pattern=root
//            String sql = "create pipe pipe_"+i+" with extractor ('extractor'='iotdb-extractor','extractor.pattern'='root', 'extractor.realtime.mode'='"+mode+"', 'extractor.forwarding-pipe-requests'='false') with connector ('connector'='"+connector+"', 'connector.node-urls'='"+targetHost+"');";
          String sql = "start pipe pipe_"+i;
//          String sql = "start pipe pipe_"+i;
//          String sql = "start pipe pipe_s_0_d_"+i;
//          String sql = "drop pipe pipe_s_0_d_"+i;
//          String sql = "drop pipe pipe_"+i+";";
            //            out.println(sql);
            pool.execute(new ExecuteSQL(session, sql));
//            pool.execute(new ExecuteSQL(session, "start pipe pipe_"+i+";"));
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
                break;
            }
        }
        session.close();
    }

}

class ExecuteSQL implements Runnable {
    private SessionPool session;
    private String sql;
    public ExecuteSQL(SessionPool session, String sql) {
        this.session = session;
        this.sql = sql;
    }

    @Override
    public void run() {
        try {
            session.executeNonQueryStatement(sql);
        } catch (StatementExecutionException e) {
            out.println(sql);
            throw new RuntimeException(e);
        } catch (IoTDBConnectionException e) {
            out.println(sql);
            throw new RuntimeException(e);
        }
    }
}