package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;

public class TestConcurrentQuery {
    private static int databaseCount = 1;
    private static int deviceCount = 100000;
    private static int sensorCount = 100;
    private static int clientCount = 2;
    private static int queryCount = 30000; //29733.33
    private static SessionPool sessionPool;
    private static List<String> hostList = new ArrayList<>(3);
    private static StringBuilder sqlBuilder = new StringBuilder();
    private static String sgPrefix = "sg_";
    private static String devicePrefix = "d_";
    private static String tsPrefix = "s_";

    static {
        hostList.add("iotdb-44:6667");
        hostList.add("iotdb-45:6667");
        hostList.add("iotdb-46:6667");
        sqlBuilder.append("SELECT ");
        for (int i = 0; i < sensorCount; i++) {
            sqlBuilder.append(tsPrefix);
            sqlBuilder.append(i);
            sqlBuilder.append(",");
        }
        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length());
        sqlBuilder.append(" FROM root.");
        sqlBuilder.append(sgPrefix);
    }

    public static void main(String[] args) {
        sessionPool = new SessionPool.Builder()
                        .nodeUrls(hostList)
                        .user("root")
                        .password("root")
                        .maxSize(clientCount)
                        .build();
        out.println("# multi ###################################");
        out.println("database="+databaseCount);
        out.println("deviceCount="+deviceCount);
        out.println("sensorCount="+sensorCount);
        out.println("clientCount="+clientCount);
        out.println("queryCount="+queryCount);
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);

        long startTime = System.currentTimeMillis();
        for (int j = 0; j < databaseCount; j++) {
            sqlBuilder.append(j);
            for (int i = 0; i < deviceCount ; i++) {
                pool.execute(new SessionClientQuery(sessionPool, sqlBuilder.toString()+"."+devicePrefix+i, devicePrefix+i));
            }
            sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length());
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                System.out.printf("共耗时: %f s", (System.currentTimeMillis()-startTime)/1000.0);
                break;
            }
        }
        sessionPool.close();
    }
}

class SessionClientQuery implements Runnable {
    private SessionPool sessionPool;
    private String sql;
    private String label;

    public SessionClientQuery(SessionPool sessionPool, String sql, String label) {
        this.sessionPool = sessionPool;
        this.sql = sql;
        this.label = label;
    }

    @Override
    public void run()  {
        try {
            long startTime = System.nanoTime();
            sessionPool.executeQueryStatement(sql);
            long elapseTime = System.nanoTime() - startTime;
            out.println(Thread.currentThread().getName()+" cost:"+elapseTime);
        } catch (IoTDBConnectionException ex) {
            out.println(Thread.currentThread().getName()+" "+ex);
        } catch (StatementExecutionException ex) {
            out.println(Thread.currentThread().getName()+" "+ex);
        }
    }
}