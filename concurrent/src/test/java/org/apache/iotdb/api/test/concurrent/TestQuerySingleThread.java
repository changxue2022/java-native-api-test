package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static java.lang.System.out;

public class TestQuerySingleThread {
    private static int databaseCount = 1;
    private static int deviceCount = 100000;
    private static int sensorCount = 100;
    private static StringBuilder sqlBuilder = new StringBuilder();
    private static String sgPrefix = "sg_";
    private static String devicePrefix = "d_";
    private static String tsPrefix = "s_";

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, IOException {
        out.println("# single #######################################");
        Path path = Paths.get(TestQuerySingleThread.class.getResource("/").getPath(), "single.csv");
        File f = path.toFile();
        f.createNewFile();
        Charset charset = Charset.forName("UTF-8");
        BufferedWriter writer = Files.newBufferedWriter(Paths.get(f.getAbsolutePath()), charset, StandardOpenOption.APPEND);

        sqlBuilder.append("SELECT ");
        for (int i = 0; i < sensorCount; i++) {
            sqlBuilder.append(tsPrefix);
            sqlBuilder.append(i);
            sqlBuilder.append(",");
        }
        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length());
        sqlBuilder.append(" FROM root.");
        sqlBuilder.append(sgPrefix);

        Session session = new Session.Builder().host("iotdb-44").build();
        session.open(false);
        long st = System.currentTimeMillis();
        for (int i = 0; i < databaseCount; i++) {
            sqlBuilder.append(i);
            for (int j = 0; j < deviceCount; j++) {
                String sql = sqlBuilder.toString()+"."+devicePrefix+j;
                    long startTime = System.nanoTime();
                    session.executeQueryStatement(sql);
                    long elapseTime = System.nanoTime() - startTime;
                    writer.write(j+","+elapseTime);
                    writer.newLine();
            }
        }
        writer.close();
        out.println("查询耗费(s):"+(System.currentTimeMillis()-st)/1000);
    }
}
