package org.apache.iotdb.api.test.utils;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.lang.System.out;

public class PrepareConnection {
    private static Session session = null;
    private static SessionPool sessionPool = null;
    private static ReadConfig config;

    static {
        try {
            config = ReadConfig.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static Session getSession() throws IoTDBConnectionException, IOException {
//        Session session = null;
        if ( session == null) {
            if (config.getValue("is_cluster").equals("true")) {
                String host_nodes_str = config.getValue("host_nodes");
                session = new Session.Builder()
                        .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                        .username(config.getValue("user"))
                        .password(config.getValue("password"))
                        .enableRedirection(false)
                        .timeOut(Long.parseLong(config.getValue("session_timeout")))
                        .build();
            } else {
                session = new Session.Builder()
                        .host(config.getValue("host"))
                        .port(Integer.parseInt(config.getValue("port")))
                        .username(config.getValue("user"))
                        .password(config.getValue("password"))
                        .enableRedirection(false)
                        .timeOut(Long.parseLong(config.getValue("session_timeout")))
                        .build();
            }
        }
        session.open(false);
        // set session fetchSize
        session.setFetchSize(10000);
        return session;
    }
    public static SessionPool getSessionPool() {
        if (sessionPool == null) {
            if (config.getValue("is_cluster").equals("true")) {
                String host_nodes_str = config.getValue("host_nodes");
                sessionPool = new SessionPool.Builder()
                        .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                        .user(config.getValue("user"))
                        .password(config.getValue("password"))
                        .maxSize(10)
                        .timeOut(Long.parseLong(config.getValue("session_timeout")))
                        .build();
            } else {
                sessionPool = new SessionPool.Builder()
                        .host(config.getValue("host"))
                        .port(Integer.parseInt(config.getValue("port")))
                        .user(config.getValue("user"))
                        .password(config.getValue("password"))
                        .maxSize(10)
                        .timeOut(Long.parseLong(config.getValue("session_timeout")))
                        .build();
            }
        }

        // set session fetchSize
        sessionPool.setFetchSize(10000);
        return sessionPool;
    }

    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        String ROOT_SG1_D1 = "root.multi.d1";
        String host="172.20.70.45";
        long timestamp = 601L;

    }
}
