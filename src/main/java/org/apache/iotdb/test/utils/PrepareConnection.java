package org.apache.iotdb.test.utils;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.out;

public class PrepareConnection {
    private static Session session = null;
    public static Session getSession() throws IoTDBConnectionException, IOException {
        ReadConfig config = ReadConfig.getInstance();
        if ( session == null) {
            if (config.getValue("is_cluster").equals("true")) {
                String host_nodes_str = config.getValue("host_nodes");
                session = new Session.Builder()
                        .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                        .username(config.getValue("user"))
                        .password(config.getValue("password"))
                        .build();
            } else {
                session = new Session.Builder()
                        .host(config.getValue("host"))
                        .port(Integer.parseInt(config.getValue("port")))
                        .username(config.getValue("user"))
                        .password(config.getValue("password"))
                        .build();
            }
        }
        session.open(false);

        // set session fetchSize
        session.setFetchSize(10000);
        return session;
    }

    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        PrepareConnection.getSession();
        try (SessionDataSet dataSet =
                     session.executeQueryStatement("select s_0 from root.test.g_0.d_0", 20)) {
            System.out.println(dataSet.getColumnNames());
            dataSet.setFetchSize(1024); // default is 10000
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        }
//        ReadConfig config = ReadConfig.getInstance();
//        for (String s : config.getValue("host_nodes").split(",")){
//            out.println(s);
//        }
//        PrepareConnection.getSession().executeQueryStatement("show cluster");
    }
}
