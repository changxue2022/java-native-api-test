package org.apache.iotdb.api.test.utils;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
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
                        .enableRedirection(false)
                        .build();
            }
        }
        session.open(false);

        // set session fetchSize
        session.setFetchSize(10000);
        return session;
    }

    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        String ROOT_SG1_D1 = "root.multi.d1";
        String host="172.20.70.45";
        long timestamp = 601L;
        // 写入行数
        int maxCount = 500;
        Session session = new Session.Builder()
                .host("172.20.70.44")
                .port(6667)
                .username("root")
                .password("root")
//                .enableRedirection(false)
                .build();
        session.open(false);
        session.setFetchSize(10000);
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s3", TSDataType.DOUBLE));

        Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 2000000);

        for (int row = 0; row < maxCount; row++) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp);
            for (int s = 0; s < 3; s++) {
                double value = new Random().nextDouble() * 730;
                tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
            }
            timestamp++;
        }
        out.println("max:"+tablet.getMaxRowNumber());
        out.println(timestamp);

        session.insertTablet(tablet);
//        PrepareConnection.getSession();
//        try (SessionDataSet dataSet =
//                     session.executeQueryStatement("show cluster", 20)) {
//            System.out.println(dataSet.getColumnNames());
//            dataSet.setFetchSize(1024); // default is 10000
//            while (dataSet.hasNext()) {
//                System.out.println(dataSet.next());
//            }
//        }
//
//        List<String> measuraments = new ArrayList<>();
//        List<TSDataType> tsDataTypes = new ArrayList<>();
//        List<Object> content = new ArrayList<>();
//        String targetDevice = "root.ln.alerting";
//        measuraments.add("table_name");
//        measuraments.add("alert_content");
//        tsDataTypes.add(TSDataType.TEXT);
//        tsDataTypes.add(TSDataType.TEXT);
//
//        content.add("root.ln.wf01.wt01");
//        content.add("CRITICAL test value greater then 100");
//        session.insertAlignedRecord(targetDevice, new Date().getTime(), measuraments, tsDataTypes, content);

//        ReadConfig config = ReadConfig.getInstance();
//        for (String s : config.getValue("host_nodes").split(",")){
//            out.println(s);
//        }
//        PrepareConnection.getSession().executeQueryStatement("show cluster");
    }
}
