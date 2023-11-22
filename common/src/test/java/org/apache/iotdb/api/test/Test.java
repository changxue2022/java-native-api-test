package org.apache.iotdb.api.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class Test {
    // 172.20.31.2
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host("172.20.31.2")
                .port(6667)
                .enableRedirection(false)
                .build();
        session.open();
        List<String> deviceIds = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<List<String>> measurements = new ArrayList<>();
        List<List<TSDataType>> dataTypes = new ArrayList<>();
        List<List<Object>> valuesLists = new ArrayList<>();

        deviceIds.add("root.sg27");
        deviceIds.add("root.sg27");

        timestamps.add(1635232143960L);
        timestamps.add(1635232153960L);

        List<String> ms = new ArrayList<>();
        ms.add("s3");
        ms.add("===OPC接口机工作组0==TEST4");
        measurements.add(ms);
        measurements.add(ms);

        List<TSDataType> dataTypeList = new ArrayList<>();
        dataTypeList.add(TSDataType.INT32);
        dataTypeList.add(TSDataType.BOOLEAN);
        dataTypes.add(dataTypeList);
        dataTypes.add(dataTypeList);

        List<Object> values1 = new ArrayList<>();
        values1.add(11);
        values1.add(false);
        valuesLists.add(values1);
        List<Object> values2 = new ArrayList<>();
        values2.add(12);
        values2.add(true);
        valuesLists.add(values2);

        session.insertRecords(deviceIds, timestamps, measurements,dataTypes, valuesLists);
    }
}
