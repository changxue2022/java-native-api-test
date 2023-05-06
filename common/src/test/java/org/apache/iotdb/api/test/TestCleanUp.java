package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestCleanUp extends BaseTestSuite {
    private static final String basepath = "root.cleanup";
//    private static final String targetIP = "127.0.0.1";
    private static final String targetIP = "172.20.70.44";
    private static final int insertCount = 100000;
    private static List<TSDataType> tsDataTypeList = new ArrayList<>();

    @BeforeClass
    public void beforeClass() {
        tsDataTypeList.add(TSDataType.BOOLEAN);
        tsDataTypeList.add(TSDataType.INT32);
        tsDataTypeList.add(TSDataType.INT64);
        tsDataTypeList.add(TSDataType.FLOAT);
        tsDataTypeList.add(TSDataType.DOUBLE);
        tsDataTypeList.add(TSDataType.TEXT);
    }
    private void createTrigger() throws IoTDBConnectionException, StatementExecutionException {
        String sql = "CREATE STATELESS TRIGGER `normal01` AFTER INSERT ON "+basepath+".** AS 'org.example.DoubleValueMonitor' WITH (   'remote_ip'='" +
                targetIP+"',   'lo' = '10',   'hi' = '15.5' )";
        session.executeNonQueryStatement(sql);
    }
    private void testRunSingle(String device, String tsName, TSDataType tsDataType, int insertCount, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        String database = device.substring(0, device.lastIndexOf("."));
        if (!checkStroageGroupExists(database)) {
            session.setStorageGroup(database);
        }
        String path = device+"."+tsName;
        if (!isAligned) {
            session.createTimeseries(path, tsDataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        }
        insertTabletSingle(device, tsName, tsDataType, insertCount, isAligned);
        checkQueryResult("select count(*) from "+device, 1);
        countLines("show regions", true);
//        session.deleteTimeseries(path);
    }
    private void testRunMulti(String device, int columns, int lines, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        String database = device.substring(0, device.lastIndexOf("."));
        List< MeasurementSchema > schemaList = new ArrayList<>(columns);
        if (!checkStroageGroupExists(database)) {
            session.setStorageGroup(database);
        }
        for (int i = 0; i < columns; i++) {
            schemaList.add(new MeasurementSchema("s_"+i,
                    tsDataTypeList.get(new Random().nextInt(tsDataTypeList.size()))));
        }
        insertTabletMulti(device, schemaList, lines, isAligned);
    }

    @Test
    public void testSameNameTS_diffType() throws IoTDBConnectionException, StatementExecutionException {
        if (!checkStroageGroupExists(basepath)) {
            session.createDatabase(basepath);
        }
        String device = basepath + ".d1";
        String tsName = "s_1";
        boolean isAligned = false;
        testRunSingle(device, tsName, TSDataType.BOOLEAN, insertCount, isAligned);
        testRunSingle(device, tsName, TSDataType.INT32, insertCount, isAligned);
        testRunSingle(device, tsName, TSDataType.FLOAT, insertCount, isAligned);
        session.deleteStorageGroup(basepath);
    }
    @Test
    public void testChildDatabase() throws IoTDBConnectionException, StatementExecutionException {
        String database = basepath + ".child";
        String device = database + ".d1";
        String tsName = "s_1";
        boolean isAligned = false;
        if (!checkStroageGroupExists(database)) {
            session.setStorageGroup(database);
        }
        testRunSingle(device, tsName, TSDataType.INT32, insertCount, isAligned);
        testRunSingle(device, tsName, TSDataType.INT64, insertCount, isAligned);
        testRunSingle(device, tsName, TSDataType.DOUBLE, insertCount, isAligned);
        session.deleteStorageGroup(database);
    }
    @Test
    public void testMulti() throws IoTDBConnectionException, StatementExecutionException {
        String device = basepath + ".d1.d1";
        testRunMulti(device, 5, 10, false);
        session.deleteStorageGroup(basepath+".d1");

        device = basepath + ".sg1.d1";
        testRunMulti(device, 6, 10, true);
        session.deleteStorageGroup(basepath+".sg1");

    }
    @Test
    public void testSameNameSG() throws IoTDBConnectionException, StatementExecutionException {
        if (!checkStroageGroupExists(basepath)) {
            session.setStorageGroup(basepath);
        }
        String device = basepath + ".d1";
        String tsName = "s_1";
        boolean isAligned = true;
        testRunSingle(device, tsName, TSDataType.INT32, insertCount, isAligned);
        testRunSingle(device, tsName, TSDataType.FLOAT, insertCount, isAligned);
        session.deleteStorageGroup(basepath);
    }



}
