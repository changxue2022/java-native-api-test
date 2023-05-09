package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static java.lang.System.out;

/**
 * insert error test cases
 * 1. 测试所有类型
 * 2. 测试boolean值的取值
 * 3. 测试数值类型的最大最小值
 * 4. 测试浮点型的精度（需要修改float_precision）
 * 5. 测试数值超限情况
 * 6. 测试时间戳各种格式
 */
public class TestInsertError extends BaseTestSuite {
    private static final String device = "root.non_algined.d1";
    private static final String alignedDevice = "root.algined.d2";

    private final List<String> paths = new ArrayList<>(6);
    private final List<String> measurements = new ArrayList<>(6);
    private final List<TSDataType> dataTypes = new ArrayList<>(6);
    private final List<MeasurementSchema> schemaList = new ArrayList<>();// tablet


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        if (checkStroageGroupExists("")) {
            session.deleteStorageGroup("root.**");
        }
        out.println("创建序列");
        session.setStorageGroup(device.substring(0,device.lastIndexOf('.')));
        session.setStorageGroup(alignedDevice.substring(0,alignedDevice.lastIndexOf('.')));
        measurements.add("s_boolean");
        measurements.add("s_int");
        measurements.add("s_long");
        measurements.add("s_float");
        measurements.add("s_double");
        measurements.add("s_text");

        dataTypes.add(TSDataType.BOOLEAN);
        dataTypes.add(TSDataType.INT32);
        dataTypes.add(TSDataType.INT64);
        dataTypes.add(TSDataType.FLOAT);
        dataTypes.add(TSDataType.DOUBLE);
        dataTypes.add(TSDataType.TEXT);

        paths.add(device + "s_boolean");
        paths.add(device + "s_int");
        paths.add(device + "s_long");
        paths.add(device + "s_float");
        paths.add(device + "s_double");
        paths.add(device + "s_text");

        schemaList.add(new MeasurementSchema("s_boolean", TSDataType.BOOLEAN));
        schemaList.add(new MeasurementSchema("s_int", TSDataType.INT32));
        schemaList.add(new MeasurementSchema("s_long", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s_float", TSDataType.FLOAT));
        schemaList.add(new MeasurementSchema("s_double", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s_text", TSDataType.TEXT));

        session.createMultiTimeseries(paths,dataTypes,
                new ArrayList<>(),new ArrayList<>(),
                null,null,null, null);
        session.createAlignedTimeseries(alignedDevice, measurements, dataTypes,
                null, null, null);

    }

    /**
     * 工具函数，用于查询比较 TS 中插入条数，清除已插入数据
     */
    public void afterMethod(int expectNonAligned, int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        int actualNonAligned = getRecordCount(device, verbose);
        int actualAligned = getRecordCount(alignedDevice, verbose);
        assert actualNonAligned == expectNonAligned : "非对齐：" + msg;
        assert actualAligned == expectAligned : "对齐：" + msg;

        out.println("清理数据");
        session.deleteData(paths, new Date().getTime());
        session.deleteData(alignedDevice, new Date().getTime());
    }

    @DataProvider(name="insertSingleError")
    public Iterator<Object[]> getSingleError() throws IOException {
        return new CustomDataProvider().load("data/insert-records-error.csv").getData();
    }


    /**
     * 单条 insertTablet error
     * 非对齐
     */
    @Test(dataProvider= "insertSingleError", expectedExceptions = StatementExecutionException.class, priority = 50)
    public void testInsertTablet_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 1);
        int rowIndex = 0;
        tablet.addTimestamp(rowIndex, Long.valueOf(time));
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, s_boolean);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, s_int);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, s_long);
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, s_float);
        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, s_double);
        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, s_text);
        session.insertTablet(tablet);
    }
    /**
     * 单条 insertTablet error
     * 对齐
     */
    @Test(dataProvider= "insertSingleError", expectedExceptions = StatementExecutionException.class, priority = 51)
    public void testInsertAlignedTablet_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
        int rowIndex = 0;
        tablet.addTimestamp(rowIndex, Long.valueOf(time));
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, s_boolean);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, s_int);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, s_long);
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, s_float);
        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, s_double);
        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, s_text);
        session.insertAlignedTablet(tablet);
    }
    @Test(priority = 52)
    public void checkResult_insertTablet() throws IoTDBConnectionException, StatementExecutionException {
        afterMethod(0,0,"Error insert tablet");
    }
    @Test
    public void testInsertTablets_error() throws IOException, IoTDBConnectionException, StatementExecutionException {
        Map<String, Tablet> tabletMap = new HashMap<>();
        for (Iterator<Object[]> it = getSingleError(); it.hasNext();) {
            Object[] line =  it.next();
            Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
            int rowIndex = 0;
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+1]);
            }
            tabletMap.put(alignedDevice, tablet);
        }
        session.insertTablets(tabletMap);
        afterMethod(0,0,"insert tablets non aligned");
    }

    @Test
    public void testInsertAlignedTablets_error() throws IOException, IoTDBConnectionException, StatementExecutionException {
        Map<String, Tablet> tabletMap = new HashMap<>();
        for (Iterator<Object[]> it = getSingleError(); it.hasNext();) {
            Object[] line =  it.next();
            Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
            int rowIndex = 0;
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+1]);
            }
            tabletMap.put(alignedDevice, tablet);
        }
        session.insertAlignedTablets(tabletMap);
        afterMethod(0,0,"insert tablets non aligned");
    }

    @Test(dataProvider= "getSingleError", expectedExceptions = StatementExecutionException.class, priority = 102)
    public void testInsertRecord_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>();
        values.add(s_boolean);
        values.add(s_int);
        values.add(s_long);
        values.add(s_float);
        values.add(s_double);
        values.add(s_text);
        session.insertRecord(device, Long.valueOf(time), measurements, dataTypes, values);
        values.clear();
    }
    @Test(dataProvider= "getSingleError", expectedExceptions = StatementExecutionException.class, priority = 102)
    public void testInsertAlignedRecord_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>();
        values.add(s_boolean);
        values.add(s_int);
        values.add(s_long);
        values.add(s_float);
        values.add(s_double);
        values.add(s_text);
        session.insertAlignedRecord(alignedDevice, Long.valueOf(time), measurements, dataTypes, values);
        values.clear();
    }

    /**
     * 单条 insertTablet error
     * 非对齐
     */
    @Test(priority = 103)
    public void testInsertTablet_alignedDevice() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
        int rowIndex = 0;
        tablet.addTimestamp(rowIndex, 1672023281895L);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, true);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, 33);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 104L);
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, 12.54f);
        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, 204.39);
        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, "insert aligned device with insertTablet");

        Assert.assertThrows(StatementExecutionException.class, ()->{session.insertTablet(tablet);});
    }
    /**
     * 单条 insertTablet error
     * 对齐
     */
    @Test(priority = 104)
    public void testInsertAlignedTablet_nonAlignedDevice() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 1);
        int rowIndex = 0;
        tablet.addTimestamp(rowIndex, 1672023281895L);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, false);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, 11);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 305L);
        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, 45.55f);
        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, 303.78);
        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, "insert non-aligned device with insertAlignedTablet");
        Assert.assertThrows(StatementExecutionException.class, ()->{session.insertAlignedTablet(tablet);});
    }
    @Test(priority = 105)
    public void checkResult_insertTablet2() throws IoTDBConnectionException, StatementExecutionException {
        afterMethod(0,0, "aligned and non-aligned of insertTablet");
    }

    @Test(priority = 106)
    public void testInsertRecord_alignedDevice() throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>();
        values.add(true);
        values.add(20);
        values.add(300L);
        values.add(35.55f);
        values.add(336.77);
        values.add("insert non-aligned device with insertRecord");
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.insertRecord(alignedDevice, 1672023281895L, measurements, dataTypes, values);
        });
        values.clear();
    }
    @Test(priority = 107)
    public void testInsertAlignedRecord_nonAlignedDevice() throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>();
        values.add(20);
        values.add(300L);
        values.add(35.55f);
        values.add(336.77);
        values.add("insert non-aligned device with insertAlignedRecord");
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.insertAlignedRecord(device, 1672023281895L, measurements, dataTypes, values);
        });
        values.clear();
    }

    @Test(priority = 108)
    public void checkResult_insertRecord() throws IoTDBConnectionException, StatementExecutionException {
        afterMethod(0,0, "aligned and non-aligned of insertRecord");
    }

}
