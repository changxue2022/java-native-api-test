package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static java.lang.System.out;

/**
 * insert
 * 1. 测试所有类型
 * 2. 测试boolean值的取值
 * 3. 测试数值类型的最大最小值
 * 4. 测试浮点型的精度（需要修改float_precision）
 * 5. 测试数值超限情况
 * 6. 测试时间戳各种格式
 */
public class TestInsertNormalWithoutCreate extends BaseTestSuite {
    private static final String device = "root.non_algined.d1";
    private static final String alignedDevice = "root.algined.d2";

    private final List<String> paths = new ArrayList<>(6);
    private final List<String> measurements = new ArrayList<>(6);
    private final List<TSDataType> dataTypes = new ArrayList<>(6);
    private final List<IMeasurementSchema> schemaList = new ArrayList<>();// tablet

    private final int expectCount = 18;
    public Boolean verbose = true;


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        cleanDatabases(verbose);
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

    }

    /**
     * 工具函数，用于查询比较 TS 中插入条数，清除已插入数据
     */
    private void afterMethod(int expectNonAligned, int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        if (expectNonAligned != -1) {
            int actualNonAligned = getRecordCount(device, verbose);
            out.println("actualNonAligned == expectNonAligned:"+actualNonAligned +"=="+ expectNonAligned);
            assert actualNonAligned == expectNonAligned-1 : "非对齐：" + msg;
            out.println("清理数据");
            session.deleteTimeseries(device+".**");
        }
        if (expectAligned != -1) {
            int actualAligned = getRecordCount(alignedDevice, verbose);
            out.println("actualAligned == expectAligned:" + actualAligned + "==" + expectAligned);
            assert actualAligned == expectAligned - 1 : "对齐：" + msg;
            out.println("清理数据");
            session.deleteTimeseries(alignedDevice + ".**");
        }
    }

    /**
     * 工具函数：检查一次插入多个设备的结果
     */
    private void checkInsertMultiDevices(String msg) throws IoTDBConnectionException, StatementExecutionException {
        int[] expectValueList = new int[]{2,1,5};
        for (int i = 0; i < 3; i++) {
            out.println("actual="+getRecordCount("root.jni.d"+(i+1), verbose));
            out.println("expect="+expectValueList[i]);
            assert getRecordCount("root.jni.d"+(i+1), verbose) == expectValueList[i] : "root.jni.d"+(i+1)+":" + msg;
        }
        out.println("清理数据");
        session.deleteTimeseries("root.jni.**");
    }

    @DataProvider(name="insertSingleNormal", parallel = true)
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-records.csv").getData();
    }
     @DataProvider(name="insertMultiRecords")
    public Iterator<Object[]> getMultiRecords() throws IOException {
        return new CustomDataProvider().load("data/insert-records-multi.csv").getData();
    }

    /**
     * tablet 工具函数:组成rowIndex行 某点数据
     */
    private void insertValue(int rowIndex, int colIndex, Tablet tablet, String value) {
        if ("null".equals(value)) {
            value = null;
            tablet.bitMaps[colIndex].mark(rowIndex);
        }
        switch(schemaList.get(colIndex).getType()) {
            case BOOLEAN:
                if (value == null) {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, GenerateValues.getBoolean());
                } else {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, Boolean.parseBoolean(value));
                }
                break;
            case INT32:
                if (value == null) {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, GenerateValues.getInt());
                } else {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, Integer.parseInt(value));
                }
                break;
            case INT64:
                if (value == null) {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, GenerateValues.getLong(10));
                } else {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, Long.parseLong(value));
                }
                break;
            case FLOAT:
                if (value == null) {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, GenerateValues.getFloat(2, 100, 200));
                } else {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, Float.parseFloat(value));
                }
                break;
            case DOUBLE:
                if (value == null) {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, GenerateValues.getDouble(2, 500, 1000));
                } else {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, Double.parseDouble(value));
                }
                break;
            case TEXT:
                if (value == null) {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, GenerateValues.getChinese());
                } else {
                    tablet.addValue(schemaList.get(colIndex).getMeasurementId(), rowIndex, value);
                }
                break;
        }
    }

    /**
     * insert tatblet 同设备
     * 非对齐/对齐
     */
    @Test(priority = 10)
    public void testInsertTablet() throws IOException, IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 100);
        tablet.initBitMaps();

        int rowIndex = 0;
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            rowIndex = tablet.rowSize++;
            Object[] line = it.next();
            tablet.addTimestamp(rowIndex, Long.valueOf((String)line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                insertValue(rowIndex, i, tablet, (String)line[i+1]);
            }
        }
        session.insertTablet(tablet);
        // 使用对齐方式插入非对齐tablet
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedTablet(tablet));
        /**
         * tablet变对齐设备
         */
        tablet.setDeviceId(alignedDevice);
        session.insertAlignedTablet(tablet);
        // 使用非对齐方法插入对齐tablet
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertTablet(tablet));
        tablet.reset();
        afterMethod(expectCount,expectCount, "insert tablet");
    }

    /**
     * insert null value with tablet
     */
    @Test(priority = 20)
    public void insertTabletWithNullValues() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 100);
        // Method 1 to add tablet data
        tablet.initBitMaps();

        long timestamp = 1672023281895L;
        for (long row = 0; row < schemaList.size(); row++) {
            int rowIndex = tablet.rowSize++;
            timestamp += 1000;
            tablet.addTimestamp(rowIndex, timestamp++);
            for (int s = 0; s < schemaList.size(); s++) {
                if (row == s) {
                    // mark null value
                    tablet.bitMaps[s].mark((int) row);
                }
                insertValue(rowIndex, s, tablet, null);
            }
        }

        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            Map tablet_map = new HashMap<>();
            tablet_map.put(device, tablet);
            session.insertTablets(tablet_map);

            tablet.setDeviceId(alignedDevice);
            session.insertAlignedTablet(tablet);
            tablet_map.clear();
            tablet_map.put(alignedDevice, tablet);
            session.insertAlignedTablets(tablet_map);
            tablet.reset();
        }
        afterMethod(schemaList.size(),schemaList.size(), "insert tablet with NULL value");
    }

    /**
     * insert tablets 同设备
     * 非对齐
     */
    @Test(priority = 30)
    public void testInsertTablets_1Device() throws IoTDBConnectionException, StatementExecutionException, IOException {
        Map<String, Tablet> tabletMap = new HashMap<>();
        int rowIndex = 0;
        Tablet tablet = new Tablet(device, schemaList, 100);
        tablet.initBitMaps();
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext();) {
            Object[] line =  it.next();
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                insertValue(rowIndex, i, tablet, (String)line[i+1]);
            }
            tabletMap.put(device, tablet);
        }
        session.insertTablets(tabletMap);
        afterMethod(expectCount,-1,"insert tablets non aligned");
    }
    @Test(priority = 40)
    public void testInsertTablets_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
        Map<String, Tablet> tabletMap = new HashMap<>();
        String d = "";
        Tablet tablet = null;
        int rowIndex = 0;
        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
            Object[] line =  it.next();
            if (!d.equals(line[1].toString())) {
                if (!d.isEmpty()) {
                    tabletMap.put(d, tablet);
                }
                tablet = new Tablet(line[1].toString(), schemaList, 10);
                d = line[1].toString();
                tablet.initBitMaps();
                rowIndex = 0;
            }
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                insertValue(rowIndex,i, tablet, (String)line[i+2]);
                if (line[i+2] == null) {
                    tablet.bitMaps[i].mark(rowIndex);
                }
            }
        }
        tabletMap.put(d, tablet);
        session.insertAlignedTablets(tabletMap);
        checkInsertMultiDevices("insertAlignedTablets_multiDevice");
    }
    /**
     * insert tables 同设备
     * 对齐
     */
    @Test(priority = 50)
    public void testInsertAlignedTablets_1Device() throws IoTDBConnectionException, StatementExecutionException, IOException {
        Map<String, Tablet> tabletMap = new HashMap<>();
        Tablet tablet = new Tablet(alignedDevice, schemaList, 20);
        int rowIndex = 0;
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext();) {
            Object[] line =  it.next();
            rowIndex = tablet.rowSize++;
            tablet.initBitMaps();
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                insertValue(rowIndex, i, tablet, (String)line[i+1]);
            }
            tabletMap.put(alignedDevice, tablet);
        }
        session.insertAlignedTablets(tabletMap);
        afterMethod(-1,expectCount,"insert tablets non aligned");
    }
    @Test(priority = 60)
    public void testInsertAlignedTablets_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
        Map<String, Tablet> tabletMap = new HashMap<>();
        String d = "";
        Tablet tablet = null;
        int rowIndex = 0;
        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
            Object[] line =  it.next();
            if (!d.equals(line[1].toString())) {
                if (!d.isEmpty()) {
                    tabletMap.put(d, tablet);
                }
                tablet = new Tablet(line[1].toString(), schemaList, 10);
                d = line[1].toString();
                tablet.initBitMaps();
                rowIndex = 0;
            }
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                insertValue(rowIndex, i, tablet, (String)line[i+2]);
                if (line[i+2] == null) {
                    tablet.bitMaps[i].mark(rowIndex);
                }
            }
        }
        tabletMap.put(d, tablet);
        session.insertAlignedTablets(tabletMap);
        checkInsertMultiDevices("insertAlignedTablets_multiDevice");
    }

    /**
     * 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据
     * 非对齐/对齐
     */
    @Test(dataProvider= "insertSingleNormal", priority = 100)
    public void testInsertRecord(String time, String s_boolean, String s_int, String s_long, String s_float, String s_double, String s_text) throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>();
        if (s_boolean == null) {
            values.add(null);
        } else {
            values.add(Boolean.parseBoolean(s_boolean));
        }
        if (s_int == null) {
            values.add(null);
        } else {
            values.add(Integer.parseInt(s_int));
        }
        if (s_long == null) {
            values.add(null);
        } else {
            values.add(Long.parseLong(s_long));
        }
        if (s_float == null) {
            values.add(null);
        } else {
            values.add(Float.parseFloat(s_float));
        }
        if (s_double == null) {
            values.add(null);
        } else {
            values.add(Double.parseDouble(s_double));
        }
        values.add(s_text);
        session.insertRecord(device, Long.valueOf(time), measurements, dataTypes, values);
        session.insertAlignedRecord(alignedDevice, Long.valueOf(time), measurements, dataTypes, values);
        if (time.equals("1669109398772")) { // 只做一次
            Assert.assertThrows(StatementExecutionException.class, ()-> session.insertRecord(alignedDevice, Long.valueOf(time), measurements, dataTypes, values));
            Assert.assertThrows(StatementExecutionException.class, ()->session.insertAlignedRecord(device, Long.valueOf(time), measurements, dataTypes, values));
        }
        values.clear();
    }

    @Test(priority = 101)
    void checkResult_insertRecord() throws IoTDBConnectionException, StatementExecutionException {
        afterMethod(expectCount,expectCount, "insertRecord 逐条写入");
    }
    /**
     * 插入同属于一个 device 的多个 Record: insertStringRecordsOfOneDevice
     * 非对齐/对齐
     */
    @Test(priority = 103)
    public void testInsertStringOneDevice() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<String>> valuesList = new ArrayList<>();
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            Object[] line = it.next();
            List<String> values = new ArrayList<>();

            times.add(Long.valueOf((String) line[0]));
            measurementsList.add(measurements);
            for (int i = 0; i < 5; i++) {
                if (line[i] == null) {
                    values.add(null);
                } else {
                    values.add(line[i].toString());
                }
            }
            valuesList.add(values);
        }
        session.insertStringRecordsOfOneDevice(device, times, measurementsList,valuesList);
        session.insertAlignedStringRecordsOfOneDevice(alignedDevice, times,measurementsList, valuesList);
        afterMethod(expectCount,expectCount, "StringRecordsOfOneDevice");
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedStringRecordsOfOneDevice(device, times, measurementsList,valuesList));
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertStringRecordsOfOneDevice(alignedDevice, times, measurementsList,valuesList));
    }
    private void formValues(ArrayList<Object> result, String ...values) {
        if (values[0] == null) {
            result.add(null);
        } else {
            result.add(Boolean.parseBoolean(values[0]));
        }
        if (values[1] == null) {
            result.add(null);
        } else {
            result.add(Integer.parseInt(values[1]));
        }
        if (values[2] == null) {
            result.add(null);
        } else {
            result.add(Long.parseLong(values[2]));
        }
        if (values[3] == null) {
            result.add(null);
        } else {
            result.add(Float.parseFloat(values[3]));
        }
        if (values[4] == null) {
            result.add(null);
        } else {
            result.add(Double.parseDouble(values[4]));
        }
        result.add(values[5]);
    }
    /**
     * insertRecordsOfOneDevice
     * 非对齐/对齐
     */
    @Test(priority = 104)
    public void testInsertRecordsOfOneDevice() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        List<List<TSDataType>> datatypeList = new ArrayList<>();
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            Object[] line = it.next();
            List<Object> values = new ArrayList<>();

            times.add(Long.valueOf((String) line[0]));
            measurementsList.add(measurements);
            for (int i = 1; i < 7; i++) {
                if (line[i] == null) {
                    values.add(null);
                } else {
                    values.add(line[i]);
                }
            }
            valuesList.add(values);
            datatypeList.add(dataTypes);
        }
        session.insertRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList);
        session.insertAlignedRecordsOfOneDevice(alignedDevice, times, measurementsList,datatypeList,valuesList);

        afterMethod(expectCount,expectCount, "插入同属于一个 device 的多个 Record");
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList));
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertRecordsOfOneDevice(alignedDevice, times, measurementsList,datatypeList,valuesList));
    }
    /**
     * insertRecords 1个设备
     * 非对齐
     */
    @Test(priority = 110)
    public void testInsertRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        List<List<TSDataType>> datatypeList = new ArrayList<>();
        List<String> deviceList = new ArrayList<>();
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            Object[] line = it.next();
            List<Object> values = new ArrayList<>();
            times.add(Long.valueOf((String) line[0]));
            deviceList.add(device);
            measurementsList.add(measurements);
            for (int i = 0; i < schemaList.size(); i++) {
                values.add(line[i+2]);
            }
            valuesList.add(values);
            datatypeList.add(dataTypes);
        }

        session.insertRecords(deviceList,times,measurementsList,datatypeList,valuesList);
        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedRecords(deviceList, times, measurementsList,datatypeList,valuesList));
    }
    @Test(priority = 120)
    public void testInsertRecords_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        List<List<TSDataType>> datatypeList = new ArrayList<>();
        List<String> deviceList = new ArrayList<>();

        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
            Object[] line =  it.next();
            times.add(Long.valueOf((String) line[0]));
            deviceList.add(line[1].toString());
            measurementsList.add(measurements);

            List<Object> values = new ArrayList<>();
            for (int i = 0; i < schemaList.size(); i++) {
                values.add(line[i+2]);
            }
            valuesList.add(values);
            datatypeList.add(dataTypes);
        }
        session.insertRecords(deviceList,times,measurementsList,datatypeList,valuesList);
        checkInsertMultiDevices("insertRecords_multiDevice");
    }
    /**
     * insertAlignedRecords
     * 对齐
     */
    @Test(dataProvider= "getSingleNormal", priority = 130)
    public void testinsertAlignedRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        List<List<TSDataType>> datatypeList = new ArrayList<>();
        List<String> deviceList = new ArrayList<>();
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            Object[] line = it.next();
            List<Object> values = new ArrayList<>();

            times.add(Long.valueOf((String) line[0]));
            measurementsList.add(measurements);
            for (int i = 0; i < 5; i++) {
                values.add(line[i]);
            }
            valuesList.add(values);
            datatypeList.add(dataTypes);
            deviceList.add(alignedDevice);
        }

        session.insertAlignedRecords(deviceList, times, measurementsList,datatypeList,valuesList);
        afterMethod(expectCount,expectCount,"insertAlignedRecords");
        Assert.assertThrows(StatementExecutionException.class,
                ()-> session.insertRecords(deviceList, times, measurementsList,datatypeList,valuesList));
    }
    @Test(priority = 140)
    public void testInsertAlignedRecords_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        List<List<TSDataType>> datatypeList = new ArrayList<>();
        List<String> deviceList = new ArrayList<>();

        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
            Object[] line =  it.next();
            times.add(Long.valueOf((String) line[0]));
            deviceList.add(line[1].toString());
            measurementsList.add(measurements);

            List<Object> values = new ArrayList<>();
            for (int i = 0; i < schemaList.size(); i++) {
                values.add(line[i+2]);
            }
            valuesList.add(values);
            datatypeList.add(dataTypes);
        }
        session.insertAlignedRecords(deviceList,times,measurementsList,datatypeList,valuesList);
        checkInsertMultiDevices("insertAlignedRecords multi device");
    }

}
