package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
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
public class TestInsert extends BaseTestSuite {
    // 数据库名称
    private static final String database = "root.testInsert";
    // 设备名称
    private static final String device = database + ".d1";
    // 对齐设备名称
    private static final String alignedDevice = database + ".d2";
    // 物理量类型信息
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(6);
    // 存储路径
    private final List<String> paths = new ArrayList<>(6);
    // 物理量
    private final List<String> measurements = new ArrayList<>(6);
    // 数据类型
    private final List<TSDataType> dataTypes = new ArrayList<>(6);
    // 物理量模板
    private final List<MeasurementSchema> schemaList = new ArrayList<>();// tablet
    // 预期的记录条数
    private final int expectCount = 17;

    // 在测试类之前准备好环境（数据库、时间序列）
    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        // 检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        // 创建数据库
        session.createDatabase(database);
        // 添加不同的数据类型
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int", TSDataType.INT32);
        measureTSTypeInfos.put("s_long", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        // 遍历measureTSTypeInfos，为时间序列添加路径、测量和数据类型
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(device + "." + key);
            measurements.add(key);
            dataTypes.add(value);
            schemaList.add(new MeasurementSchema(key, value, TSEncoding.PLAIN, CompressionType.GZIP));
        });
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(6);
        List<CompressionType> compressionTypes = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 创建多个非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes,
                encodings, compressionTypes,
                null, null, null, null);
        // 创建对齐时间序列
        session.createAlignedTimeseries(alignedDevice, measurements, dataTypes,
                encodings, compressionTypes, null);

    }

    // 在测试类之后执行的删除数据库
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        // 删除数据库
        session.deleteDatabase(database);
        out.println("删除数据库"+database);
    }

    /**show
     * 用于查询比较对齐和非对齐插入条数看是否和预期相同
     *
     * @param expectNonAligned 预期非对齐时间序列的记录条数
     * @param expectAligned    预期对齐时间序列的记录条数
     * @param msg              断言失败时的消息
     */
    public void afterMethod(int expectNonAligned, int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非对齐时间序列的实际记录条数(不含null值的)
        int actualNonAligned = getRecordCount(device, verbose);
        // 获取对齐时间序列的实际记录条数
        int actualAligned = getRecordCount(alignedDevice, verbose);
        // 断言实际记录条数与预期相等
        assert actualNonAligned == expectNonAligned : "非对齐：" + msg + " actual=" + actualNonAligned + " expect=" + expectNonAligned;
        assert actualAligned == expectAligned : "对齐：" + msg + " actual=" + actualAligned + " expect=" + expectAligned;;
        // 打印清理数据的消息
        out.println("清理数据");
        // 清理非对齐时间序列的数据
        session.deleteData(device, new Date().getTime());
        // 清理对齐时间序列的数据
        session.deleteData(alignedDevice, new Date().getTime());
    }

    /**
     * 工具函数：检查一次插入多个设备的结果
     *
     * @param msg 断言失败时的消息
     */
    public void checkInsertMultiDevices(String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 定义预期的记录条数列表
        int[] expectValueList = new int[]{2, 1, 6};
        // 检查每个设备的实际记录条数是否与预期相符
        for (int i = 0; i < 3; i++) {
            assert getRecordCount("root.jni.d" + (i + 1), true) == expectValueList[i] : "root.jni.d" + (i + 1) + ":" + msg;
        }
        // 打印清理数据的消息
        out.println("清理数据");
        // 清理所有时间序列数据
        session.deleteTimeseries("root.jni.**");
    }

    // 获取正确的数据
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-records.csv").getData();
    }

    // 获取错误的数据
    @DataProvider(name = "insertSingleError")
    public Iterator<Object[]> getSingleError() throws IOException {
        return new CustomDataProvider().load("data/insert-records-error.csv").getData();
    }

    // 用于提供多条记录的数据
    // @DataProvider(name="insertMultiRecords")
    public Iterator<Object[]> getMultiRecords() throws IOException {
        return new CustomDataProvider().load("data/insert-records-multi.csv").getData();
    }

    /**
     * 测试插入tablet到同一设备中
     * 包括非对齐和对齐两种情况
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void testInsertTablet() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 创建一个新的tablet实例
        Tablet tablet = new Tablet(device, schemaList, 20);
        // 初始化bitmap，用于标记null值
        tablet.initBitMaps();
        // 行索引初始化为0
        int rowIndex = 0;

        // 遍历获取的单行数据
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            Object[] line = it.next();
            rowIndex = tablet.rowSize++;
            // 向tablet添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            out.println("########### " + (rowIndex + 1) + ":" + line[0]); // 打印行索引和时间戳
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
                out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
                out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值

                // 处理null值
                if (line[i + 1] == null) {
                    out.println("process null value");
                    tablet.bitMaps[i].mark(rowIndex); // 使用bitmap标记null值
                }

                // 根据数据类型添加值到tablet
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN: // 布尔类型
                        // 添加布尔值，如果为null则添加默认值false
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32: // 32位整数类型
                        // 添加整数值，如果为null则添加默认值1
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64: // 64位整数类型
                        // 添加长整数值，如果为null则添加默认值1L
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT: // 浮点类型
                        // 添加浮点数值，如果为null则添加默认值1.01f
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE: // 双精度浮点类型
                        // 添加双精度浮点数值，如果为null则添加默认值1.0
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT: // 文本类型
                        // 添加文本值，如果为null则添加默认值"stringnull"
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                }
            }
        }

        // 将创建的tablet插入到会话中
        session.insertTablet(tablet);

        // 执行SQL查询并计算行数
        int countLines = countLines("select * from " + device, verbose);

        // 注释掉的代码，原意是测试非对齐方式插入对齐tablet时应该抛出异常
        // Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedTablet(tablet));

        // 将tablet的设备ID设置为对齐设备ID
        tablet.setDeviceId(alignedDevice);
        // 插入对齐tablet
        session.insertAlignedTablet(tablet);

        // 注释掉的代码，原意是测试使用非对齐方法插入对齐tablet时应该抛出异常
        // Assert.assertThrows(StatementExecutionException.class, ()-> session.insertTablet(tablet));

        // 重置tablet状态
        tablet.reset();

        // 对比是否操作成功
        afterMethod(expectCount, expectCount, "insert tablet");
    }

//    /**
//     * insert null value with tablet
//     */
//    @Test(priority = 20)
//    public void insertTabletWithNullValues() throws IoTDBConnectionException, StatementExecutionException {
//        Tablet tablet = new Tablet(device, schemaList, 10);
//        // Method 1 to add tablet data
//        tablet.initBitMaps();
//
//        long timestamp = 1672023281895L;
//        for (long row = 0; row < schemaList.size(); row++) {
//            int rowIndex = tablet.rowSize++;
//            tablet.addTimestamp(rowIndex, timestamp++);
//            for (int s = 0; s < schemaList.size(); s++) {
//                if (row == s) {
//                    // mark null value
//                    tablet.bitMaps[s].mark((int) row);
//                }
//                tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, 0);
//            }
//            if (tablet.rowSize == tablet.getMaxRowNumber()) {
//                session.insertTablet(tablet, true);
//                tablet.reset();
//            }
//        }
//
//        if (tablet.rowSize != 0) {
//            session.insertTablet(tablet);
//            Map tablet_map = new HashMap<>();
//            tablet_map.put(device, tablet);
//            session.insertTablets(tablet_map);
//
//            tablet.setDeviceId(alignedDevice);
//            session.insertAlignedTablet(tablet);
//            tablet_map.clear();
//            tablet_map.put(alignedDevice, tablet);
//            session.insertAlignedTablets(tablet_map);
//            tablet.reset();
//        }
//        afterMethod(schemaList.size(),schemaList.size(), "insert tablet with NULL value");
//    }
//
//    /**
//     * insert tablets 同设备
//     * 非对齐
//     */
//    @Test(priority = 30)
//    public void testInsertTablets_1Device() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        Map<String, Tablet> tabletMap = new HashMap<>();
//        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext();) {
//            Object[] line =  it.next();
//            Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
//            int rowIndex = 0;
//            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            for (int i = 0; i < schemaList.size(); i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+1]);
//            }
//            tabletMap.put(device, tablet);
//        }
//        session.insertTablets(tabletMap);
//        afterMethod(expectCount,expectCount,"insert tablets non aligned");
//    }
//    @Test(priority = 40)
//    public void testInsertTablets_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        Map<String, Tablet> tabletMap = new HashMap<>();
//        String d = "";
//        Tablet tablet = null;
//        int rowIndex = 0;
//        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
//            Object[] line =  it.next();
//            if (!d.equals(line[1].toString())) {
//                if (!d.isEmpty()) {
//                    tabletMap.put(d, tablet);
//                }
//                tablet = new Tablet(line[1].toString(), schemaList, 10);
//                d = line[1].toString();
//                tablet.initBitMaps();
//                rowIndex = 0;
//            }
//            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            for (int i = 0; i < schemaList.size(); i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+2]);
//                if (line[i+2] == null) {
//                    tablet.bitMaps[i].mark(rowIndex);
//                }
//            }
//            rowIndex++;
//        }
//        session.insertAlignedTablets(tabletMap);
//        checkInsertMultiDevices("insertAlignedTablets_multiDevice");
//    }
//    /**
//     * insert tables 同设备
//     * 对齐
//     */
//    @Test(priority = 50)
//    public void testInsertAlignedTablets_1Device() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        Map<String, Tablet> tabletMap = new HashMap<>();
//        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext();) {
//            Object[] line =  it.next();
//            Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
//            int rowIndex = 0;
//            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            for (int i = 0; i < schemaList.size(); i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+1]);
//            }
//            tabletMap.put(alignedDevice, tablet);
//        }
//        session.insertAlignedTablets(tabletMap);
//        afterMethod(expectCount,expectCount,"insert tablets non aligned");
//    }
//    @Test(priority = 60)
//    public void testInsertAlignedTablets_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        Map<String, Tablet> tabletMap = new HashMap<>();
//        String d = "";
//        Tablet tablet = null;
//        int rowIndex = 0;
//        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
//            Object[] line =  it.next();
//            if (!d.equals(line[1].toString())) {
//                if (!d.isEmpty()) {
//                    tabletMap.put(d, tablet);
//                }
//                tablet = new Tablet(line[1].toString(), schemaList, 10);
//                d = line[1].toString();
//                tablet.initBitMaps();
//                rowIndex = 0;
//            }
//            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            for (int i = 0; i < schemaList.size(); i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+2]);
//                if (line[i+2] == null) {
//                    tablet.bitMaps[i].mark(rowIndex);
//                }
//            }
//            rowIndex++;
//        }
//        session.insertAlignedTablets(tabletMap);
//        checkInsertMultiDevices("insertAlignedTablets_multiDevice");
//    }
//
//    /**
//     * 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据
//     * 非对齐/对齐
//     */
//    @Test(dataProvider= "getSingleNormal", priority = 100)
//    public void testInsertRecord(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
//        List<Object> values = new ArrayList<>();
//        values.add(s_boolean);
//        values.add(s_int);
//        values.add(s_long);
//        values.add(s_float);
//        values.add(s_double);
//        values.add(s_text);
//        session.insertRecord(device, Long.valueOf(time), measurements, dataTypes, values);
//        session.insertAlignedRecord(alignedDevice, Long.valueOf(time), measurements, dataTypes, values);
//        if (time.equals("1669109398772")) { // 只做一次
//            Assert.assertThrows(StatementExecutionException.class, ()-> session.insertRecord(alignedDevice, Long.valueOf(time), measurements, dataTypes, values));
//            Assert.assertThrows(StatementExecutionException.class, ()->session.insertAlignedRecord(device, Long.valueOf(time), measurements, dataTypes, values));
//        }
//        values.clear();
//    }
//
//    @Test(priority = 101)
//    void checkResult_insertRecord() throws IoTDBConnectionException, StatementExecutionException {
//        afterMethod(expectCount,expectCount, "insertRecord 逐条写入");
//    }
//    /**
//     * 插入同属于一个 device 的多个 Record: insertStringRecordsOfOneDevice
//     * 非对齐/对齐
//     */
//    @Test(priority = 103)
//    public void testInsertStringOneDevice() throws IOException, IoTDBConnectionException, StatementExecutionException {
//        List<Long> times = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<String>> valuesList = new ArrayList<>();
//        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
//            Object[] line = it.next();
//            List<String> values = new ArrayList<>();
//
//            times.add(Long.valueOf((String) line[0]));
//            measurementsList.add(measurements);
//            for (int i = 0; i < 5; i++) {
//                values.add(line[i].toString());
//            }
//            valuesList.add(values);
//        }
//        session.insertStringRecordsOfOneDevice(device, times, measurementsList,valuesList);
//        session.insertAlignedStringRecordsOfOneDevice(alignedDevice, times,measurementsList, valuesList);
//        afterMethod(expectCount,expectCount, "StringRecordsOfOneDevice");
//        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedStringRecordsOfOneDevice(device, times, measurementsList,valuesList));
//        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertStringRecordsOfOneDevice(alignedDevice, times, measurementsList,valuesList));
//    }
//    /**
//     * insertRecordsOfOneDevice
//     * 非对齐/对齐
//     */
//    @Test(priority = 104)
//    public void testInsertRecordsOfOneDevice() throws IOException, IoTDBConnectionException, StatementExecutionException {
//        List<Long> times = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<Object>> valuesList = new ArrayList<>();
//        List<List<TSDataType>> datatypeList = new ArrayList<>();
//        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
//            Object[] line = it.next();
//            List<Object> values = new ArrayList<>();
//
//            times.add(Long.valueOf((String) line[0]));
//            measurementsList.add(measurements);
//            for (int i = 0; i < 5; i++) {
//                values.add(line[i]);
//            }
//            valuesList.add(values);
//            datatypeList.add(dataTypes);
//        }
//        session.insertRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList);
//        session.insertAlignedRecordsOfOneDevice(alignedDevice, times, measurementsList,datatypeList,valuesList);
//
//        afterMethod(expectCount,expectCount, "插入同属于一个 device 的多个 Record");
//        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList));
//        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertRecordsOfOneDevice(alignedDevice, times, measurementsList,datatypeList,valuesList));
//    }
//    /**
//     * insertRecords 1个设备
//     * 非对齐
//     */
//    @Test(priority = 110)
//    public void testInsertRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
//        List<Long> times = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<Object>> valuesList = new ArrayList<>();
//        List<List<TSDataType>> datatypeList = new ArrayList<>();
//        List<String> deviceList = new ArrayList<>();
//        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
//            Object[] line = it.next();
//            List<Object> values = new ArrayList<>();
//            times.add(Long.valueOf((String) line[0]));
//            deviceList.add(device);
//            measurementsList.add(measurements);
//            for (int i = 0; i < schemaList.size(); i++) {
//                values.add(line[i+2]);
//            }
//            valuesList.add(values);
//            datatypeList.add(dataTypes);
//        }
//
//        session.insertRecords(deviceList,times,measurementsList,datatypeList,valuesList);
//        Assert.assertThrows(StatementExecutionException.class, ()-> session.insertAlignedRecords(deviceList, times, measurementsList,datatypeList,valuesList));
//    }
//    @Test(priority = 120)
//    public void testInsertRecords_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        List<Long> times = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<Object>> valuesList = new ArrayList<>();
//        List<List<TSDataType>> datatypeList = new ArrayList<>();
//        List<String> deviceList = new ArrayList<>();
//
//        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
//            Object[] line =  it.next();
//            times.add(Long.valueOf((String) line[0]));
//            deviceList.add(line[1].toString());
//            measurementsList.add(measurements);
//
//            List<Object> values = new ArrayList<>();
//            for (int i = 0; i < schemaList.size(); i++) {
//                values.add(line[i+2]);
//            }
//            valuesList.add(values);
//            datatypeList.add(dataTypes);
//        }
//        session.insertRecords(deviceList,times,measurementsList,datatypeList,valuesList);
//        checkInsertMultiDevices("insertRecords_multiDevice");
//    }
//    /**
//     * insertAlignedRecords
//     * 对齐
//     */
//    @Test(priority = 126)
//    public void testinsertAlignedRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
//        List<Long> times = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<Object>> valuesList = new ArrayList<>();
//        List<List<TSDataType>> datatypeList = new ArrayList<>();
//        List<String> deviceList = new ArrayList<>();
//        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
//            Object[] line = it.next();
//            List<Object> values = new ArrayList<>();
//
//            times.add(Long.valueOf((String) line[0]));
//            measurementsList.add(measurements);
//            for (int i = 0; i < 5; i++) {
//                values.add(line[i]);
//            }
//            valuesList.add(values);
//            datatypeList.add(dataTypes);
//            deviceList.add(alignedDevice);
//        }
//
//        session.insertAlignedRecords(deviceList, times, measurementsList,datatypeList,valuesList);
//        afterMethod(expectCount,expectCount,"insertAlignedRecords");
//        Assert.assertThrows(StatementExecutionException.class,
//                ()-> session.insertRecords(deviceList, times, measurementsList,datatypeList,valuesList));
//    }
//    @Test(priority = 130)
//    public void testInsertAlignedRecords_multiDevice() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        List<Long> times = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<Object>> valuesList = new ArrayList<>();
//        List<List<TSDataType>> datatypeList = new ArrayList<>();
//        List<String> deviceList = new ArrayList<>();
//
//        for (Iterator<Object[]> it = getMultiRecords(); it.hasNext();) {
//            Object[] line =  it.next();
//            times.add(Long.valueOf((String) line[0]));
//            deviceList.add(line[1].toString());
//            measurementsList.add(measurements);
//
//            List<Object> values = new ArrayList<>();
//            for (int i = 0; i < schemaList.size(); i++) {
//                values.add(line[i+2]);
//            }
//            valuesList.add(values);
//            datatypeList.add(dataTypes);
//        }
//        session.insertAlignedRecords(deviceList,times,measurementsList,datatypeList,valuesList);
//        checkInsertMultiDevices("insertAlignedRecords multi device");
//    }
//
//    /**
//     * 单条 insertTablet error
//     * 非对齐
//     */
//    @Test(dataProvider= "insertSingleError", expectedExceptions = StatementExecutionException.class, priority = 50)
//    public void testInsertTablet_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException, IOException {
//        Tablet tablet = new Tablet(device, schemaList, 1);
//        int rowIndex = 0;
//        tablet.addTimestamp(rowIndex, Long.valueOf(time));
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, s_boolean);
//        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, s_int);
//        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, s_long);
//        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, s_float);
//        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, s_double);
//        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, s_text);
//        Session s = PrepareConnection.getSession();
//        try {
//            s.insertTablet(tablet);
//        } finally {
//            s.close();
//        }
//    }
//    /**
//     * 单条 insertTablet error
//     * 对齐
//     */
//    @Test(dataProvider= "insertSingleError", expectedExceptions = StatementExecutionException.class, priority = 51)
//    public void testInsertAlignedTablet_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException, IOException {
//        Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
//        int rowIndex = 0;
//        tablet.addTimestamp(rowIndex, Long.valueOf(time));
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, s_boolean);
//        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, s_int);
//        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, s_long);
//        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, s_float);
//        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, s_double);
//        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, s_text);
//        Session s = PrepareConnection.getSession();
//        try {
//            s.insertAlignedTablet(tablet);
//        } finally {
//            s.close();
//        }
//
//    }
//    @Test(priority = 52)
//    public void checkResult_insertTablet() throws IoTDBConnectionException, StatementExecutionException {
//        afterMethod(0,0,"Error insert tablet");
//    }
//    @Test
//    public void testInsertTablets_error() throws IOException, IoTDBConnectionException, StatementExecutionException {
//        Map<String, Tablet> tabletMap = new HashMap<>();
//        for (Iterator<Object[]> it = getSingleError(); it.hasNext();) {
//            Object[] line =  it.next();
//            Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
//            int rowIndex = 0;
//            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            for (int i = 0; i < schemaList.size(); i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+1]);
//            }
//            tabletMap.put(alignedDevice, tablet);
//        }
//        session.insertTablets(tabletMap);
//        afterMethod(0,0,"insert tablets non aligned");
//    }
//
//    @Test
//    public void testInsertAlignedTablets_error() throws IOException, IoTDBConnectionException, StatementExecutionException {
//        Map<String, Tablet> tabletMap = new HashMap<>();
//        for (Iterator<Object[]> it = getSingleError(); it.hasNext();) {
//            Object[] line =  it.next();
//            Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
//            int rowIndex = 0;
//            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            for (int i = 0; i < schemaList.size(); i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i+1]);
//            }
//            tabletMap.put(alignedDevice, tablet);
//        }
//        session.insertAlignedTablets(tabletMap);
//        afterMethod(0,0,"insert tablets non aligned");
//    }
//
//    @Test(dataProvider= "getSingleError", expectedExceptions = StatementExecutionException.class, priority = 102)
//    public void testInsertRecord_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
//        List<Object> values = new ArrayList<>();
//        values.add(s_boolean);
//        values.add(s_int);
//        values.add(s_long);
//        values.add(s_float);
//        values.add(s_double);
//        values.add(s_text);
//        try (Session s = PrepareConnection.getSession()) {
//            s.insertRecord(device, Long.valueOf(time), measurements, dataTypes, values);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        values.clear();
//    }
//    @Test(dataProvider= "getSingleError", expectedExceptions = StatementExecutionException.class, priority = 102)
//    public void testInsertAlignedRecord_error(String time, Object s_boolean, Object s_int, Object s_long, Object s_float, Object s_double, Object s_text) throws IoTDBConnectionException, StatementExecutionException {
//        List<Object> values = new ArrayList<>();
//        values.add(s_boolean);
//        values.add(s_int);
//        values.add(s_long);
//        values.add(s_float);
//        values.add(s_double);
//        values.add(s_text);
//        try (Session s = PrepareConnection.getSession()) {
//            s.insertAlignedRecord(alignedDevice, Long.valueOf(time), measurements, dataTypes, values);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        values.clear();
//    }
//
//    /**
//     * 单条 insertTablet error
//     * 非对齐
//     */
//    @Test(priority = 103)
//    public void testInsertTablet_alignedDevice() throws IoTDBConnectionException, StatementExecutionException {
//        Tablet tablet = new Tablet(alignedDevice, schemaList, 1);
//        int rowIndex = 0;
//        tablet.addTimestamp(rowIndex, 1672023281895L);
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, true);
//        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, 33);
//        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 104L);
//        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, 12.54f);
//        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, 204.39);
//        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, "insert aligned device with insertTablet");
//
//        Assert.assertThrows(StatementExecutionException.class, ()->{session.insertTablet(tablet);});
//    }
//    /**
//     * 单条 insertTablet error
//     * 对齐
//     */
//    @Test(priority = 104)
//    public void testInsertAlignedTablet_nonAlignedDevice() throws IoTDBConnectionException, StatementExecutionException {
//        Tablet tablet = new Tablet(device, schemaList, 1);
//        int rowIndex = 0;
//        tablet.addTimestamp(rowIndex, 1672023281895L);
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, false);
//        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, 11);
//        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 305L);
//        tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, 45.55f);
//        tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, 303.78);
//        tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, "insert non-aligned device with insertAlignedTablet");
//        Assert.assertThrows(StatementExecutionException.class, ()->{session.insertAlignedTablet(tablet);});
//    }
//    @Test(priority = 105)
//    public void checkResult_insertTablet2() throws IoTDBConnectionException, StatementExecutionException {
//        afterMethod(0,0, "aligned and non-aligned of insertTablet");
//    }
//
//    @Test(priority = 106)
//    public void testInsertRecord_alignedDevice() throws IoTDBConnectionException, StatementExecutionException {
//        List<Object> values = new ArrayList<>();
//        values.add(true);
//        values.add(20);
//        values.add(300L);
//        values.add(35.55f);
//        values.add(336.77);
//        values.add("insert non-aligned device with insertRecord");
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            session.insertRecord(alignedDevice, 1672023281895L, measurements, dataTypes, values);
//        });
//        values.clear();
//    }
//    @Test(priority = 107)
//    public void testInsertAlignedRecord_nonAlignedDevice() throws IoTDBConnectionException, StatementExecutionException {
//        List<Object> values = new ArrayList<>();
//        values.add(20);
//        values.add(300L);
//        values.add(35.55f);
//        values.add(336.77);
//        values.add("insert non-aligned device with insertAlignedRecord");
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            session.insertAlignedRecord(device, 1672023281895L, measurements, dataTypes, values);
//        });
//        values.clear();
//    }
//
//    @Test(priority = 108)
//    public void checkResult_insertRecord2() throws IoTDBConnectionException, StatementExecutionException {
//        afterMethod(0,0, "aligned and non-aligned of insertRecord");
//    }

}
