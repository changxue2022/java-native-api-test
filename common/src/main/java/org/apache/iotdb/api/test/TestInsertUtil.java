package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.DataProvider;

import java.io.IOException;
import java.util.*;

import static java.lang.System.out;

public class TestInsertUtil extends BaseTestSuite {
    // 数据库名称
    protected static String database = "root.testInsert";
    // 设备名称
    protected static String deviceId = database + ".fdq";
    protected static String alignedDeviceId = database + ".dq";
    // 时间戳
    protected Long time;

    // 存储多个设备
    protected List<String> deviceIds;
    // 存储多个时间戳
    protected List<Long> times;
    // 存储路径
    protected List<String> paths;
    // 存储物理量
    protected List<String> measurements;
    // 存储数据类型
    protected List<TSDataType> dataTypes;
    // 物理量的Schema
    protected List<MeasurementSchema> schemaList;
    // 存储多个设备的多个物理量
    protected List<List<String>> measurementsList;
    // 存储多个设备的多个时间序列数据类型
    protected List<List<TSDataType>> typesList;
    // 用于存储多个Tablet
    protected Map<String, Tablet> tablets;
    // 用于存储值
//    protected List<String> valuesInference;
//    protected List<Object> values;
//    // 存储多个设备的多个值
//    protected List<List<String>> valuesListInference;
//    protected List<List<Object>> valuesList;

    // 物理量类型信息
    protected Map<String, TSDataType> measureTSTypeInfos;

    /**
     * 获取数据
     *
     * @return 对应文件的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-fail.csv").getData();
    }

    /**
     * 创建非对齐时间序列
     */
    public void createTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
        // 检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        // 创建数据库
        session.createDatabase(database);
        // 实例化对象
        time = null;
        measureTSTypeInfos = new LinkedHashMap<>(10);
        paths = new ArrayList<>(10);
        measurements = new ArrayList<>(10);
        dataTypes = new ArrayList<>(10);
        schemaList = new ArrayList<>(10);
        deviceIds = new ArrayList<>(1);
        measurementsList = new ArrayList<>(1);
        typesList = new ArrayList<>(1);
        // 添加不同的数据类型
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int32", TSDataType.INT32);
        measureTSTypeInfos.put("s_int64", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        measureTSTypeInfos.put("s_string", TSDataType.STRING);
        measureTSTypeInfos.put("s_blob", TSDataType.BLOB);
        measureTSTypeInfos.put("s_timestamp", TSDataType.TIMESTAMP);
        measureTSTypeInfos.put("s_date", TSDataType.DATE);
        // 遍历measureTSTypeInfos，为时间序列添加路径、测量和数据类型
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId + "." + key);
            measurements.add(key);
            dataTypes.add(value);
            schemaList.add(new MeasurementSchema(key, value, TSEncoding.PLAIN, CompressionType.GZIP));
        });
        // 将集合存入对应的集合中
        deviceIds.add(deviceId);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 创建多个非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
        // 初始化
//        paths.clear();
//        measurements.clear();
//        dataTypes.clear();
//        schemaList.clear();
//        deviceIds.clear();
//        measurementsList.clear();
//        typesList.clear();
//        encodings.clear();
//        compressionTypes.clear();
    }

    /**
     * 创建对齐时间序列
     */
    public void createAlignedTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
        // 检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        // 创建数据库
        session.createDatabase(database);
        // 实例化对象
        time = null;
        measureTSTypeInfos = new LinkedHashMap<>(10);
        paths = new ArrayList<>(10);
        measurements = new ArrayList<>(10);
        dataTypes = new ArrayList<>(10);
        schemaList = new ArrayList<>(10);
        deviceIds = new ArrayList<>(1);
        measurementsList = new ArrayList<>(1);
        typesList = new ArrayList<>(1);
        // 添加不同的数据类型
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int32", TSDataType.INT32);
        measureTSTypeInfos.put("s_int64", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        measureTSTypeInfos.put("s_string", TSDataType.STRING);
        measureTSTypeInfos.put("s_blob", TSDataType.BLOB);
        measureTSTypeInfos.put("s_timestamp", TSDataType.TIMESTAMP);
        measureTSTypeInfos.put("s_date", TSDataType.DATE);
        // 遍历measureTSTypeInfos，为时间序列添加路径、测量和数据类型
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(alignedDeviceId + "." + key);
            measurements.add(key);
            dataTypes.add(value);
            schemaList.add(new MeasurementSchema(key, value, TSEncoding.PLAIN, CompressionType.GZIP));
        });
        // 将集合存入对应的集合中
        deviceIds.add(alignedDeviceId);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 创建多个非对齐时间序列
        session.createAlignedTimeseries(alignedDeviceId, measurements, dataTypes,
                encodings, compressionTypes, null);
        // 初始化
//        paths.clear();
//        measurements.clear();
//        dataTypes.clear();
//        schemaList.clear();
//        deviceIds.clear();
//        measurementsList.clear();
//        typesList.clear();
//        encodings.clear();
//        compressionTypes.clear();
    }


    /**
     * 创建推断类型对应的非对齐时间序列
     */
    public void createTimeSeriesInference() throws IoTDBConnectionException, StatementExecutionException {
        // 1、检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }

        // 2、创建数据库
        session.createDatabase(database);
        // 实例化对象
        time = null;
        measureTSTypeInfos = new LinkedHashMap<>(10);
        paths = new ArrayList<>(10);
        measurements = new ArrayList<>(10);
        dataTypes = new ArrayList<>(10);
        deviceIds = new ArrayList<>(1);
        measurementsList = new ArrayList<>(1);
        typesList = new ArrayList<>(1);
        // 3、创建时间序列
        // 3.1、添加数据类型
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int32", TSDataType.INT32);
        measureTSTypeInfos.put("s_int64", TSDataType.TEXT);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        measureTSTypeInfos.put("s_string", TSDataType.STRING);
        measureTSTypeInfos.put("s_blob", TSDataType.BLOB);
        measureTSTypeInfos.put("s_timestamp", TSDataType.TEXT);
        measureTSTypeInfos.put("s_date", TSDataType.TEXT);
        // 3.2、遍历measureTSTypeInfos，将路径、物理量和数据类型存入对应集合中
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId + "." + key);
            measurements.add(key);
            dataTypes.add(value);
        });
        // 将集合存入对应的集合中
        deviceIds.add(deviceId);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 3.3、创建并添加编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 3.4、创建多条非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
        // 初始化
//        paths.clear();
//        measurements.clear();
//        dataTypes.clear();
//        schemaList.clear();
//        deviceIds.clear();
//        measurementsList.clear();
//        typesList.clear();
//        encodings.clear();
//        compressionTypes.clear();
    }

    /**
     * 创建推断类型的对齐时间序列
     */
    public void createAlignedTimeSeriesInference() throws IoTDBConnectionException, StatementExecutionException {
        // 1、检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }

        // 2、创建数据库
        session.createDatabase(database);
        // 实例化对象
        time = null;
        measureTSTypeInfos = new LinkedHashMap<>(10);
        paths = new ArrayList<>(10);
        measurements = new ArrayList<>(10);
        dataTypes = new ArrayList<>(10);
        deviceIds = new ArrayList<>(1);
        measurementsList = new ArrayList<>(1);
        typesList = new ArrayList<>(1);
        // 添加不同的数据类型
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int32", TSDataType.INT32);
        measureTSTypeInfos.put("s_int64", TSDataType.TEXT);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        measureTSTypeInfos.put("s_string", TSDataType.STRING);
        measureTSTypeInfos.put("s_blob", TSDataType.BLOB);
        measureTSTypeInfos.put("s_timestamp", TSDataType.TEXT);
        measureTSTypeInfos.put("s_date", TSDataType.TEXT);
        // 遍历measureTSTypeInfos，为时间序列添加路径、测量和数据类型
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(alignedDeviceId + "." + key);
            measurements.add(key);
            dataTypes.add(value);
        });
        // 将集合存入对应的集合中
        deviceIds.add(alignedDeviceId);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 创建多个非对齐时间序列
        session.createAlignedTimeseries(alignedDeviceId, measurements, dataTypes,
                encodings, compressionTypes, null);
    }

    /**
     * 用于查询比较对齐和非对齐插入条数看是否和预期相同
     *
     * @param expectNonAligned 预期非对齐时间序列的记录条数
     * @param expectAligned    预期对齐时间序列的记录条数
     * @param msg              失败时的方法
     */
    public void afterMethod(int expectNonAligned, int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非对齐时间序列的实际记录条数(不含null值的)
        int actualNonAligned = getRecordCount(deviceId, verbose);
        // 获取对齐时间序列的实际记录条数
        int actualAligned = getRecordCount(alignedDeviceId, verbose);
        // 断言实际记录条数与预期相等
        if (expectAligned == 0) {
            if (actualNonAligned != expectNonAligned) {
                // 构建错误信息
                String errorMessage = "非对齐：" + msg + " actual=" + actualNonAligned + " expect=" + expectNonAligned;
                // 抛出异常
                throw new RuntimeException(errorMessage);
            }
        } else {
            if (actualAligned != expectAligned) {
                // 构建错误信息
                String errorMessage = "对齐：" + msg + " actual=" + actualAligned + " expect=" + expectAligned;
                // 抛出异常
                throw new RuntimeException(errorMessage);
            }
        }
    }

}
