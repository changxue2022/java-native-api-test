package org.apache.iotdb.api.test.insert;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.*;

import static java.lang.System.out;

/**
 * 测试数据写入——insertRecords 正常情况
 * author：肖林捷
 */
public class TestInsertRecords_Normal extends BaseTestSuite {
    // 数据库名称
    private static final String database = "root.testInsertRecords";
    // 非对齐设备名称
    private static final String deviceId1 = database + ".fdq1";
    private static final String deviceId2 = database + ".fdq2";
    private static final String deviceId3 = database + ".fdq3";

    // 存储多个设备
    private final List<String> deviceIds = new ArrayList<>(30);
    // 存储多个times
    private final List<Long> times = new ArrayList<>(30);
    // 存储多个设备的多个物理量
    private final List<List<String>> measurementsList = new ArrayList<>(30);
    // 存储多个设备的多个时间序列数据类型
    private final List<List<TSDataType>> typesList = new ArrayList<>(30);
    // 存储多个设备的多个值
    private final List<List<Object>> valuesList = new ArrayList<>(30);
    // 存储单个设备多个物理量
    private List<String> measurements;
    // 存储单个设备多个数据类型
    private List<TSDataType> dataTypes;
    // 存储单个设备多个值
    private ArrayList<Object> values;
    // 存储路径
    private final List<String> paths = new ArrayList<>(10);

    // 存储物理量名称和数据类型
    private final Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(10);
    // 预期的记录条数
    private final int EXPECTANT = 30;

    /**
     * 在测试类之前准备好环境（数据库、时间序列）
     */
    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        // 1、检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }

        // 2、创建数据库
        session.createDatabase(database);

        // 3、创建时间序列
        // 3.1、添加数据类型
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
        // 3.2、遍历measureTSTypeInfos，将路径、物理量和数据类型存入对应集合中
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId1 + "." + key);
        });
        for (int i = 0; i < 10; i++) {
            measurements = new ArrayList<>(10);
            dataTypes = new ArrayList<>(10);
            measureTSTypeInfos.forEach((key, value) -> {
                measurements.add(key);
                dataTypes.add(value);
            });
            // 将集合存入对应的集合中
            deviceIds.add(deviceId1);
            measurementsList.add(measurements);
            typesList.add(dataTypes);
        }
        // 3.3、创建并添加编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 3.4、为设备1创建非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
        // 清空容器
        paths.clear();

        // 3.5、重复操作为设备2创建非对齐时间序列
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId2 + "." + key);
        });
        for (int i = 0; i < 10; i++) {
            measurements = new ArrayList<>(10);
            dataTypes = new ArrayList<>(10);
            measureTSTypeInfos.forEach((key, value) -> {
                measurements.add(key);
                dataTypes.add(value);
            });
            // 将集合存入对应的集合中
            deviceIds.add(deviceId2);
            measurementsList.add(measurements);
            typesList.add(dataTypes);
        }
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
        paths.clear();

//         3.6、重复操作为设备3创建非对齐时间序列
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId3 + "." + key);
        });
        for (int i = 0; i < 10; i++) {
            measurements = new ArrayList<>(10);
            dataTypes = new ArrayList<>(10);
            measureTSTypeInfos.forEach((key, value) -> {
                measurements.add(key);
                dataTypes.add(value);
            });
            // 将集合存入对应的集合中
            deviceIds.add(deviceId3);
            measurementsList.add(measurements);
            typesList.add(dataTypes);
        }
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
    }

    /**
     * 测试 insertRecords 方法接口
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
        int number = 1;
        for (int i = 0; i < 3; i++) {
            // 遍历获取的单行数据，为设备添加值
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                values = new ArrayList<>(10);
                // 获取每行数据
                Object[] line = it.next();
                // 打印行索引和时间戳
                //            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
                // 遍历schemaList，为每列添加数据
                for (int j = 0; j < measurements.size(); j++) {
                    //                out.println("datatype=" + typesList.get(0).get(j)); // 打印数据类型
                    //                out.println("line[" + (j + 1) + "]=" + line[j + 1]); // 打印当前行的列值
                    // 根据数据类型添加值到values中
                    switch (dataTypes.get(j)) {
                        case BOOLEAN:
                            values.add(line[j + 1] == null ? null : Boolean.valueOf((String) line[j + 1]));
                            break;
                        case INT32:
                            values.add(line[j + 1] == null ? null : Integer.valueOf((String) line[j + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[j + 1] == null ? null : Long.valueOf((String) line[j + 1]));
                            break;
                        case FLOAT:
                            values.add(line[j + 1] == null ? null : Float.valueOf((String) line[j + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[j + 1] == null ? null : Double.valueOf((String) line[j + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[j + 1] == null ? null : line[j + 1]);
                            break;
                        case BLOB:
                            values.add(line[j + 1] == null ? null : new Binary((String) line[j + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[j + 1] == null ? null : LocalDate.parse((CharSequence) line[j + 1]));
                            break;
                    }
                }
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
            }
        }
        // 插入数据
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        // 执行SQL查询并计算行数
        countLines("select * from " + deviceId1, verbose);
        countLines("select * from " + deviceId2, verbose);
        countLines("select * from " + deviceId3, verbose);
        // 对比是否操作成功
        afterMethod(EXPECTANT, "insert tablet");
    }

    /**
     * 获取正确的数据
     *
     * @return 对应文件的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-record-success.csv").getData();
    }

    /**
     * 用于查询比较插入条数看是否和预期相同
     *
     * @param expectNonAligned 预期非对齐时间序列的记录条数
     * @param msg              断言失败时的消息
     */
    public void afterMethod(int expectNonAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非对齐时间序列的实际记录条数
        int actualNonAligned = getRecordCount(deviceId1, verbose) + getRecordCount(deviceId2, verbose) + getRecordCount(deviceId3, verbose);
        // 断言实际记录条数与预期相等
        assert actualNonAligned == expectNonAligned : "非对齐：实际记录条数与预期不一致 " + msg + " actual=" + actualNonAligned + " expect=" + expectNonAligned;
//        out.println("InsertRecords_Normal 测试通过");
        // 打印清理数据的消息
//        out.println("清理数据");
        // 清理非对齐时间序列的数据
        session.deleteData(deviceId1, new Date().getTime());
        session.deleteData(deviceId2, new Date().getTime());
        session.deleteData(deviceId3, new Date().getTime());
    }

    /**
     * 在测试类之后执行的删除数据库
     */
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        // 删除数据库
        session.deleteDatabase(database);
    }

}
