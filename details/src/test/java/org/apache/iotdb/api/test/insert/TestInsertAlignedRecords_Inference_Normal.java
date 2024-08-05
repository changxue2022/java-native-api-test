package org.apache.iotdb.api.test.insert;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class TestInsertAlignedRecords_Inference_Normal extends BaseTestSuite {
    // 数据库名称
    private static final String database = "root.testInsertAlignedRecords";
    // 对齐设备名称
    private static final String alignedDeviceId1 = database + ".dq1";
    private static final String alignedDeviceId2 = database + ".dq2";
    private static final String alignedDeviceId3 = database + ".dq3";

    // 存储多个设备
    private final List<String> deviceIds = new ArrayList<>(3);
    // 存储多个times
    private final List<Long> times = new ArrayList<>(3);
    // 存储多个设备的多个物理量
    private final List<List<String>> measurementsList = new ArrayList<>(3);
    // 存储多个设备的多个时间序列数据类型
    private final List<List<TSDataType>> typesList = new ArrayList<>(3);
    // 存储多个设备的多个值
    private final List<List<String>> valuesList = new ArrayList<>(3);
    // 存储物理量名称和数据类型
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(10);
    // 存储单个设备多个物理量
    private final List<String> measurements = new ArrayList<>(10);
    // 存储单个设备多个数据类型
    private final List<TSDataType> dataTypes = new ArrayList<>(10);
    // 存储单个设备多个值
    private final ArrayList<String> values = new ArrayList<>(100);
    // 存储路径
    private final List<String> paths = new ArrayList<>(10);
    // 预期的记录条数
    private final int expectCount = 15;

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
        measureTSTypeInfos.put("s_int64", TSDataType.TEXT);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        measureTSTypeInfos.put("s_string", TSDataType.STRING);
        measureTSTypeInfos.put("s_blob", TSDataType.BLOB);
        measureTSTypeInfos.put("s_timestamp", TSDataType.TEXT);
        measureTSTypeInfos.put("s_date", TSDataType.TEXT);
        // 3.2、遍历measureTSTypeInfos，将时间序列添加路径、物理量和数据类型存入对应集合中
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(alignedDeviceId1 + "." + key);
            measurements.add(key);
            dataTypes.add(value);
        });
        deviceIds.add(alignedDeviceId1);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 3.3、为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 3.4、为设备1创建对齐时间序列
        session.createAlignedTimeseries(alignedDeviceId1, measurements, dataTypes,
                encodings, compressionTypes, null);
        // 清空容器
        paths.clear();

        // 遍历measureTSTypeInfos，将时间序列添加路径、物理量和数据类型存入对应集合中
        measureTSTypeInfos.forEach((key, value) -> {paths.add(alignedDeviceId2 + "." + key);});
        deviceIds.add(alignedDeviceId2);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 为设备2创建对齐时间序列
        session.createAlignedTimeseries(alignedDeviceId2, measurements, dataTypes,
                encodings, compressionTypes, null);
        // 清空容器;
        paths.clear();

        // 遍历measureTSTypeInfos，将时间序列添加路径、物理量和数据类型存入对应集合中
        measureTSTypeInfos.forEach((key, value) -> {paths.add(alignedDeviceId3 + "." + key);});
        deviceIds.add(alignedDeviceId3);
        measurementsList.add(measurements);
        typesList.add(dataTypes);
        // 为设备3创建对齐时间序列
        session.createAlignedTimeseries(alignedDeviceId3, measurements, dataTypes,
                encodings, compressionTypes, null);
    }

    /**
     * 测试 insertAlignedRecords 方法接口
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertAlignedRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
//                out.println("datatype=" + typesList.get(0).get(i)); // 打印数据类型
//                out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        values.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        values.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        values.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        values.add("X'696f74646236'" );
                        break;
                    case DATE:
                        values.add(line[i + 1] == null ? "2024-07-25": (String) line[i + 1]);
                        break;
                }
            }
            // 将数据复制三份
            for (int j = 0; j < 3; j++) {
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
            }
            // 插入数据
            session.insertAlignedRecords(deviceIds, times, measurementsList,valuesList);
            // 初始化
            values.clear();
            valuesList.clear();
            times.clear();
        }
        // 执行SQL查询并计算行数
        countLines("select * from " + alignedDeviceId1, verbose);
        countLines("select * from " + alignedDeviceId2, verbose);
        countLines("select * from " + alignedDeviceId3, verbose);
        // 对比是否操作成功
        afterMethod(expectCount, "insert tablet");
    }

    /**
     * 获取正确的数据
     *
     * @return 对应文件的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-record-inference-success.csv").getData();
    }

    /**
     * 用于查询比较插入条数看是否和预期相同
     *
     * @param expectAligned    预期对齐时间序列的记录条数
     * @param msg              断言失败时的消息
     */
    public void afterMethod(int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非齐时间序列的实际记录条数
        int actualAligned = getRecordCount(alignedDeviceId1, verbose) + getRecordCount(alignedDeviceId2, verbose) + getRecordCount(alignedDeviceId3, verbose);
        // 断言实际记录条数与预期相等
        assert actualAligned == expectAligned : "对齐：实际记录条数与预期不一致 " + msg + " actual=" + actualAligned + " expect=" + expectAligned;
//        out.println("InsertAlignedRecords_Inference_Normal 测试通过");
        // 打印清理数据的消息
//        out.println("清理数据");
        // 清理非对齐时间序列的数据
        session.deleteData(alignedDeviceId1, new Date().getTime());
        session.deleteData(alignedDeviceId1, new Date().getTime());
        session.deleteData(alignedDeviceId1, new Date().getTime());
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
