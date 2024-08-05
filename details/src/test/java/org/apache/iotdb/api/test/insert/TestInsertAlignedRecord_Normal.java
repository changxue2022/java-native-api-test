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

public class TestInsertAlignedRecord_Normal extends BaseTestSuite {
    // 数据库名称
    private static final String database = "root.testInsertAlignedRecord";
    // 对齐设备名称
    private static final String alignedDeviceId = database + ".dq";
    // 时间戳
    private Long time = null;
    // 时间序列路径
//    private String path;
    // 数据类型
//    private TSDataType dataType;

    // 存储时间序列路径
    private final List<String> paths = new ArrayList<>(10);
    // 用于存储物理量
    private final ArrayList<String> measurements = new ArrayList<>();
    // 用于存储数据类型
    private final ArrayList<TSDataType> dataTypes = new ArrayList<>();
    // 用于存储值
    private final ArrayList<Object> values = new ArrayList<>();

    // 存储物理量名称和数据类型
    private final Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(10);
    // 预期的记录条数
    private final int EXPECTANT = 10;

    // 在测试类之前准备好环境（数据库、时间序列）
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
            paths.add(alignedDeviceId + "." + key);
            measurements.add(key);
            dataTypes.add(value);
        });
        // 3.3、创建并添加编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 3.4、创建对齐时间序列
        session.createAlignedTimeseries(alignedDeviceId, measurements, dataTypes,
                encodings, compressionTypes, null);
    }

    /**
     * 测试 insertAlignedRecord 方法接口
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertAlignedRecord() throws IOException, IoTDBConnectionException, StatementExecutionException {
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
//                out.println("datatype=" + dataTypes.get(i)); // 打印数据类型
//                out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        values.add(line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        values.add(line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        values.add(line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        values.add(line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
            // 插入数据
            session.insertAlignedRecord(alignedDeviceId, time, measurements, dataTypes, values);
            // 清空容器
            values.clear();
        }

        // 执行SQL查询并计算行数
        countLines("select * from " + alignedDeviceId, verbose);
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
     * @param expectAligned 预期非对齐时间序列的记录条数
     * @param msg           断言失败时的消息
     */
    public void afterMethod(int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 获取对齐时间序列的实际记录条数
        int actualAligned = getRecordCount(alignedDeviceId, verbose);
        // 断言实际记录条数与预期相等
        assert actualAligned == expectAligned : "对齐：实际记录条数与预期不一致 " + msg + " actual=" + actualAligned + " expect=" + expectAligned;
//        out.println("InsertAlignedRecord_Normal 测试通过");
        // 打印清理数据的消息
//        out.println("清理数据");
        // 清理对齐时间序列的数据
        session.deleteData(alignedDeviceId, new Date().getTime());
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
