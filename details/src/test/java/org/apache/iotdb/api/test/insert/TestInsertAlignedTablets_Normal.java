package org.apache.iotdb.api.test.insert;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.*;

import static java.lang.System.out;

public class TestInsertAlignedTablets_Normal extends BaseTestSuite {
    // 数据库名称
    private static final String database = "root.testInsertAlignedTables";
    // 设备名称
    private static final String deviceId = database + ".dq";
    // 文件路径
    private static final String filepath1 = "data/insert-tablet-success1.csv";
    private static final String filepath2 = "data/insert-tablet-success2.csv";
    private static final String filepath3 = "data/insert-tablet-success3.csv";

    // 存储路径
    private final List<String> paths = new ArrayList<>(10);
    // 存储物理量
    private final List<String> measurements = new ArrayList<>(10);
    // 存储数据类型
    private final List<TSDataType> dataTypes = new ArrayList<>(10);
    // 物理量的Schema
    private final List<MeasurementSchema> schemaList = new ArrayList<>();
    // 用于存储多个Tablet
    private final Map<String, Tablet> tablets = new HashMap<>();

    // 物理量类型信息
    private final Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(10);
    // 预期的记录条数
    private final int expectCount = 30;

    /**
     * 在测试类之前准备好环境（数据库、时间序列）
     */
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
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 创建对齐时间序列
        session.createAlignedTimeseries(deviceId, measurements, dataTypes, encodings, compressionTypes, null);
    }

    /**
     * 测试 insertAlignedTables 方法接口
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertAlignedTables() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 1、创建多个新的tablet实例
        Tablet tablet1 = new Tablet(deviceId, schemaList, 10);
        Tablet tablet2 = new Tablet(deviceId, schemaList, 10);
        Tablet tablet3 = new Tablet(deviceId, schemaList, 10);

        // 2、遍历获取的单行数据，进行数据处理
        // 第一份文件的数据
        for (Iterator<Object[]> it = getSingleNormal(filepath1); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet1.rowSize++;
            // 向tablet添加时间戳
            tablet1.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
//                out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                // 根据数据类型添加值到tablet
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet1.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        // 第二份文件的路径
        for (Iterator<Object[]> it = getSingleNormal(filepath2); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet2.rowSize++;
            // 向tablet添加时间戳
            tablet2.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
//                out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                // 根据数据类型添加值到tablet
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet2.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        // 第三份文件的数据
        for (Iterator<Object[]> it = getSingleNormal(filepath3); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet3.rowSize++;
            // 向tablet添加时间戳
            tablet3.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//            out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
//                out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                // 根据数据类型添加值到tablet
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet3.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        // 4、将生成的tablet添加的集合中
        tablets.put("1",tablet1);
        tablets.put("2",tablet2);
        tablets.put("3",tablet3);

        // 5、插入数据
        session.insertTablets(tablets);

        // 6、执行SQL查询并计算行数
        countLines("select * from " + deviceId, verbose);

        // 7、对比是否操作成功
        afterMethod(expectCount, "insert tablet");
    }

    /**
     * 用于查询比较插入条数看是否和预期相同
     *
     * @param expectAligned 预期非对齐时间序列的记录条数
     * @param msg              断言失败时的消息
     */
    public void afterMethod(int expectAligned, String msg) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非对齐时间序列的实际记录条数
        int actualAligned = getRecordCount(deviceId, verbose);
        // 断言实际记录条数与预期相等
        assert actualAligned == expectAligned : "非对齐：实际记录条数与预期不一致 " + msg + " actual=" + actualAligned + " expect=" + expectAligned;
        //out.println("InsertAlignedTablets_Normal 测试通过");
        // 打印清理数据的消息
//        out.println("清理数据");
        // 清理非对齐时间序列的数据
        session.deleteData(deviceId, new Date().getTime());
    }

    /**
     * 获取正确的数据
     * @param filepath 文件路径
     * @return 对应文件的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal(String filepath) throws IOException {
        return new CustomDataProvider().load(filepath).getData();
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
