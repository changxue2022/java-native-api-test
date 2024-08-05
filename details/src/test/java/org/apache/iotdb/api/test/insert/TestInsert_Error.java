package org.apache.iotdb.api.test.insert;


import org.apache.iotdb.api.test.TestInsertUtil;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.*;

import static java.lang.System.out;

public class TestInsert_Error extends TestInsertUtil {

    /**
     * 测试 insertTablet 的错误情况：类型不一致
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void testInsertTabletError1() throws IoTDBConnectionException, StatementExecutionException {
        // TODO：初始化createTimeSeries()方法内部变量：方式一：使用局部变量来操作createTimeSeries(变量1，变量2)；方式二：再写个方法用于初始化createTimeSeries()内的所有变量；方式三：使用单例模式或工厂模式
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        // 2、初始化bitmap，用于标记null值
        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
//                 处理null值
                    if (line[i + 1] == null) {
                        out.println("process null value");
                        tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                    }
                }
            }
            // 插入数据
            session.insertTablet(tablet);
        } catch (Exception e) {
            assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertTabletError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablet 的错误情况：类型不一致
     */
    @Test(priority = 11) // 测试执行的优先级为10
    public void testInsertAlignedTabletError1() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 测试插入接口
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        // 2、初始化bitmap，用于标记null值
        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
//                 处理null值
                    if (line[i + 1] == null) {
                        out.println("process null value");
                        tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                    }
                }
            }
            // 插入数据
            session.insertAlignedTablet(tablet);
        } catch (Exception e) {
            assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedTabletError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablet 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 12) // 测试执行的优先级为10
    public void testInsertTabletError2() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        // 2、初始化bitmap，用于标记null值
        // tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行测试
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                    // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 插入数据
            session.insertTablet(tablet);
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertTabletError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablet 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 13) // 测试执行的优先级为10
    public void testInsertAlignedTabletError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
// 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                    // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 插入数据
            session.insertAlignedTablet(tablet);
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedTabletError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablet 的错误情况：未实例化有效行rowSize
     */
    @Test(priority = 14) // 测试执行的优先级为10
    public void testInsertTabletError3() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 4、遍历获取的单行数据，进行数据处理
        Iterator<Object[]> it = getSingleNormal();
        // 获取每行数据
        Object[] line = it.next();
        // 实例化有效行并切换行索引
//            rowIndex = tablet.rowSize++;
        // 向tablet添加时间戳
        tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//        out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
        // 遍历schemaList，为每列添加数据
        for (int i = 0; i < schemaList.size(); i++) {
//            out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//            out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
            // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
            // 根据数据类型添加值到tablet
            switch (schemaList.get(i).getType()) {
                case BOOLEAN:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                    break;
                case INT32:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                    break;
                case INT64:
                case TIMESTAMP:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                    break;
                case FLOAT:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                    break;
                case DOUBLE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                    break;
                case TEXT:
                case STRING:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? "stringnull" : line[i + 1]);
                    break;
                case BLOB:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                    break;
                case DATE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                    break;
            }
        }
        // 执行
        try {
            // 插入数据
            session.insertTablet(tablet);
            // 执行SQL查询并计算行数
            countLines("select * from " + deviceId, verbose);
            // 对比是否操作成功
            afterMethod(1, 0, "insert tablet");
        } catch (Exception e) {
            assert "java.lang.RuntimeException".equals(e.getClass().getName()): "InsertTabletError3 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablet 的错误情况：未实例化有效行rowSize
     */
    @Test(priority = 15) // 测试执行的优先级为10
    public void testInsertAlignedTabletError3() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 4、遍历获取的单行数据，进行数据处理
        Iterator<Object[]> it = getSingleNormal();
        // 获取每行数据
        Object[] line = it.next();
        // 实例化有效行并切换行索引
//            rowIndex = tablet.rowSize++;
        // 向tablet添加时间戳
        tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//        out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
        // 遍历schemaList，为每列添加数据
        for (int i = 0; i < schemaList.size(); i++) {
//            out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//            out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
            // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
            // 根据数据类型添加值到tablet
            switch (schemaList.get(i).getType()) {
                case BOOLEAN:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                    break;
                case INT32:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                    break;
                case INT64:
                case TIMESTAMP:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                    break;
                case FLOAT:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                    break;
                case DOUBLE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                    break;
                case TEXT:
                case STRING:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? "stringnull" : line[i + 1]);
                    break;
                case BLOB:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                    break;
                case DATE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                    break;
            }
        }
        // 执行
        try {
            // 插入数据
            session.insertAlignedTablet(tablet);
            // 执行SQL查询并计算行数
            countLines("select * from " + deviceId, verbose);
            // 对比是否操作成功
            afterMethod(0, 1, "insert tablet");
        } catch (Exception e) {
            assert "java.lang.RuntimeException".equals(e.getClass().getName()): "InsertAlignedTabletError3 测试失败-其他错误:" + e;
        }
    }


    /**
     * 测试 insertTablets 的错误情况：类型不一致
     */
    @Test(priority = 16) // 测试执行的优先级为10
    public void testInsertTabletsError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、初始化bitmap，用于标记null值
        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
//                 处理null值
                    if (line[i + 1] == null) {
                        out.println("process null value");
                        tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                    }
                }
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertTablets(tablets);
        } catch (Exception e) {
            assert "java.lang.ClassCastException".equals(e.getClass().getName()): "InsertTabletsError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablets 的错误情况：类型不一致
     */
    @Test(priority = 17) // 测试执行的优先级为10
    public void testInsertAlignedTabletsError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 测试插入接口
// 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、初始化bitmap，用于标记null值
        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
//                 处理null值
                    if (line[i + 1] == null) {
                        out.println("process null value");
                        tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                            break;
                    }
                }
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertAlignedTablets(tablets);
        } catch (Exception e) {
            assert "java.lang.ClassCastException".equals(e.getClass().getName()): "InsertAlignedTabletsError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablets 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 18) // 测试执行的优先级为10
    public void testInsertTabletsError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                    // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertTablets(tablets);
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertTabletsError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablets 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 19) // 测试执行的优先级为10
    public void testInsertAlignedTabletsError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
// 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                rowIndex = tablet.rowSize++;
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//                out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
//                    out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                    // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertAlignedTablets(tablets);
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedTabletsError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablets 的错误情况：未实例化有效行rowSize
     */
    @Test(priority = 20) // 测试执行的优先级为10
    public void testInsertTabletsError3() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 4、遍历获取的单行数据，进行数据处理
        Iterator<Object[]> it = getSingleNormal();
        // 获取每行数据
        Object[] line = it.next();
        // 实例化有效行并切换行索引
//            rowIndex = tablet.rowSize++;
        // 向tablet添加时间戳
        tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//        out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
        // 遍历schemaList，为每列添加数据
        for (int i = 0; i < schemaList.size(); i++) {
//            out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//            out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
            // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
            // 根据数据类型添加值到tablet
            switch (schemaList.get(i).getType()) {
                case BOOLEAN:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                    break;
                case INT32:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                    break;
                case INT64:
                case TIMESTAMP:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                    break;
                case FLOAT:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                    break;
                case DOUBLE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                    break;
                case TEXT:
                case STRING:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? "stringnull" : line[i + 1]);
                    break;
                case BLOB:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                    break;
                case DATE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                    break;
            }
        }
        // 执行
        try {
            tablets.put("1", tablet);
            // 插入数据
            session.insertTablets(tablets);
            // 执行SQL查询并计算行数
            countLines("select * from " + deviceId, verbose);
            // 对比是否操作成功
            afterMethod(1, 0, "insert tablet");
        } catch (Exception e) {
            assert "java.lang.RuntimeException".equals(e.getClass().getName()) : "InsertTabletsError3 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablets 的错误情况：未实例化有效行rowSize
     */
    @Test(priority = 21) // 测试执行的优先级为10
    public void testInsertAlignedTabletsError3() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、初始化bitmap，用于标记null值
//        tablet.initBitMaps();
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 4、遍历获取的单行数据，进行数据处理
        Iterator<Object[]> it = getSingleNormal();
        // 获取每行数据
        Object[] line = it.next();
        // 实例化有效行并切换行索引
//            rowIndex = tablet.rowSize++;
        // 向tablet添加时间戳
        tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
//        out.println("########### 行号：" + (rowIndex + 1) + " | 时间戳:" + line[0] + " ###########"); // 打印行索引和时间戳
        // 遍历schemaList，为每列添加数据
        for (int i = 0; i < schemaList.size(); i++) {
//            out.println("datatype=" + schemaList.get(i).getType()); // 打印数据类型
//            out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
            // 处理null值
//                if (line[i + 1] == null) {
//                    out.println("process null value");
//                    tablet.bitMaps[i].mark((int) rowIndex); // 使用bitmap标记null值
//                }
            // 根据数据类型添加值到tablet
            switch (schemaList.get(i).getType()) {
                case BOOLEAN:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                    break;
                case INT32:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                    break;
                case INT64:
                case TIMESTAMP:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                    break;
                case FLOAT:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                    break;
                case DOUBLE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                    break;
                case TEXT:
                case STRING:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? "stringnull" : line[i + 1]);
                    break;
                case BLOB:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                    break;
                case DATE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex,
                            line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                    break;
            }
        }
        // 执行
        try {
            tablets.put("1", tablet);
            // 插入数据
            session.insertAlignedTablets(tablets);
            // 执行SQL查询并计算行数
            countLines("select * from " + deviceId, verbose);
            // 对比是否操作成功
            afterMethod(0, 1, "insert tablet");
        } catch (Exception e) {
            assert "java.lang.RuntimeException".equals(e.getClass().getName()) : "InsertAlignedTabletsError3 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertRecord 的错误情况:类型不一致
     */
    @Test(priority = 22) // 测试执行的优先级为10
    public void testInsertRecordError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
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
                        values.add(line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1]);
                        break;
                    case FLOAT:
                        values.add(line[i + 1]);
                        break;
                    case DOUBLE:
                        values.add(line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1]);
                        break;
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertRecord(deviceId, time, measurements, dataTypes, values);
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertRecordError1 测试失败-其他错误:" + e;
                return;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 insertRecord 的错误情况:数值超出范围、数据格式不合法、含空值
     */
    @Test(priority = 23) // 测试执行的优先级为10
    public void testInsertRecordError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
//                    out.println("datatype=" + dataTypes.get(i)); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
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
                session.insertRecord(deviceId, time, measurements, dataTypes, values);
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertRecordError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 InsertRecords 的错误情况:类型不一致
     */
    @Test(priority = 24) // 测试执行的优先级为10
    public void testInsertRecordsError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                switch (typesList.get(0).get(i)) {
                    case BOOLEAN:
                        values.add(line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1]);
                        break;
                    case FLOAT:
                        values.add(line[i + 1]);
                        break;
                    case DOUBLE:
                        values.add(line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1]);
                        break;
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertRecordsError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertRecords 的错误情况:数值超出范围、数据格式不合法、含空值
     */
    @Test(priority = 25) // 测试执行的优先级为10
    public void testInsertRecordsError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 执行
            try {
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < measurementsList.get(0).size(); i++) {
//                    out.println("datatype=" + typesList.get(0).get(i)); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                    // 根据数据类型添加值到values中
                    switch (typesList.get(0).get(i)) {
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
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertRecordsError2 测试失败-其他错误:" + e;
            }
            // 清理容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }

    /**
     * 测试 InsertRecordsOfOneDevice 的错误情况:类型不一致
     */
    @Test(priority = 26) // 测试执行的优先级为10
    public void testInsertRecordsOfOneDeviceError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                        values.add(line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1]);
                        break;
                    case FLOAT:
                        values.add(line[i + 1]);
                        break;
                    case DOUBLE:
                        values.add(line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1]);
                        break;
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertRecordsOfOneDeviceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertRecordsOfOneDevice 的错误情况:数值超出范围、数据格式不合法、含空值
     */
    @Test(priority = 27) // 测试执行的优先级为10
    public void testInsertRecordsOfOneDeviceError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
//                    out.println("datatype=" + dataTypes.get(i)); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
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
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertRecordsOfOneDeviceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }

    /**
     * 测试 insertAlignedRecord 的错误情况:类型不一致
     */
    @Test(priority = 28) // 测试执行的优先级为10
    public void testInsertAlignedRecordError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
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
                        values.add(line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1]);
                        break;
                    case FLOAT:
                        values.add(line[i + 1]);
                        break;
                    case DOUBLE:
                        values.add(line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1]);
                        break;
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, dataTypes, values);
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedRecordError1 测试失败-其他错误:" + e;
                return;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 insertAlignedRecord 的错误情况:数值超出范围、数据格式不合法、含空值
     */
    @Test(priority = 29) // 测试执行的优先级为10
    public void testInsertAlignedRecordError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
//                    out.println("datatype=" + dataTypes.get(i)); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
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
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedRecordError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:类型不一致
     */
    @Test(priority = 30) // 测试执行的优先级为10
    public void testInsertAlignedRecordsError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                switch (typesList.get(0).get(i)) {
                    case BOOLEAN:
                        values.add(line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1]);
                        break;
                    case FLOAT:
                        values.add(line[i + 1]);
                        break;
                    case DOUBLE:
                        values.add(line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1]);
                        break;
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedRecordsError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:数值超出范围、数据格式不合法、含空值
     */
    @Test(priority = 31) // 测试执行的优先级为10
    public void testInsertAlignedRecordsError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 执行
            try {
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < measurementsList.get(0).size(); i++) {
//                    out.println("datatype=" + typesList.get(0).get(i)); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
                    // 根据数据类型添加值到values中
                    switch (typesList.get(0).get(i)) {
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
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedRecordsError2 测试失败-其他错误:" + e;
            }
            // 清理容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:类型不一致
     */
    @Test(priority = 32) // 测试执行的优先级为10
    public void testInsertAlignedRecordsOfOneDeviceError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                        values.add(line[i + 1]);
                        break;
                    case INT32:
                        values.add(line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1]);
                        break;
                    case FLOAT:
                        values.add(line[i + 1]);
                        break;
                    case DOUBLE:
                        values.add(line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1]);
                        break;
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecordsOfOneDevice(alignedDeviceId, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedRecordsOfOneDeviceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:数值超出范围、数据格式不合法、含空值
     */
    @Test(priority = 33) // 测试执行的优先级为10
    public void testInsertAlignedRecordsOfOneDeviceError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        int number = 1;
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 打印行索引和时间戳
//            out.println("########### 行号：" + number++ + "| 时间戳:" + line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
//                    out.println("datatype=" + dataTypes.get(i)); // 打印数据类型
//                    out.println("line[" + (i + 1) + "]=" + line[i + 1]); // 打印当前行的列值
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
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertAlignedRecordsOfOneDevice(alignedDeviceId, times, measurementsList, typesList, valuesList);
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedRecordsOfOneDeviceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }


    /**
     * 测试带推断类型的 insertRecord 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 34) // 测试执行的优先级为10
    public void testInsertRecordInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertRecord(deviceId, time, measurements, valuesInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertRecords 不合法的错误情况：数值超出范围、含空值
     */
    @Test(priority = 35) // 测试执行的优先级为10
    public void testInsertRecordInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertRecord(deviceId, time, measurements, valuesInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordInferenceError2 测试失败-其他错误:" + e;
                return;
            }
            // 清空容器
            valuesInference.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertRecords 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 36) // 测试执行的优先级为10
    public void testInsertRecordsInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordsInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 insertRecord 不合法的错误情况：数值超出范围、含空值
     */
    @Test(priority = 37) // 测试执行的优先级为10
    public void testInsertRecordsInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertRecords(deviceIds, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordsInferenceError2 测试失败-其他错误:" + e;
            }
            // 初始化
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertStringRecordsOfOneDevice 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 38) // 测试执行的优先级为10
    public void testInsertStringRecordsOfOneDeviceInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertStringRecordsOfOneDeviceInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertStringRecordsOfOneDevice 不合法的错误情况：数值超出范围、含空值
     */
    @Test(priority = 39) // 测试执行的优先级为10
    public void testInsertStringRecordsOfOneDeviceInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的非对齐时间序列
        createTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertStringRecordsOfOneDeviceInferenceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试带推断类型的 insertAlignedRecord 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 40) // 测试执行的优先级为10
    public void testInsertAlignedRecordInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createAlignedTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, valuesInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordInferenceError1 测试失败-其他错误:" + e;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedRecords 不合法的错误情况：数值超出范围、含空值
     */
    @Test(priority = 41) // 测试执行的优先级为10
    public void testInsertAlignedRecordInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createAlignedTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, valuesInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordInferenceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            valuesInference.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedRecords 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 42) // 测试执行的优先级为10
    public void testInsertAlignedRecordsInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createAlignedTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordsInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 insertAlignedRecord 不合法的错误情况：数值超出范围、含空值
     */
    @Test(priority = 43) // 测试执行的优先级为10
    public void testInsertAlignedRecordsInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createAlignedTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordsInferenceError2 测试失败-其他错误:" + e;
            }
            // 初始化
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedStringRecordsOfOneDevice 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 44) // 测试执行的优先级为10
    public void testInsertAlignedStringRecordsOfOneDeviceInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建对齐时间序列
        createAlignedTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedStringRecordsOfOneDevice(alignedDeviceId, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedStringRecordsOfOneDeviceInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedStringRecordsOfOneDevice 不合法的错误情况：数值超出范围、含空值
     */
    @Test(priority = 45) // 测试执行的优先级为10
    public void testInsertAlignedStringRecordsOfOneDeviceInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createAlignedTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 行号
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
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedStringRecordsOfOneDevice(alignedDeviceId, times, measurementsList, valuesListInference);
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedStringRecordsOfOneDeviceInferenceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试TS不合法的错误情况：创建部分数据类型的时间序列不支持的编码和压缩类型
     */
    @Test(priority = 46) // 测试执行的优先级为10
    public void testTSError() throws IoTDBConnectionException, StatementExecutionException {
        // 设备名称
        String deviceId = super.database + ".fdq";

        // 存储路径
        List<String> paths = new ArrayList<>(10);
        // 存储数据类型
        List<TSDataType> dataTypes = new ArrayList<>(10);
        // 物理量类型信息
        Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(10);
        // 检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(super.database)) {
            session.deleteDatabase(super.database);
        }
        // 创建数据库
        session.createDatabase(super.database);
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
            dataTypes.add(value);
        });
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.RLBE);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 测试
        try {
            // 可能抛出异常的代码
            // 创建多个非对齐时间序列
            session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                    null, null, null, null);
        } catch (Exception e) {
            assert "org.apache.iotdb.rpc.BatchExecutionException".equals(e.getClass().getName()) : "TSError 测试失败：其他错误" + e.getMessage();
        }
    }


    /**
     * 在测试类之后执行的删除数据库
     */
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        // 删除数据库
        session.deleteDatabase(super.database);
    }
}
