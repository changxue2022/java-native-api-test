package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.out;
import static java.lang.Thread.sleep;

public class TestTimeSeries extends BaseTestSuite {
    private TSDataType dataType = null;
    private TSEncoding encoding = null;
    private CompressionType compressionType = null;
    private Map<String, String> props = new HashMap<>();
    private Map<String, String> tags = new HashMap<>();
    private Map<String, String> attrs = new HashMap<>();
    private static List<String> normalTSs = new ArrayList<>();


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        normalTSs = new CustomDataProvider().getDeviceAndTs("data/timeseries-single.csv");
        cleanDatabases(verbose);
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(verbose);
    }

    private void translateString2Type(String datatypeStr, String encodingStr, String compressStr) {
        switch (datatypeStr) {
            case "boolean":
                dataType = TSDataType.BOOLEAN;
                break;
            case "int":
                dataType = TSDataType.INT32;
                break;
            case "long":
                dataType = TSDataType.INT64;
                break;
            case "float":
                dataType = TSDataType.FLOAT;
                break;
            case "double":
                dataType = TSDataType.DOUBLE;
                break;
            case "vector":
                dataType = TSDataType.VECTOR;
                break;
            case "text":
                dataType = TSDataType.TEXT;
                break;
        }
        switch (encodingStr) {
            case "PLAIN":
                encoding = TSEncoding.PLAIN;
                break;
            case "DICTIONARY":
                encoding = TSEncoding.DICTIONARY;
                break;
            case "RLE":
                encoding = TSEncoding.RLE;
                break;
            case "DIFF":
                encoding = TSEncoding.DIFF;
                break;
            case "TS_2DIFF":
                encoding = TSEncoding.TS_2DIFF;
                break;
            case "BITMAP":
                encoding = TSEncoding.BITMAP;
                break;
            case "GORILLA_V1":
                encoding = TSEncoding.GORILLA_V1;
                break;
            case "REGULAR":
                encoding = TSEncoding.REGULAR;
                break;
            case "GORILLA":
                encoding = TSEncoding.GORILLA;
                break;
            case "ZIGZAG":
                encoding = TSEncoding.ZIGZAG;
                break;
            case "FREQ":
                encoding = TSEncoding.FREQ;
                break;
        }
        switch (compressStr) {
            case "UNCOMPRESSED":
                compressionType = CompressionType.UNCOMPRESSED;
                break;
            case "SNAPPY":
                compressionType = CompressionType.SNAPPY;
                break;
            case "GZIP":
                compressionType = CompressionType.GZIP;
                break;
//            case "LZO":
//                compressionType = CompressionType.LZO;
//                break;
//            case "SDT":
//                compressionType = CompressionType.SDT;
//                break;
//            case "PAA":
//                compressionType = CompressionType.PAA;
//                break;
//            case "PLA":
//                compressionType = CompressionType.PLA;
//                break;
            case "lz4":
                compressionType = CompressionType.LZ4;
                break;
        }
    }


    /**
     * 工具类，测试props,tags,attrs个数
     *
     * @param value
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    private void testCreateTimeSeries_tags(int value) throws IoTDBConnectionException, StatementExecutionException {
        String path = "root.t1.pros" + value;
        for (int i = 0; i < value; i++) {
//            props.put("prop_" + i, "弹出模板" + i);
            tags.put("tag_" + i, "竖编辑模式" + i);
            attrs.put("attr_" + i, "显示当前类" + i);
        }
        System.out.println("props=" + props.size());
        System.out.println("tags=" + tags.size());
        System.out.println("attrs=" + attrs.size());
        session.createTimeseries(path,
                TSDataType.BOOLEAN,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED,
                props,
                tags,
                attrs,
                value + "_props_tags_attrs");
        props.clear();
        tags.clear();
        attrs.clear();
    }

    @Test(priority = 1)
    public void testDuplicateCreateTS() throws IoTDBConnectionException, StatementExecutionException {
        int count = getTimeSeriesCount("", false);
        String database = "root.testDuplicate";
        String path = database + ".device.s_name";
        session.createDatabase(database);
        session.createTimeseries(path, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        count++;
        assert count == getTimeSeriesCount("", verbose):"创建TS成功";
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseries(path, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        });
        assert count == getTimeSeriesCount("", verbose):"重复创建失败";
        insertRecordSingle(path, TSDataType.BOOLEAN, false, null);
        // 删除
        List<String> paths = new ArrayList<>(1);
        paths.add(path);
        session.deleteTimeseries(paths);
        count--;
        assert count == countLines("show timeseries", verbose):"删除成功";
        session.deleteDatabase(database);
    }
    @DataProvider(name = "createSingleTimeSeriesNormal", parallel = true)
    private Iterator<Object[]> getSingleTimeSeriesNormal() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single.csv").getData();
    }

    // TIMECHODB-124
    @Test(priority = 10, dataProvider = "createSingleTimeSeriesNormal")
    public void testCreateSingleTimeSeries_normal(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        translateString2Type(datatypeStr, encodingStr, compressStr);
        session.createTimeseries(device+"."+tsName, dataType, encoding, compressionType, props, tags, attrs, alias);
    }
    @Test(priority = 20, dataProvider = "createSingleTimeSeriesNormal")
    public void testDeleteSingleTimeSeries_normal(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(device+"."+tsName);
    }
    @DataProvider(name = "createSingleTimeSeriesError", parallel = true)
    private Iterator<Object[]> getSingleTimeSeriesError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single-error.csv").getData();
    }
    @Test(priority = 21, dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteSingleTimeSeries_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    @Test(priority = 30, dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testCreateSingleTimeSeries_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        translateString2Type(datatypeStr, encodingStr, compressStr);
        session.createTimeseries(path, dataType, encoding, compressionType, props, tags, attrs, alias);
    }

    @Test(priority = 40, dataProvider = "createSingleTimeSeriesNormal")
    public void testCreateSingleTimeSeriesAligned_normal(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        translateString2Type(datatypeStr, encodingStr, compressStr);
        List<String> tsList = new ArrayList<>(1);
        List<String> aliasList = new ArrayList<>(1);
        List<TSDataType> tsDataTypes = new ArrayList<>(1);
        List<TSEncoding> tsEncodings = new ArrayList<>(1);
        List<CompressionType> compressionTypes = new ArrayList<>(1);
        tsDataTypes.add(dataType);
        tsEncodings.add(encoding);
        compressionTypes.add(compressionType);
        tsList.add(tsName);
        aliasList.add(alias);
        session.createAlignedTimeseries(device, tsList, tsDataTypes, tsEncodings, compressionTypes, aliasList);
    }
    @DataProvider(name = "deleteNormalWildcard")
    private Iterator<Object[]> deleteNormal_wildcard() throws IOException {
        return new CustomDataProvider().load("data/timeseries-delete.csv").getData();
    }

    @Test(priority = 41, dataProvider = "deleteNormalWildcard")
    public void testDeleteMultiTS(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    @Test(priority = 50)
    public void testCreateTimeSeries_10props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(10);
    }
    // TIMECHODB-126
    @Test(priority = 51)
    public void testCreateTimeSeries_100props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(100);
    }

    @Test(priority = 52)
    public void testCreateTimeSeries_max_props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(700); //默认值：tag_attribute_total_size=700
    }

    @Test(priority = 53, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_GTprops_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(701);
    }

    @DataProvider(name = "deleteTimeSeriesError", parallel = true)
    private Iterator<Object[]> getDeleteTimeSeriesError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-delete-error.csv").getData();
    }
    @Test(priority = 60, dataProvider = "deleteTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteError(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    @DataProvider(name = "deleteTimeSeriesMultiError")
    private Iterator<Object[]> getDeleteTimeSeriesMultiError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-deleteG-error.csv").getData();
    }
    private void createTimeSeriesIgnoreError(int count) throws IoTDBConnectionException, IOException {
        Iterator<Object[]> lines = new CustomDataProvider().load("data/timeseries-single.csv").getData();
        for (int i=0; i <= count && lines.hasNext(); i++) {
            Object[] line = lines.next();
            System.out.println(line);
            try {
                translateString2Type((String)line[1], (String)line[2], (String)line[3]);
                session.createTimeseries((String)line[0], dataType, encoding, compressionType, new HashMap<>(), new HashMap<>(), new HashMap<>(), (String)line[7]);
            } catch (StatementExecutionException e) {
            }
        }
    }
    @Test(priority = 80, dataProvider = "deleteTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteMultiTimeSeries_error(String errTS, String total_str, String position_str, String msg) throws IoTDBConnectionException, StatementExecutionException, InterruptedException, IOException {
        int total = Integer.parseInt(total_str);
        int position = Integer.parseInt(position_str);
        List<String> ts_list = new ArrayList<>(total);
        if (position == 1) {
            ts_list.add(errTS);
            for (int i = 1; i < total; i++) {
                ts_list.add(normalTSs.get(i-1));
            }
        } else if (position == total) {
            for (int i = 0; i < total-1; i++) {
                ts_list.add(normalTSs.get(i));
            }
            ts_list.add(errTS);
        } else {
            int i = 0;
            for (; i < position; i++) {
                ts_list.add(normalTSs.get(i));
            }
            ts_list.add(errTS);
            i++;
            for (; i < total; i++) {
                ts_list.add(normalTSs.get(i-1));
            }
        }
//        System.out.println(ts_list);
        createTimeSeriesIgnoreError(total-1);
        int expectCount = getTimeSeriesCount("", verbose) - total + 1 ;
        session.deleteStorageGroups(ts_list);
//        int actualCount = getTimeSeriesCount("", verbose);
//        System.out.println(msg);
//        Thread.sleep(1000);
//        assert expectCount == actualCount: "删除后 actual=:" + actualCount + ", expect=" +expectCount + ", total="+total;
    }

}
