package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.TimeSeriesBaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class TestTimeSeries extends TimeSeriesBaseTestSuite {
    private TSDataType dataType = null;
    private TSEncoding encoding = null;
    private CompressionType compressionType = null;
    private Map<String, String> props = new HashMap<>();
    private Map<String, String> tags = new HashMap<>();
    private Map<String, String> attrs = new HashMap<>();
    private static List<String> normalTSs = new ArrayList<>();
    private Iterator<Object[]> lines = null;

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        normalTSs = new CustomDataProvider().getDeviceAndTs("data/timeseries-single.csv");
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
    @Test(priority = 9)
    public void testSingle() throws IoTDBConnectionException, StatementExecutionException {
        String database = "root.sg.d1";
        String path = database + ".ts1.boolean";
        session.createDatabase(database);
        session.createTimeseries(path, TSDataType.TEXT, TSEncoding.DICTIONARY,CompressionType.GZIP);
        insertRecordSingle(path, TSDataType.TEXT, false, null);
        session.deleteTimeseries(path);
        session.deleteDatabase(database);
    }

    // TIMECHODB-124
    @Test(priority = 10, dataProvider = "createSingleTimeSeriesNormal")
    public void testCreateSingleTimeSeries_normal(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<Object> result = translateString2Type(datatypeStr, encodingStr, compressStr);
        Session s = PrepareConnection.getSession();
        s.createTimeseries(device+"."+tsName, (TSDataType) result.get(0),
                (TSEncoding)result.get(1) , (CompressionType) result.get(2), props, tags, attrs, alias);
        insertRecordSingle(device+"."+tsName,  (TSDataType) result.get(0), false, alias);
        s.close();
    }
    @Test(priority = 20, dataProvider = "createSingleTimeSeriesNormal")
    public void testDeleteSingleTimeSeries_normal(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        Session s = PrepareConnection.getSession();
        s.deleteTimeseries(device+"."+tsName);
        s.close();
    }
    @DataProvider(name = "createSingleTimeSeriesError", parallel = true)
    private Iterator<Object[]> getSingleTimeSeriesError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single-error.csv").getData();
    }
    @Test(priority = 21, dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteSingleTimeSeries_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        Session s = PrepareConnection.getSession();
        try {
            s.deleteTimeseries(path);
        } finally {
            s.close();
        }
    }
    @Test(priority = 30, dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testCreateSingleTimeSeries_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<Object> result = translateString2Type(datatypeStr, encodingStr, compressStr);
        Session s = PrepareConnection.getSession();
        s.createTimeseries(path, (TSDataType) result.get(0), (TSEncoding) result.get(1),
                (CompressionType) result.get(2), props, tags, attrs, alias);
        s.close();
    }
    @Test(priority = 40, dataProvider = "createSingleTimeSeriesNormal")
    public void testCreateSingleTimeSeriesAligned_normal(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<Object> result = translateString2Type(datatypeStr, encodingStr, compressStr);
        List<String> tsList = new ArrayList<>(1);
        List<String> aliasList = new ArrayList<>(1);
        List<TSDataType> tsDataTypes = new ArrayList<>(1);
        List<TSEncoding> tsEncodings = new ArrayList<>(1);
        List<CompressionType> compressionTypes = new ArrayList<>(1);
        tsDataTypes.add((TSDataType) result.get(0));
        tsEncodings.add((TSEncoding) result.get(1));
        compressionTypes.add((CompressionType) result.get(2));
        tsList.add(tsName);
        aliasList.add(alias);
//        out.println(device+"."+tsName+" datatype:"+tsDataTypes +" encoding:"+tsEncodings+ " compress:"+compressionTypes +" props:"+props+" tags:"+ tags+" attrs:"+ attrs+" alias:"+ aliasList);
        Session s = PrepareConnection.getSession();
        s.createAlignedTimeseries(device, tsList, tsDataTypes, tsEncodings, compressionTypes, aliasList);
        s.close();
    }
    @DataProvider(name = "deleteNormalWildcard")
    private Iterator<Object[]> deleteNormal_wildcard() throws IOException {
        return new CustomDataProvider().load("data/timeseries-delete.csv").getData();
    }
   @Test(priority = 41, dataProvider = "deleteNormalWildcard")
    public void testDeleteMultiTS(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    /**
     * toDo
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
//    @Test(priority = 42, dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
//    public void testCreateAlignedTimeSeries_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
//        List<Object> result = translateString2Type(datatypeStr, encodingStr, compressStr);
//        List<String> tsList = new ArrayList<>(1);
//        List<String> aliasList = new ArrayList<>(1);
//        List<TSDataType> tsDataTypes = new ArrayList<>(1);
//        List<TSEncoding> tsEncodings = new ArrayList<>(1);
//        List<CompressionType> compressionTypes = new ArrayList<>(1);
//        tsDataTypes.add((TSDataType) result.get(0));
//        tsEncodings.add((TSEncoding) result.get(1));
//        compressionTypes.add((CompressionType) result.get(2));
//        tsList.add(tsName);
//        aliasList.add(alias);
//        out.println(device+"."+tsName+" datatype:"+tsDataTypes +" encoding:"+tsEncodings+ " compress:"+compressionTypes +" props:"+props+" tags:"+ tags+" attrs:"+ attrs+" alias:"+ aliasList);
//        session.createAlignedTimeseries(device, tsList, tsDataTypes, tsEncodings, compressionTypes, aliasList);
//    }


    @Test(priority = 50)
    public void testCreateTimeSeries_10props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(10);
    }
    @Test(priority = 51)
    public void testCreateTimeSeries_1props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(1);
    }

    // TIMECHODB-126
    @Test(priority = 52, expectedExceptions = StatementExecutionException.class)
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
    public void testDeleteError(String path, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        Session s = PrepareConnection.getSession();
        try {
            s.deleteTimeseries(path);
        } finally {
            s.close();
        }
    }

    @DataProvider(name = "deleteTimeSeriesMultiError")
    private Iterator<Object[]> getDeleteTimeSeriesMultiError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-deleteG-error.csv").getData();
    }

    @Test(priority = 80, dataProvider = "deleteTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteMultiTimeSeries_error(String errTS, String total_str, String position_str, String msg) throws IoTDBConnectionException, StatementExecutionException, InterruptedException, IOException {
        int total = Integer.parseInt(total_str);
        int position = Integer.parseInt(position_str);
        List<String> tsList = new ArrayList<>(total);
        List<String> tsExists = new ArrayList<>(total-1);
        List<TSDataType> dataTypes = new ArrayList<>(total-1);
        List<TSEncoding> encodings = new ArrayList<>(total-1);
        List<CompressionType> compressionTypes = new ArrayList<>(total-1);
        if (position == 1) {
            tsList.add(errTS);
            for (int i = 1; i < total; i++) {
                tsList.add(normalTSs.get(i-1));
                tsExists.add(normalTSs.get(i-1));
            }
        } else if (position == total) {
            for (int i = 0; i < total-1; i++) {
                tsList.add(normalTSs.get(i));
                tsExists.add(normalTSs.get(i));
            }
            tsList.add(errTS);
        } else {
            int i = 0;
            for (; i < position; i++) {
                tsList.add(normalTSs.get(i));
                tsExists.add(normalTSs.get(i));
            }
            tsList.add(errTS);
            i++;
            for (; i < total; i++) {
                tsList.add(normalTSs.get(i-1));
                tsExists.add(normalTSs.get(i-1));
            }
        }
        for (int i = 0; i < tsExists.size(); i++) {
            dataTypes.add(TSDataType.FLOAT);
            encodings.add(TSEncoding.TS_2DIFF);
            compressionTypes.add(CompressionType.LZ4);
        }
        session.createMultiTimeseries(tsExists, dataTypes, encodings, compressionTypes, null, null, null, null);
        int expectCount = getTimeSeriesCount("", verbose) - total + 1 ;
        int actualCount = getTimeSeriesCount("", verbose);
        session.deleteTimeseries(tsList);
//        Thread.sleep(1000);
        assert expectCount == actualCount: "删除后 actual=:" + actualCount + ", expect=" +expectCount + ", total="+total;
    }

}
