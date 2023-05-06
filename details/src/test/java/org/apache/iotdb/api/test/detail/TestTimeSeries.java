package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

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
        normalTSs = new CustomDataProvider().getFirstColumns("data/timeseries-single.csv");
        if (checkStroageGroupExists("")) {
            session.deleteStorageGroup("root.**");
        }
    }

    public void translateString2Type(String datatypeStr, String encodingStr, String compressStr) {
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

    @DataProvider(name = "createSingleTimeSeriesNormal")
    public Iterator<Object[]> getSingleTimeSeriesNormal() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single.csv").getData();
    }

    @DataProvider(name = "createSingleTimeSeriesError")
    public Iterator<Object[]> getSingleTimeSeriesError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single-error.csv").getData();
    }

    /**
     * 工具类，测试props,tags,attrs个数
     *
     * @param value
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    public void testCreateTimeSeries_tags(int value) throws IoTDBConnectionException, StatementExecutionException {
        String path = "root.t1.pros" + value;
        for (int i = 0; i < value; i++) {
            props.put("prop_" + i, "弹出模板" + i);
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
    public void testCreateTS_null(String name) throws IoTDBConnectionException, StatementExecutionException {
        String path = "root.sg.testNull." + name;
        TSDataType dataType = TSDataType.BOOLEAN;
        TSEncoding encoding = TSEncoding.PLAIN;
        CompressionType compress = CompressionType.UNCOMPRESSED;

        Map<String, String> props = new HashMap<>();
        Map<String, String> tags = new HashMap<>();
        Map<String, String> attrs = new HashMap<>();
        String alias = "test_null_" + name;
        switch (name) {
            case "path":
                path = null;
                break;
            case "dataType":
                dataType = null;
                break;
            case "encoding":
                encoding = null;
                break;
            case "compress":
                compress = null;
                break;
            case "props":
                props = null;
                break;
            case "tags":
                tags = null;
                break;
            case "attrs":
                attrs = null;
                break;
            case "alias":
                alias = null;
                break;
        }
        session.createTimeseries(
                path,
                dataType,
                encoding,
                compress,
                props,
                tags,
                attrs,
                alias
        );
    }

    @Test(dataProvider = "createSingleTimeSeriesNormal")
    public void testCreateSingleTimeSeries_normal(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        translateString2Type(datatypeStr, encodingStr, compressStr);
        session.createTimeseries(path, dataType, encoding, compressionType, props, tags, attrs, alias);
    }
    @Test(dataProvider = "createSingleTimeSeriesNormal")
    public void testDeleteSingleTimeSeries_normal(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        translateString2Type(datatypeStr, encodingStr, compressStr);
        out.println(msg);
        session.deleteTimeseries(path);
    }

    @Test(dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testCreateSingleTimeSeries_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        translateString2Type(datatypeStr, encodingStr, compressStr);
        session.createTimeseries(path, dataType, encoding, compressionType, props, tags, attrs, alias);
    }

    @Test
    public void testCreateTimeSeries_10props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(10);
    }

    @Test
    public void testCreateTimeSeries_100props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(100);
    }

    @Test
    public void testCreateTimeSeries_max_props() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(700); //默认值：tag_attribute_total_size=700
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_GTprops_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTimeSeries_tags(701);
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullPath_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("path");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullDatatype_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("dataType");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullEncoding_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("encoding");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullCompress_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("compress");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullProps_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("props");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullTags_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("tags");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullAttrs_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("attrs");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullAlias_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("alias");
    }


}
