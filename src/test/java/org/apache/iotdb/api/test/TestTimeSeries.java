package org.apache.iotdb.api.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.test.utils.CustomDataProvider;
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
            case "LZO":
                compressionType = CompressionType.LZO;
                break;
            case "SDT":
                compressionType = CompressionType.SDT;
                break;
            case "PAA":
                compressionType = CompressionType.PAA;
                break;
            case "PLA":
                compressionType = CompressionType.PLA;
                break;
            case "lz4":
                compressionType = CompressionType.LZ4;
                break;
        }
    }
    public void createTimeSeriesIgnoreError(int count) throws IoTDBConnectionException, IOException {
        Iterator<Object[]> lines = new CustomDataProvider().load("data/timeseries-single.csv").getData();
        for (int i=0; i <= count && lines.hasNext(); i++) {
            Object[] line = lines.next();
            out.println(line);
            try {
                translateString2Type((String)line[1], (String)line[2], (String)line[3]);
                session.createTimeseries((String)line[0], dataType, encoding, compressionType, new HashMap<>(), new HashMap<>(), new HashMap<>(), (String)line[7]);
            } catch (StatementExecutionException e) {
            }
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
    @DataProvider(name = "createTimeSeriesMulti")
    public Iterator<Object[]> getTimeSeriesMulti() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMultiError")
    public Iterator<Object[]> getTimeSeriesMultiError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi-error.csv").getData();
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
        out.println("props=" + props.size());
        out.println("tags=" + tags.size());
        out.println("attrs=" + attrs.size());
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
    public void testCreateMultiTS_null(String name) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> props = new ArrayList<>();
        List<Map<String, String>> tags = new ArrayList<>();
        List<Map<String, String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        String path = "root.sgMulti.testNull." + name;
        TSDataType dataType = TSDataType.BOOLEAN;
        TSEncoding encoding = TSEncoding.PLAIN;
        CompressionType compress = CompressionType.UNCOMPRESSED;

        Map<String, String> prop = new HashMap<>();
        Map<String, String> tag = new HashMap<>();
        Map<String, String> attr = new HashMap<>();
        String alias_single = "test_null_" + name;

        paths.add(path);
        dataTypes.add(dataType);
        encodings.add(encoding);
        compressionTypes.add(compress);
        props.add(prop);
        tags.add(tag);
        attrs.add(attr);
        alias.add(alias_single);

        switch (name) {
            case "path":
                paths = null;
                break;
            case "dataType":
                dataTypes = null;
                break;
            case "encoding":
                encodings = null;
                break;
            case "compress":
                compressionTypes = null;
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
        session.createMultiTimeseries(
                paths,
                dataTypes,
                encodings,
                compressionTypes,
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

    @Test(dataProvider = "createTimeSeriesMulti")
    public void testCreateTimeSeriesMulti(List<String> paths, List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, props, tags, attrs, alias);
    }
    @Test(dataProvider = "createTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeriesMulti_error(List<String> paths, List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, props, tags, attrs, alias);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullPaths() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("path");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullDataTypes() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("dataType");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullEncoding() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("encoding");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullProps() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("props");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullTags() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("tags");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullAttrs() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("attrs");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateMulti_nullAlias() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("alias");
    }

    @Test(dataProvider = "getAlignedTimeSeriesNormal", enabled = false)
    public void testAlignedTS(String deviceId, List<String> meaurements, List<TSDataType> dataTypes, List<TSEncoding> encodings, List<CompressionType> compressionTypes, List<Map<String, String>> props, List<Map<String, String>> tags, List<Map<String, String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        session.createAlignedTimeseries(deviceId, meaurements, dataTypes, encodings, compressionTypes, alias, props, tags);
    }

    @Test(dataProvider = "deleteTimeSeriesNormal", enabled = false)
    public void testDeleteTimeSeries_normal(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    @Test(dataProvider = "deleteTimeSeriesError", enabled = false, expectedExceptions = StatementExecutionException.class)
    public void testDeleteTimeSeries_error(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    @Test(enabled = false)
    public void testDeleteMultiTimeSeries_normal(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }
    @Test(enabled = false)
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
        out.println(ts_list);
        createTimeSeriesIgnoreError(total-1);
        int expectCount = getTimeSeriesCount("", true) - total + 1 ;
        session.deleteStorageGroups(ts_list);
        int actualCount = getTimeSeriesCount("", true);
        out.println(msg);
        sleep(1000);
        assert expectCount == actualCount: "删除后 actual=:" + actualCount + ", expect=" +expectCount + ", total="+total;

    }


    @Test(enabled = false)
    public void test() throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("root.sg1.d2.s1");
        paths.add("root.sg1.d2.s2");
        List<TSDataType> tsDataTypes = new ArrayList<>();
        tsDataTypes.add(TSDataType.INT64);
        tsDataTypes.add(TSDataType.INT64);
        List<TSEncoding> tsEncodings = new ArrayList<>();
        tsEncodings.add(TSEncoding.RLE);
        tsEncodings.add(TSEncoding.RLE);
        List<CompressionType> compressionTypes = new ArrayList<>();
        compressionTypes.add(CompressionType.SNAPPY);
        compressionTypes.add(CompressionType.SNAPPY);

        List<Map<String, String>> tagsList = new ArrayList<>();
        Map<String, String> tags = new HashMap<>();
        tags.put("unit", "kg");
        tagsList.add(tags);
        tagsList.add(tags);

        List<Map<String, String>> attributesList = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("minValue", "1");
        attributes.put("maxValue", "100");
        attributesList.add(attributes);
        attributesList.add(attributes);

        List<String> alias = new ArrayList<>();
        alias.add("weight1");
        alias.add("weight2");

        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList, attributesList, alias);

    }
//    @Test
    public void testInsert() throws IoTDBConnectionException, StatementExecutionException {
        String deviceId = "root.t";
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        measurements.add("c");
//        measurements.add("s2");
//        measurements.add("s3");
        types.add(TSDataType.BOOLEAN);
//        types.add(TSDataType.INT32);
//        types.add(TSDataType.INT64);

        List<Object> values = new ArrayList<>();
        values.add(true);
//            values.add(2L);
//            values.add(3L);
        session.insertRecord(deviceId, new Date().getTime(), measurements, types, values);

    }
}
