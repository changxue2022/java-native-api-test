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

public class TestTimeSeriesMulti extends BaseTestSuite {
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
    public void createTimeSeriesIgnoreError(int count) throws IoTDBConnectionException, IOException {
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
    @DataProvider(name = "createSingleTimeSeriesNormal")
    public Iterator<Object[]> getSingleTimeSeriesNormal() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMulti")
    public Iterator<Object[]> getTimeSeriesMulti() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMultiError")
    public Iterator<Object[]> getTimeSeriesMultiError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi-error.csv").getData();
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


    @Test(dataProvider = "createTimeSeriesMulti", priority = 20)
    public void testCreateTimeSeriesMulti_normal(List<String> paths, List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, props, tags, attrs, alias);
    }
    @Test(priority = 22)
    public void testCreateTimeSeriesMulti() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String,String>> props = new ArrayList<>();
        List<Map<String,String>> tags = new ArrayList<>();
        List<Map<String,String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        for (Iterator<Object[]> it = getSingleTimeSeriesNormal(); it.hasNext();) {
            Object[] line = it.next();
            paths.add(line[0].toString());
            translateString2Type((String)line[1], (String)line[2], (String)line[3]);
            tsDataTypes.add(dataType);
            tsEncodings.add(encoding);
            compressionTypes.add(compressionType);
            props.add((Map<String,String>)line[4]);
            tags.add((Map<String,String>)line[5]);
            attrs.add((Map<String,String>)line[6]);
            alias.add(line[7].toString());
        }
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, props, tags, attrs, alias);
        assert getTimeSeriesCount("root.**", true) == paths.size() : "createMultiTimeseries normal";
        session.deleteTimeseries(paths);
    }
    @Test(dataProvider = "createTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class, priority = 25)
    public void testCreateTimeSeriesMulti_error(List<String> paths, List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, props, tags, attrs, alias);
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 30)
    public void testCreateMulti_nullPaths() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("path");
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 31)
    public void testCreateMulti_nullDataTypes() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("dataType");
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 32)
    public void testCreateMulti_nullEncoding() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("encoding");
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 33)
    public void testCreateMulti_nullProps() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("props");
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 34)
    public void testCreateMulti_nullTags() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("tags");
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 35)
    public void testCreateMulti_nullAttrs() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("attrs");
    }
    @Test(expectedExceptions = StatementExecutionException.class, priority = 36)
    public void testCreateMulti_nullAlias() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("alias");
    }
    @Test
    public void testDeleteAll() throws IoTDBConnectionException, StatementExecutionException {
        // cleanup environment
        session.deleteTimeseries("root.**");
    }

    @Test(enabled = true, priority = 40)
    public void testAddAlignedTimeSeries_normal() throws IoTDBConnectionException, StatementExecutionException, IOException {
        String deviceId = "";
        String oldDeviceId = "";
        List<String> meaurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> props = new ArrayList<>();
        List<Map<String, String>> tags = new ArrayList<>();
        List<Map<String, String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();
        for (Iterator<Object[]> it = getSingleTimeSeriesNormal(); it.hasNext();) {
            Object[] line = it.next();
            deviceId = line[0].toString().substring(0, line[0].toString().lastIndexOf('.'));
            if (!deviceId.equals(oldDeviceId)) {
                if (!oldDeviceId.isEmpty()) {
                    session.createAlignedTimeseries(deviceId, meaurements, dataTypes, encodings, compressionTypes, alias, props, tags);
                    assert getTimeSeriesCount("root.**", true) == meaurements.size() : "createAlignedTimeseries normal: "+deviceId;
                }
                oldDeviceId = deviceId;
            }
            meaurements.add(line[0].toString());
            translateString2Type((String)line[1], (String)line[2], (String)line[3]);
            dataTypes.add(dataType);
            encodings.add(encoding);
            compressionTypes.add(compressionType);
            props.add((Map<String, String>)line[4]);
            tags.add((Map<String, String>)line[5]);
            attrs.add((Map<String, String>)line[6]);
            alias.add(line[7].toString());
        }
    }

    /**
     * toDo
     */
    @Test(expectedExceptions = StatementExecutionException.class, priority = 42)
    public void testCreateAlignedTimeSeriesMulti_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        List<String> meaurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> props_list = new ArrayList<>();
        List<Map<String, String>> tags_list = new ArrayList<>();
        List<Map<String, String>> attrs_list = new ArrayList<>();
        List<String> alias_list = new ArrayList<>();
        String deviceId = path.substring(0, path.lastIndexOf('.'));
        meaurements.add(path);
        translateString2Type(datatypeStr, encodingStr, compressStr);
        dataTypes.add(dataType);
        encodings.add(encoding);
        compressionTypes.add(compressionType);
        props_list.add(props);
        tags_list.add(tags);
        attrs_list.add(attrs);
        alias_list.add(alias);
        session.createAlignedTimeseries(deviceId, meaurements, dataTypes, encodings, compressionTypes, alias_list, props_list, tags_list);
        assert getTimeSeriesCount("root.**", false) == 0 : "createAlignedTimeseries error: "+deviceId;
    }


    @Test(dataProvider = "deleteTimeSeriesError", enabled = true, expectedExceptions = StatementExecutionException.class, priority = 43)
    public void testDeleteTimeSeries_error(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

    @Test(enabled = true, priority = 45)
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
        System.out.println(ts_list);
        createTimeSeriesIgnoreError(total-1);
        int expectCount = getTimeSeriesCount("", true) - total + 1 ;
        session.deleteStorageGroups(ts_list);
        int actualCount = getTimeSeriesCount("", true);
        System.out.println(msg);
        Thread.sleep(1000);
        assert expectCount == actualCount: "删除后 actual=:" + actualCount + ", expect=" +expectCount + ", total="+total;

    }

}
