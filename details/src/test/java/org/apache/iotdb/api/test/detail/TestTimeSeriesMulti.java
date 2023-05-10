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

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
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
    @DataProvider(name = "createSingleTimeSeriesNormal")
    private Iterator<Object[]> getSingleTimeSeriesNormal() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMulti", parallel = true)
    private Iterator<Object[]> getTimeSeriesMulti() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMultiError", parallel = true)
    private Iterator<Object[]> getTimeSeriesMultiError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi-error.csv").getData();
    }

    @Test(dataProvider = "createTimeSeriesMulti", priority = 20)
    public void testCreateTimeSeriesMulti_normal(List<String> paths, List<String> tsDataTypes, List<String> tsEncodings, List<String> compressionTypes, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypeLists = new ArrayList<>(paths.size());
        List<TSEncoding> tsEncodingLists = new ArrayList<>(paths.size());
        List<CompressionType> compressionTypeLists = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            translateString2Type(tsDataTypes.get(i), tsEncodings.get(i), compressionTypes.get(i));
            tsDataTypeLists.add(dataType);
            tsEncodingLists.add(encoding);
            compressionTypeLists.add(compressionType);
        }
        session.createMultiTimeseries(
                paths, tsDataTypeLists, tsEncodingLists, compressionTypeLists, null, tags, attrs, alias);
    }
    @Test(priority = 22)
    public void testCreateTimeSeriesMulti_inBatch() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String,String>> tags = new ArrayList<>();
        List<Map<String,String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        for (Iterator<Object[]> it = getSingleTimeSeriesNormal(); it.hasNext();) {
            Object[] line = it.next();
            paths.add(line[0].toString()+"."+line[1]);
            translateString2Type((String)line[2], (String)line[3], (String)line[4]);
            tsDataTypes.add(dataType);
            tsEncodings.add(encoding);
            compressionTypes.add(compressionType);
            tags.add((Map<String,String>)line[6]);
            attrs.add((Map<String,String>)line[7]);
            alias.add(line[8] == null ?null:line[8].toString());
        }
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, tags, attrs, alias);
        assert paths.size() == getTimeSeriesCount("", verbose)  : "createMultiTimeseries normal";
        session.deleteTimeseries(paths);
        assert 0 == getTimeSeriesCount("", verbose) : "清理成功";
    }
    @Test(priority = 23)
    public void testCreateTimeSeriesMultiAligned_inBatch() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<String> paths = null;
        List<String> deletePaths = null;
        List<TSDataType> tsDataTypes = null;
        List<TSEncoding> tsEncodings = null;
        List<CompressionType> compressionTypes = null;
        List<String> alias = null;
        String device = "";
        for (Iterator<Object[]> it = getSingleTimeSeriesNormal(); it.hasNext();) {
            Object[] line = it.next();
            deletePaths.add(line[0] + "." + line[1]);
            if (!device.equals(line[0])) {
                if (!device.isEmpty()) {
                    session.createAlignedTimeseries(device, paths, tsDataTypes, tsEncodings, compressionTypes, alias);
                    assert paths.size() == getTimeSeriesCount(device+".**", verbose):"创建 aligned序列成功";
                    insertRecordMulti(device,paths,tsDataTypes, 10, true, alias);
                }
                paths = new ArrayList<>();
                tsDataTypes = new ArrayList<>();
                tsEncodings = new ArrayList<>();
                compressionTypes = new ArrayList<>();
                alias = new ArrayList<>();
                device = line[0].toString();
            }
            paths.add(line[1].toString());
            translateString2Type((String)line[2], (String)line[3], (String)line[4]);
            tsDataTypes.add(dataType);
            tsEncodings.add(encoding);
            compressionTypes.add(compressionType);
            alias.add(line[8] == null ?null:line[8].toString());
        }
        session.deleteTimeseries(deletePaths);
        assert 0 == getTimeSeriesCount("", verbose) : "清理成功";
    }
    @Test(dataProvider = "createTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class, priority = 25)
    public void testCreateTimeSeriesMulti_error(List<String> paths, List<String> tsDataTypeLists, List<String> tsEncodingLists, List<String> compressionTypeLists, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            translateString2Type(tsDataTypeLists.get(i), tsEncodingLists.get(i), compressionTypeLists.get(i));
            tsDataTypes.add(dataType);
            tsEncodings.add(encoding);
            compressionTypes.add(compressionType);
        }
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, tags, attrs, alias);
    }
    @Test(priority = 30)
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
                    assert getTimeSeriesCount("", verbose) == meaurements.size() : "createAlignedTimeseries normal: "+deviceId;
                }
                oldDeviceId = deviceId;
            }
            meaurements.add(line[0].toString());
            translateString2Type((String)line[1], (String)line[2], (String)line[3]);
            dataTypes.add(dataType);
            encodings.add(encoding);
            compressionTypes.add(compressionType);
            tags.add((Map<String, String>)line[5]);
            attrs.add((Map<String, String>)line[6]);
            alias.add(line[7].toString());
        }
    }

    @Test(dataProvider = "createTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class, priority = 42)
    public void testCreateAlignedTimeSeriesMulti_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        List<String> meaurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> tags_list = new ArrayList<>();
        List<Map<String, String>> attrs_list = new ArrayList<>();
        List<String> alias_list = new ArrayList<>();
        String deviceId = path.substring(0, path.lastIndexOf('.'));
        meaurements.add(path);
        translateString2Type(datatypeStr, encodingStr, compressStr);
        dataTypes.add(dataType);
        encodings.add(encoding);
        compressionTypes.add(compressionType);
        tags_list.add(tags);
        attrs_list.add(attrs);
        alias_list.add(alias);
        session.createAlignedTimeseries(deviceId, meaurements, dataTypes, encodings, compressionTypes, alias_list, null, tags_list);
        assert getTimeSeriesCount("", false) == 0 : "createAlignedTimeseries error: "+deviceId;
    }




}
