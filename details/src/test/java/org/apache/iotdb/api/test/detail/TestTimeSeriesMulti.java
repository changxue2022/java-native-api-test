package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.TimeSeriesBaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class TestTimeSeriesMulti extends TimeSeriesBaseTestSuite {

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        cleanDatabases(verbose);
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(verbose);
    }

    @DataProvider(name = "createSingleTimeSeriesNormal", parallel = true)
    private Iterator<Object[]> getSingleTimeSeriesNormal() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single.csv").getData();
    }
    @DataProvider(name = "createSingleTimeSeriesError", parallel = true)
    private Iterator<Object[]> getSingleTimeSeriesError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single-error.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMulti", parallel = true)
    private Iterator<Object[]> getTimeSeriesMulti() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi.csv").getData();
    }
    @DataProvider(name = "createTimeSeriesMultiError", parallel = true)
    private Iterator<Object[]> getTimeSeriesMultiError() throws IOException {
        return new CustomDataProvider().load("data/timeseries-multi-error.csv").getData();
    }


    @Test(priority = 10, dataProvider = "createSingleTimeSeriesNormal")
    public void testCreateTimeSeriesMulti_single(String device, String tsName, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        List<TSDataType> tsDataTypes = new ArrayList<>(1);
        List<TSEncoding> tsEncodings = new ArrayList<>(1);
        List<CompressionType> compressionTypes = new ArrayList<>(1);
        List<Map<String,String>> tagList = new ArrayList<>(1);
        List<Map<String,String>> attrList = new ArrayList<>(1);
        List<String> aliasList = new ArrayList<>(1);
        List<Object> result  = translateString2Type(datatypeStr, encodingStr, compressStr);

        paths.add(device+"."+tsName);
        tsDataTypes.add((TSDataType) result.get(0));
        tsEncodings.add((TSEncoding) result.get(1));
        compressionTypes.add((CompressionType) result.get(2));
        if (tags != null) {tagList.add(tags);} else {tagList = null;}
        if (attrs != null) {attrList.add(attrs);} else {attrList = null;}
        if (alias != null) {aliasList.add(alias);} else {aliasList = null;}
        System.out.println(paths+" datatype:"+tsDataTypes +" encoding:"+tsEncodings+ " compress:"+compressionTypes +" tags:"+ tagList+" attrs:"+ attrList+" alias:"+ aliasList);
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes,
                null, tagList, attrList, aliasList);
        assert 1 == getTimeSeriesCount(device+"."+tsName, verbose) : "创建成功";
        insertRecordSingle(device+"."+tsName, (TSDataType) result.get(0), false, alias);
        session.deleteTimeseries(paths);
        assert 0 == getTimeSeriesCount(device+"."+tsName, verbose) : "清理成功";
    }
    @Test(dataProvider = "createTimeSeriesMulti", priority = 20)
    public void testCreateTimeSeriesMulti_normal(List<String> paths, List<String> tsDataTypes, List<String> tsEncodings, List<String> compressionTypes, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypeLists = new ArrayList<>(paths.size());
        List<TSEncoding> tsEncodingLists = new ArrayList<>(paths.size());
        List<CompressionType> compressionTypeLists = new ArrayList<>(paths.size());
        List<Object> result;
        for (int i = 0; i < paths.size(); i++) {
            result = translateString2Type(tsDataTypes.get(i), tsEncodings.get(i), compressionTypes.get(i));
            tsDataTypeLists.add((TSDataType) result.get(0));
            tsEncodingLists.add((TSEncoding) result.get(1));
            compressionTypeLists.add((CompressionType) result.get(2));
        }
        session.createMultiTimeseries(
                paths, tsDataTypeLists, tsEncodingLists, compressionTypeLists, null, tags, attrs, alias);
    }

    // TIMECHODB-127
    @Test(priority = 22)
    public void testCreateTimeSeriesMulti_inBatch() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String,String>> tags = new ArrayList<>();
        List<Map<String,String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();
        List<Object> result;

        for (Iterator<Object[]> it = getSingleTimeSeriesNormal(); it.hasNext();) {
            Object[] line = it.next();
            paths.add(line[0]+"."+line[1]);
            result = translateString2Type((String)line[2], (String)line[3], (String)line[4]);
            tsDataTypes.add((TSDataType) result.get(0));
            tsEncodings.add((TSEncoding) result.get(1));
            compressionTypes.add((CompressionType) result.get(2));
            tags.add((Map<String,String>)line[6]);
            attrs.add((Map<String,String>)line[7]);
            alias.add(line[8] == null ?null:line[8].toString());
//            System.out.println(line[0] +" ," +line[1]+" datatype:"+tsDataTypes +" encoding:"+tsEncodings+ " compress:"+compressionTypes +" tags:"+ tags+" attrs:"+ attrs+" alias:"+ alias);
        }
//        System.out.println("paths="+paths.size());
//        System.out.println("tsDataTypes="+tsDataTypes.size());
//        System.out.println("tsEncodings="+tsEncodings.size());
//        System.out.println("compressionTypes="+compressionTypes.size());
//        System.out.println("tags="+tags.size());
//        System.out.println("attrs="+attrs.size());
//        System.out.println("alias="+alias.size());
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, tags, attrs, alias);
        assert paths.size() == getTimeSeriesCount("", verbose)  : "createMultiTimeseries normal";
        session.deleteTimeseries(paths);
        assert 0 == getTimeSeriesCount("", verbose) : "清理成功";
    }

    @Test(priority = 43)
    public void testCreateTimeSeriesMultiAligned_inBatch() throws IOException, IoTDBConnectionException, StatementExecutionException {
        List<String> paths = null;
        List<String> deletePaths = new ArrayList<>();
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
            List<Object> result = translateString2Type((String)line[2], (String)line[3], (String)line[4]);
            tsDataTypes.add((TSDataType) result.get(0));
            tsEncodings.add((TSEncoding) result.get(1));
            compressionTypes.add((CompressionType) result.get(2));
            alias.add(line[8] == null ?null:line[8].toString());
        }
        session.deleteTimeseries(deletePaths);
//        assert 0 == getTimeSeriesCount("", verbose) : "清理成功";
    }

    @Test(priority = 25, dataProvider = "createSingleTimeSeriesError", expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeriesMulti_single_error(String path, String datatypeStr, String encodingStr, String compressStr, Map<String, String> props, Map<String, String> tags, Map<String, String> attrs, String alias, String msg) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> attrList = new ArrayList<>();
        List<Map<String,String>> tagList = new ArrayList<>();
        List<String> aliasList = new ArrayList<>();

        paths.add(path);
        List<Object> result = translateString2Type(datatypeStr, encodingStr, compressStr);
        tsDataTypes.add((TSDataType) result.get(0));
        tsEncodings.add((TSEncoding) result.get(1));
        compressionTypes.add((CompressionType) result.get(2));
        attrList.add(attrs);
        tagList.add(tags);
        aliasList.add(alias);
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, tagList, attrList, aliasList);
    }
    @Test(priority = 26, dataProvider = "createTimeSeriesMultiError", expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeriesMulti_error(List<String> paths, List<String> tsDataTypeLists, List<String> tsEncodingLists, List<String> compressionTypeLists, List<Map<String,String>> props, List<Map<String,String>> tags, List<Map<String,String>> attrs, List<String> alias) throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            List<Object> result = translateString2Type(tsDataTypeLists.get(i), tsEncodingLists.get(i), compressionTypeLists.get(i));
            tsDataTypes.add((TSDataType) result.get(0));
            tsEncodings.add((TSEncoding) result.get(1));
            compressionTypes.add((CompressionType) result.get(2));
        }
        session.createMultiTimeseries(
                paths, tsDataTypes, tsEncodings, compressionTypes, null, tags, attrs, alias);
    }


}
