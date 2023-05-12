package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.TimeSeriesBaseTestSuite;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.System.out;

public class TestCreateAlignedTimeseries extends TimeSeriesBaseTestSuite {
    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(verbose);
    }
    @DataProvider(name = "createSingleTimeSeriesNormal", parallel = false)
    private Iterator<Object[]> getSingleTimeSeriesNormal() throws IOException {
        return new CustomDataProvider().load("data/timeseries-single-bk.csv").getData();
    }
    @DataProvider(name = "deleteNormalWildcard")
    private Iterator<Object[]> deleteNormal_wildcard() throws IOException {
        return new CustomDataProvider().load("data/timeseries-delete.csv").getData();
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
        out.println(device+"."+tsName+" datatype:"+tsDataTypes +" encoding:"+tsEncodings+ " compress:"+compressionTypes +" props:"+props+" tags:"+ tags+" attrs:"+ attrs+" alias:"+ aliasList);
        session.createAlignedTimeseries(device, tsList, tsDataTypes, tsEncodings, compressionTypes, aliasList);
    }
    @Test(priority = 41, dataProvider = "deleteNormalWildcard")
    public void testDeleteMultiTS(String path, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(path);
    }

}
