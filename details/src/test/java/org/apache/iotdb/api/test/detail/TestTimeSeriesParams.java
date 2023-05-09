package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class TestTimeSeriesParams extends BaseTestSuite {
    private String database = "root.ts";
    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        if(!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase("root.ts");
    }
    private void testCreateTS_null(String name) throws IoTDBConnectionException, StatementExecutionException {
        String path = database+".testNull." + name;
        TSDataType dataType = TSDataType.FLOAT;
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
    @Test(priority = 80, expectedExceptions = IoTDBConnectionException.class)
    public void testCreateTimeSeries_nullPath_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("path");
    }
    // TIMECHODB-121
    @Test(priority = 81, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullDatatype_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("dataType");
    }
    @Test(priority = 82, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullEncoding_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("encoding");
    }
    @Test(priority = 83, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullCompress_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("compress");
    }

    @Test(priority = 84)
    public void testCreateTimeSeries_nullProps_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("props");
    }
    @Test(priority = 85)
    public void testCreateTimeSeries_nullTags_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("tags");
    }
    @Test(priority = 86)
    public void testCreateTimeSeries_nullAttrs_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("attrs");
    }
    @Test(priority = 87)
    public void testCreateTimeSeries_nullAlias_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("alias");
    }

}
