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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    @Test(priority = 20, expectedExceptions = IoTDBConnectionException.class)
    public void testCreateTimeSeries_nullPath_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("path");
    }
    // TIMECHODB-121
    @Test(priority = 21, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullDatatype_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("dataType");
    }
    @Test(priority = 22, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullEncoding_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("encoding");
    }
    @Test(priority = 23, expectedExceptions = StatementExecutionException.class)
    public void testCreateTimeSeries_nullCompress_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("compress");
    }

    @Test(priority = 24)
    public void testCreateTimeSeries_nullProps_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("props");
    }
    @Test(priority = 25)
    public void testCreateTimeSeries_nullTags_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("tags");
    }
    @Test(priority = 26)
    public void testCreateTimeSeries_nullAttrs_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("attrs");
    }
    @Test(priority = 27)
    public void testCreateTimeSeries_nullAlias_error() throws IoTDBConnectionException, StatementExecutionException {
        testCreateTS_null("alias");
    }

    private void testCreateMultiTS_null(String name) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> props = new ArrayList<>();
        List<Map<String, String>> tags = new ArrayList<>();
        List<Map<String, String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        String path = database+"testMultiCreateNull." + name;
        TSDataType dataType = TSDataType.BOOLEAN;
        TSEncoding encoding = TSEncoding.PLAIN;
        CompressionType compress = CompressionType.UNCOMPRESSED;

        Map<String, String> prop = new HashMap<>();
        Map<String, String> tag = new HashMap<>();
        Map<String, String> attr = new HashMap<>();
        String alias_single = "test_multiCreateNull_" + name;

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
    // TIMECHODB-127
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
    @Test(expectedExceptions = StatementExecutionException.class, priority = 32)
    public void testCreateMulti_nullCompression() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("compress");
    }
    @Test(priority = 33)
    public void testCreateMulti_nullProps() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("props");
    }
    @Test(priority = 34)
    public void testCreateMulti_nullTags() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("tags");
    }
    @Test(priority = 35)
    public void testCreateMulti_nullAttrs() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("attrs");
    }
    @Test(priority = 36)
    public void testCreateMulti_nullAlias() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("alias");
    }

    // TIMECHODB-128
    @Test(priority = 40, expectedExceptions = IoTDBConnectionException.class)
    public void testDeleteTS_null() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries((String) null);
    }
    @Test(priority = 41, expectedExceptions = StatementExecutionException.class)
    public void testDeleteMultiTS_Null() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries((List<String>) null);
    }
    @Test(priority = 42, expectedExceptions = StatementExecutionException.class)
    public void testDeleteTS_empty() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries("");
    }
    @Test(priority = 43, expectedExceptions = StatementExecutionException.class)
    public void testDeleteMultiTS_empty() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(new ArrayList<>(0));
    }
    @Test(priority = 44, expectedExceptions = StatementExecutionException.class)
    public void testDeleteTS_nonExists() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(database+".d.ts");
    }
    @Test(priority = 45, expectedExceptions = StatementExecutionException.class)
    public void testDeleteMultiTS_nonExists() throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        paths.add(database + ".d.ts");
        session.deleteTimeseries(paths);
    }
    @Test(priority = 46)
    public void testDeleteMultiTS_duplicate() throws IoTDBConnectionException, StatementExecutionException {
        String name = "normalCreate";
        String tsName = database+".testNull." + name;
        testCreateTS_null(name);
        assert 1 == getTimeSeriesCount(tsName, verbose) : "创建TS成功";
        List<String> paths = new ArrayList<>(2);
        paths.add(tsName);
        paths.add(tsName);
        session.deleteTimeseries(paths);
        assert 0 == getTimeSeriesCount(tsName, verbose) : "删除TS成功";
    }
}
