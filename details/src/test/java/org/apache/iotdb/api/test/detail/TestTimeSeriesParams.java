package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTimeSeriesParams extends BaseTestSuite {
    private String database = "root.params.ts";
    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(database);
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase(database);
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
//    @Test(priority = 20, expectedExceptions = StatementExecutionException.class)
//    public void testCreateTimeSeries_nullPath_error() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateTS_null("path");
//    }
//    // TIMECHODB-121
//    @Test(priority = 21, expectedExceptions = StatementExecutionException.class)
//    public void testCreateTimeSeries_nullDatatype_error() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateTS_null("dataType");
//    }
//    @Test(priority = 22, expectedExceptions = StatementExecutionException.class)
//    public void testCreateTimeSeries_nullEncoding_error() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateTS_null("encoding");
//    }
//    @Test(priority = 23, expectedExceptions = StatementExecutionException.class)
//    public void testCreateTimeSeries_nullCompress_error() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateTS_null("compress");
//    }

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

    private void testCreateMultiTS_null(String name, boolean isAligned, boolean nullInList) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        List<Map<String, String>> props = new ArrayList<>();
        List<Map<String, String>> tags = new ArrayList<>();
        List<Map<String, String>> attrs = new ArrayList<>();
        List<String> alias = new ArrayList<>();

        String device = database+".testMultiCreateNull_true_"+nullInList;
        String path = "";
        if (isAligned) {
            path = name;
        } else {
            path = database+".testMultiCreateNull_false_"+nullInList +"_" + name;
        }
        TSDataType dataType = TSDataType.BOOLEAN;
        TSEncoding encoding = TSEncoding.PLAIN;
        CompressionType compress = CompressionType.UNCOMPRESSED;

        Map<String, String> prop = new HashMap<>();
        Map<String, String> tag = new HashMap<>();
        Map<String, String> attr = new HashMap<>();
        String alias_single = "test_multiCreateNull_"+isAligned+"_" + nullInList +"_"+ name;

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
                if (nullInList) {
                    paths.clear();
                    paths.add(null);
                } else {
                    paths = null;
                }
                break;
            case "dataType":
                if (nullInList) {
                    dataTypes.clear();
                    dataTypes.add(null);
                } else {
                    dataTypes = null;
                }
                break;
            case "encoding":
                if (nullInList) {
                    encodings.clear();
                    encodings.add(null);
                } else {
                    encodings = null;
                }
                break;
            case "compress":
                if (nullInList) {
                    compressionTypes.clear();
                    compressionTypes.add(null);
                } else {
                    compressionTypes = null;
                }
                break;
            case "props":
                if (nullInList) {
                    props.clear();
                    props.add(null);
                } else {
                    props = null;
                }
                break;
            case "tags":
                if (nullInList) {
                    tags.clear();
                    tags.add(null);
                } else {
                    tags = null;
                }
                break;
            case "attrs":
                if (nullInList) {
                    attrs.clear();
                    attrs.add(null);
                } else {
                    attrs = null;
                }
                break;
            case "alias":
                if (nullInList) {
                    alias.clear();
                    alias.add(null);
                } else {
                    alias = null;
                }
                break;
        }
        if (isAligned) {
            session.createAlignedTimeseries(device, paths, dataTypes,
                    encodings,
                    compressionTypes,
                    alias,
                    tags,
                    attrs
            );
        } else {
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
    }
    // TIMECHODB-127
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 50)
//    public void testCreateAligned_nullPaths() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("path", true, false);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 51)
//    public void testCreatAligned_nullDataTypes() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("dataType", true, false);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 52)
//    public void testCreateAligned_nullEncoding() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("encoding", true, false);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 53)
//    public void testCreateAligned_nullCompression() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("compress", true, false);
//    }
    @Test(priority = 54)
    public void testCreateAligned_nullProps() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("props", true, false);
    }
    @Test(priority = 55)
    public void testCreateAligned_nullTags() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("tags", true, false);
    }
    @Test(priority = 56)
    public void testCreateAligned_nullAttrs() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("attrs", true, false);
    }
    @Test(priority = 57)
    public void testCreateAligned_nullAlias() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("alias", true, false);
    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 30)
//    public void testCreateMulti_nullPaths() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("path", false, false);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 31)
//    public void testCreateMulti_nullDataTypes() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("dataType", false, false);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 32)
//    public void testCreateMulti_nullEncoding() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("encoding", false, false);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 33)
//    public void testCreateMulti_nullCompression() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("compress", false, false);
//    }
    @Test(priority = 34)
    public void testCreateMulti_nullProps() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("props", false, false);
    }
    @Test(priority = 35)
    public void testCreateMulti_nullTags() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("tags", false, false);
    }
    @Test(priority = 36)
    public void testCreateMulti_nullAttrs() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("attrs", false, false);
    }
    @Test(priority = 37)
    public void testCreateMulti_nullAlias() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("alias", false, false);
    }
//   @Test(expectedExceptions = StatementExecutionException.class, priority = 60)
//    public void testCreateAligned_pathsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("path", true, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 61)
//    public void testCreatAligned_dataTypesNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("dataType", true, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 62)
//    public void testCreateAligned_encodingNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("encoding", true, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 63)
//    public void testCreateAligned_CompressionNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("compress", true, true);
//    }
    @Test(priority = 64)
    public void testCreateAligned_PropsNullInList() throws IoTDBConnectionException, StatementExecutionException {
        testCreateMultiTS_null("props", true, true);
    }
//    @Test(priority = 65)
//    public void testCreateAligned_TagsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("tags", true, true);
//    }
//    @Test(priority = 66)
//    public void testCreateAligned_AttrsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("attrs", true, true);
//    }
//    @Test(priority = 67)
//    public void testCreateAligned_AliasNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("alias", true, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 70)
//    public void testCreateMulti_PathsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("path", false, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 71)
//    public void testCreateMulti_DataTypesNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("dataType", false, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 72)
//    public void testCreateMulti_EncodingNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("encoding", false, true);
//    }
//    @Test(expectedExceptions = StatementExecutionException.class, priority = 73)
//    public void testCreateMulti_CompressionNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("compress", false, true);
//    }
//    @Test(priority = 74)
//    public void testCreateMulti_PropsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("props", false, true);
//    }
//    @Test(priority = 75)
//    public void testCreateMulti_TagsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("tags", false, true);
//    }
//    @Test(priority = 76)
//    public void testCreateMulti_AttrsNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("attrs", false, true);
//    }
//    @Test(priority = 77)
//    public void testCreateMulti_AliasNullInList() throws IoTDBConnectionException, StatementExecutionException {
//        testCreateMultiTS_null("alias", false, true);
//    }
//
//    // TIMECHODB-128
////    @Test(priority = 40, expectedExceptions = StatementExecutionException.class)
////    public void testDeleteTS_null() throws IoTDBConnectionException, StatementExecutionException {
////        session.deleteTimeseries((String) null);
////    }
//    @Test(priority = 41, expectedExceptions = StatementExecutionException.class)
//    public void testDeleteMultiTS_Null() throws IoTDBConnectionException, StatementExecutionException {
//        session.deleteTimeseries((List<String>) null);
//    }
//    @Test(priority = 42, expectedExceptions = StatementExecutionException.class)
//    public void testDeleteTS_empty() throws IoTDBConnectionException, StatementExecutionException {
//        session.deleteTimeseries("");
//    }
//    @Test(priority = 43, expectedExceptions = StatementExecutionException.class)
//    public void testDeleteMultiTS_empty() throws IoTDBConnectionException, StatementExecutionException {
//        session.deleteTimeseries(new ArrayList<>(0));
//    }
//    @Test(priority = 44, expectedExceptions = StatementExecutionException.class)
//    public void testDeleteTS_nonExists() throws IoTDBConnectionException, StatementExecutionException {
//        session.deleteTimeseries(database+".d.ts");
//    }
//    @Test(priority = 45, expectedExceptions = StatementExecutionException.class)
//    public void testDeleteMultiTS_nonExists() throws IoTDBConnectionException, StatementExecutionException {
//        List<String> paths = new ArrayList<>(1);
//        paths.add(database + ".d.ts");
//        session.deleteTimeseries(paths);
//    }
//    @Test(priority = 46)
//    public void testDeleteMultiTS_duplicate() throws IoTDBConnectionException, StatementExecutionException {
//        String name = "normalCreate";
//        String tsName = database+".testNull." + name;
//        testCreateTS_null(name);
//        assert 1 == getTimeSeriesCount(tsName, verbose) : "创建TS成功";
//        List<String> paths = new ArrayList<>(2);
//        paths.add(tsName);
//        paths.add(tsName);
//        session.deleteTimeseries(paths);
//        assert 0 == getTimeSeriesCount(tsName, verbose) : "删除TS成功";
//    }
}
