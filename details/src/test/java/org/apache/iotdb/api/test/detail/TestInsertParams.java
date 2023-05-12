package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static java.lang.System.out;

/**
 * insert params test cases
 * 各个参数的null,空值check
 */
public class TestInsertParams extends BaseTestSuite {
    private static final String database = "root.aligned";
    private static final String device = database+".d1";
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(6);

    private final List<String> paths = new ArrayList<>(1);
    private final List<String> measurements = new ArrayList<>(1);
    private final List<TSDataType> dataTypes = new ArrayList<>(1);
    private final List<MeasurementSchema> schemaList = new ArrayList<>(1);// tablet

    public void testInsertTablet_deviceNull() {

    }


}
