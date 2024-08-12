package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * insert params test cases
 * 各个参数的null,空值check
 */
public class TestInsertParams extends BaseTestSuite {
    private final String database = "root.params";
    private final String device = database+".d1";
    private final String deviceAligned = database+".d_aligned";
    private final String tsName = "s_float";

    private List<String> paths = new ArrayList<>(1);
    private List<String> measurements = new ArrayList<>(1);
    private List<TSDataType> dataTypes = new ArrayList<>(1);
    private List<IMeasurementSchema> schemaList = new ArrayList<>(1);// tablet
    private List<Object> values = new ArrayList<>(1);

    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        session.createDatabase(database);
        paths.add(device + "." + tsName);
        measurements.add(tsName);
        dataTypes.add(TSDataType.FLOAT);
        schemaList.add(new MeasurementSchema(tsName, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.GZIP));
        values.add(33.3f);
        // 非对齐
        session.createTimeseries(paths.get(0), dataTypes.get(0), TSEncoding.PLAIN, CompressionType.GZIP);

        // 对齐
        List<TSEncoding> encodings = new ArrayList<>(1);
        List<CompressionType> compressionTypes = new ArrayList<>(1);
        encodings.add(TSEncoding.PLAIN);
        compressionTypes.add(CompressionType.GZIP);
        session.createAlignedTimeseries(deviceAligned, measurements, dataTypes, encodings, compressionTypes, null);
    }
//    @AfterClass
//    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
//        session.deleteDatabase(database);
//    }
    private void checkResult(long timestamp, Object expect) throws IoTDBConnectionException, StatementExecutionException {
        checkQueryResult("select "+tsName+" from "+device +" where time ="+timestamp, expect);
    }
//    @Test(priority = 10)
//    public void testInsertTablet_null() throws IoTDBConnectionException, StatementExecutionException {
//        session.insertTablet(null);
//    }
//    // TIMECHODB-149
//    @Test(priority = 11, expectedExceptions = StatementExecutionException.class)
//    public void testInsertTablet_deviceNull() throws IoTDBConnectionException, StatementExecutionException {
//        Tablet tablet = new Tablet(null, schemaList);
//        session.insertTablet(tablet);
//    }
    @Test(priority = 12, expectedExceptions = NullPointerException.class) //TIMECHODB-144
    public void testInsertTablet_schemaListNull() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, null);
        session.insertTablet(tablet);
    }
    @Test(priority = 13, expectedExceptions = NullPointerException.class) //TIMECHODB-145
    public void testInsertTablet_schemaListNullIn() throws IoTDBConnectionException, StatementExecutionException {
        List<IMeasurementSchema> schemas = new ArrayList<>(1);
        schemas.add(null);
        Tablet tablet = new Tablet(device, schemas);
        session.insertTablet(tablet);
    }
    // TIMECHODB-530
//    @Test(priority = 13, expectedExceptions = NullPointerException.class)
    @Test(priority = 13, expectedExceptions = IoTDBConnectionException.class)
    public void testInsertTablet_schemaListNullIn2() throws IoTDBConnectionException, StatementExecutionException {
        int insertCount = 1;
        List<IMeasurementSchema> schemas = new ArrayList<>(3);
        schemas.add(new MeasurementSchema("s_0", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema(null, TSDataType.INT32));
        schemas.add(new MeasurementSchema("s_1", TSDataType.INT32));
        Tablet tablet = new Tablet(device, schemas, insertCount);
        int rowIndex = 0;
        long timestamp = baseTime;
        for (int row = 0; row < insertCount; row++) {
            rowIndex = tablet.rowSize++;
            timestamp += 3600000; //+1小时
//            System.out.println("row="+row+" rowIndex="+rowIndex);
            tablet.addTimestamp(rowIndex, timestamp);
//            tablet.addTimestamp(rowIndex, row);
            for (int i = 0; i < schemas.size(); i++) {
                switch(schemas.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getBoolean());
                        break;
                    case INT32:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getInt());
                        break;
                    case INT64:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getLong(10));
                        break;
                    case FLOAT:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getFloat(2,100,200));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getDouble(2,500,1000));
                        break;
                    case TEXT:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getChinese());
                        break;
                }
            }
        }
        PrepareConnection.getSession().insertTablet(tablet);
    }

    @Test(priority = 14)
    public void testInsertTablet_NoValue() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        session.insertTablet(tablet);
    }
    @Test(priority = 15, expectedExceptions = ClassCastException.class)
    public void testInsertTablet_typeError() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 1.0);
        session.insertTablet(tablet);
    }
    @Test(priority = 16, expectedExceptions = ClassCastException.class)
    public void testInsertTablet_typeError2() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, "1.0");
        session.insertTablet(tablet);
    }
    @Test(priority = 17)
    public void testInsertTablet_rowIndex0() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = 0;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 3.5f);
        session.insertTablet(tablet);
        checkResult(0L, 3.5f);
    }
    @Test(priority = 18)
    public void testInsertTablet_rowIndex2() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = 2;
        tablet.addTimestamp(rowIndex, rowIndex);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 8.8f);
        session.insertTablet(tablet);
        checkResult(rowIndex,8.8f);
    }
    @Test(priority = 19, expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void testInsertTablet_rowIndexNegative() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = -2;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 18.8f);
        session.insertTablet(tablet);
    }
    @Test(priority = 20, expectedExceptions = IndexOutOfBoundsException.class)
    public void testInsertTablet_schemaOutOfIndex() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 18.8f);
        session.insertTablet(tablet);
    }
//    @Test(priority = 21)
//    public void testInsertTablet_schemaNullIn() throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemas = new ArrayList<>(1);
//        schemas.add(null);
//        Tablet tablet = new Tablet(device, schemas);
//        int row = 0;
//        int rowIndex = tablet.rowSize++;
//        tablet.addTimestamp(rowIndex, row);
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 18.8f);
//        session.insertTablet(tablet);
//    }
//    @Test(priority = 22)
//    public void testInsertTablet_schemaErrorMeasurementSchema_empty() throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemas = new ArrayList<>(1);
//        schemas.add(new MeasurementSchema());
//        Tablet tablet = new Tablet(device, schemas);
//        int row = 0;
//        int rowIndex = tablet.rowSize++;
//        tablet.addTimestamp(rowIndex, row);
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 18.8f);
//        session.insertTablet(tablet);
//    }
//    @Test(priority = 23)
//    public void testInsertTablet_schemaErrorMeasurementSchema_nullTSName() throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemas = new ArrayList<>(1);
//        schemas.add(new MeasurementSchema(null, TSDataType.FLOAT));
//        Tablet tablet = new Tablet(device, schemas);
//        int row = 0;
//        int rowIndex = tablet.rowSize++;
//        tablet.addTimestamp(rowIndex, row);
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 18.8f);
//        session.insertTablet(tablet);
//    }
//    @Test(priority = 24)
//    public void testInsertTablet_schemaErrorMeasurementSchema_emptyTSName() throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemas = new ArrayList<>(1);
//        schemas.add(new MeasurementSchema("", TSDataType.FLOAT));
//        Tablet tablet = new Tablet(device, schemas);
//        int row = 0;
//        int rowIndex = tablet.rowSize++;
//        tablet.addTimestamp(rowIndex, row);
//        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 18.8f);
//        session.insertTablet(tablet);
//    }
//    @Test(priority = 25) // TIMECHODB-143
//    public void testInsertTablet_schemaErrorMeasurementSchema_nullDatatype() throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemas = new ArrayList<>(1);
//        schemas.add(new MeasurementSchema("tmp", null));
//        Tablet tablet = new Tablet(device, schemas);
//        int row = 0;
//        int rowIndex = tablet.rowSize++;
//        tablet.addTimestamp(rowIndex, row);
//        tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 18.8f);
//        session.insertTablet(tablet);
//    }
    @Test(priority = 26)
    public void testInsertTablet_timestampNegative() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, -1);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, -11.8f);
        session.insertTablet(tablet);
        checkResult(-1, -11.8f);
    }
    @Test(priority = 27)
    public void testInsertTablet_timestampFuture() throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList);
        int row = 0;
        int rowIndex = tablet.rowSize++;
        // "2098-12-09T08:00:00+08:00"
        tablet.addTimestamp(rowIndex, 4068921600000L);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, -1.8f);
        session.insertTablet(tablet);
        checkResult(4068921600000L,-1.8f);
    }

//    @Test(priority = 30, expectedExceptions = StatementExecutionException.class)
//    public void testInsertRecord_nullDevice() throws IoTDBConnectionException, StatementExecutionException {
//        session.insertRecord(null, 100L, measurements, dataTypes, values.get(0));
//    }
    @Test(priority = 31)
    public void testInsertRecord_timestampNegative() throws IoTDBConnectionException, StatementExecutionException {
        session.insertRecord(device, -1, measurements, dataTypes, values);
        checkResult(-1, values.get(0));
    }
    @Test(priority = 32)
    public void testInsertRecord_timestampFuture() throws IoTDBConnectionException, StatementExecutionException {
        session.insertRecord(device, 4068921600000L, measurements, dataTypes, values.get(0));
        checkResult(4068921600000L, values.get(0));
    }
//    // TIMECHODB-149
//    @Test(priority = 33, expectedExceptions = StatementExecutionException.class)
//    public void testInsertRecord_tsNameNull() throws IoTDBConnectionException, StatementExecutionException {
//        session.insertRecord(device, 100L, null, dataTypes, values.get(0));
//    }
//    // TIMECHODB-149
//    @Test(priority = 34, expectedExceptions = StatementExecutionException.class)
//    public void testInsertRecord_tsNameNullIn() throws IoTDBConnectionException, StatementExecutionException {
//        List<String> tsNames = new ArrayList<>(1);
//        tsNames.add(null);
//        session.insertRecord(device, 100L, tsNames, dataTypes, values.get(0));
//    }
    @Test(priority = 35)
    public void testInsertRecord_tsNameEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.insertRecord(device, 100L, new ArrayList<>(0), dataTypes, values.get(0));
    }
//    @Test(priority = 36)
//    public void testInsertRecord_datatypeNull() throws IoTDBConnectionException, StatementExecutionException {
//        session.insertRecord(device, 100L, measurements, null, values.get(0));
//    }
//    @Test(priority = 37)
//    public void testInsertRecord_datatypeNullIn() throws IoTDBConnectionException, StatementExecutionException {
//        List<TSDataType> dataTypesTmp = new ArrayList<>(1);
//        dataTypesTmp.add(null);
//        session.insertRecord(device, 100L, measurements, dataTypesTmp, values.get(0));
//    }
    @Test(priority = 38, expectedExceptions = IndexOutOfBoundsException.class)
    public void testInsertRecord_datatypeEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.insertRecord(device, 100L, measurements, new ArrayList<>(1), values.get(0));
    }
    @Test(priority = 39) // 执行成功，但是可能没有效果
    public void testInsertRecord_valuesNull() throws IoTDBConnectionException, StatementExecutionException {
        session.insertRecord(device, 100L, measurements, dataTypes, (Object) null);
    }
    @Test(priority = 40, expectedExceptions = StatementExecutionException.class)
    public void testInsertRecord_valuesEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.insertRecord(device, 100L, measurements, dataTypes, new ArrayList<>(0));
    }
    @Test(priority = 41)
    public void testInsertRecord_valuesNullIn() throws IoTDBConnectionException, StatementExecutionException {
        List<Object> v = new ArrayList<>(1);
        v.add(null);
        session.insertRecord(device, 100L, measurements, dataTypes, v);
    }
    @Test(priority = 42, expectedExceptions = ClassCastException.class)
    public void testInsertRecord_valuesErrorType() throws IoTDBConnectionException, StatementExecutionException {
        List<Object> v = new ArrayList<>(1);
        v.add("1a1b");
        session.insertRecord(device, 100L, measurements, dataTypes, v);
    }
    @Test(priority = 43, expectedExceptions = StatementExecutionException.class)
    public void testInsertRecord_sizeTSNameOver() throws IoTDBConnectionException, StatementExecutionException {
        List<String> tsNames = new ArrayList<>(2);
        tsNames.add(tsName);
        tsNames.add(tsName+"2");
        session.insertRecord(device, 100L, tsNames, dataTypes, values.get(0));
    }
    @Test(priority = 44, expectedExceptions = StatementExecutionException.class)
    public void testInsertRecord_sizeTSNameDup() throws IoTDBConnectionException, StatementExecutionException {
        List<String> tsNames = new ArrayList<>(2);
        tsNames.add(tsName);
        tsNames.add(tsName);
        session.insertRecord(device, 100L, tsNames, dataTypes, values.get(0));
    }
    @Test(priority = 45)
    public void testInsertRecord_sizedatatypeDup() throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> dataTypesTmp = new ArrayList<>(2);
        dataTypesTmp.add(TSDataType.FLOAT);
        dataTypesTmp.add(TSDataType.FLOAT);
        session.insertRecord(device, 100L, measurements, dataTypesTmp, values.get(0));
    }
    @Test(priority = 46, expectedExceptions = IndexOutOfBoundsException.class)
    public void testInsertRecord_sizeValuesDup() throws IoTDBConnectionException, StatementExecutionException {
        List<Object> v = new ArrayList<>(2);
        v.add(1.3f);
        v.add(2.3f);
        session.insertRecord(device, 100L, measurements, dataTypes, v);
    }

    @Test(priority = 50, expectedExceptions = StatementExecutionException.class)
    public void testInsertTablet_sameTS() throws IoTDBConnectionException, StatementExecutionException {
        int insertCount = 1;
        List<IMeasurementSchema> schemas = new ArrayList<>(3);
        schemas.add(new MeasurementSchema("s_0", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("s_1", TSDataType.INT32));
        schemas.add(new MeasurementSchema("s_1", TSDataType.INT32));
        Tablet tablet = new Tablet(database+".d_11", schemas, insertCount);
        int rowIndex = 0;
        long timestamp = baseTime;
        for (int row = 0; row < insertCount; row++) {
            rowIndex = tablet.rowSize++;
            timestamp += 3600000; //+1小时
//            System.out.println("row="+row+" rowIndex="+rowIndex);
            tablet.addTimestamp(rowIndex, timestamp);
//            tablet.addTimestamp(rowIndex, row);
            for (int i = 0; i < schemas.size(); i++) {
                switch(schemas.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getBoolean());
                        break;
                    case INT32:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, GenerateValues.getInt());
                        break;
                }
            }
        }
        PrepareConnection.getSession().insertTablet(tablet);
    }

    @Test(priority = 51, expectedExceptions = StatementExecutionException.class)
    public void testInsertRecord_sameTS() throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> schemas = new ArrayList<>(3);
        List<String> paths = new ArrayList<>(3);
        List<Object> values = new ArrayList<>(3);
        schemas.add(TSDataType.BOOLEAN);
        schemas.add(TSDataType.INT32);
        schemas.add(TSDataType.INT32);
        paths.add("s_0");
        paths.add("s_1");
        paths.add("s_1");
        values.add(true);
        values.add(32);
        values.add(64);
        PrepareConnection.getSession().insertRecord(device,1635232143960L, paths, schemas, values);
    }
    @Test(priority = 52, expectedExceptions = StatementExecutionException.class)
    public void testInsertRecords_sameTS() throws IoTDBConnectionException, StatementExecutionException {
        List<String> devices = new ArrayList<>(1);
        List<Long> timestamps = new ArrayList<>(1);
        List<List<TSDataType>> schemas = new ArrayList<>(1);
        List<List<String>> measurements = new ArrayList<>(1);
        List<List<Object>> values = new ArrayList<>(1);
        devices.add(device);
        timestamps.add(1635232143960L);
        List<TSDataType> schema = new ArrayList<>(3);
        List<String> paths = new ArrayList<>(3);
        List<Object> value = new ArrayList<>(3);
        schema.add(TSDataType.BOOLEAN);
        schema.add(TSDataType.INT32);
        schema.add(TSDataType.INT32);
        schemas.add(schema);
        paths.add("s_0");
        paths.add("s_1");
        paths.add("s_1");
        measurements.add(paths);
        value.add(true);
        value.add(32);
        value.add(64);
        values.add(value);
        PrepareConnection.getSession().insertRecords(devices,timestamps, measurements, schemas, values);
    }

}
