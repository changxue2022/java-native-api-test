package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
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
    private List<MeasurementSchema> schemaList = new ArrayList<>(1);// tablet
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
//    @Test(priority = 12) //TIMECHODB-144
//    public void testInsertTablet_schemaListNull() throws IoTDBConnectionException, StatementExecutionException {
//        Tablet tablet = new Tablet(device, null);
//        session.insertTablet(tablet);
//    }
//    @Test(priority = 13) //TIMECHODB-145
//    public void testInsertTablet_schemaListNullIn() throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemas = new ArrayList<>(1);
//        schemas.add(null);
//        Tablet tablet = new Tablet(device, schemas);
//        session.insertTablet(tablet);
//    }
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

}
