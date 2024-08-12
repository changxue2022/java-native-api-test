package org.apache.iotdb.api.test.business;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class TestTemplateNonAlignedAddTS extends BaseTestSuite {
    private String database = "root.blendAddTS";
    private String[] devices = new String[]{database+".alignedUsingTemp", database+".nonAlignedUsingTemp"};
    private String[] templateNames = new String[]{"aligned_template", "nonAligned_template"};
    private int expectCount = 17;
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    private List<String> measurements = new ArrayList<>(7);
    private List<TSDataType> dataTypes = new ArrayList<>(7);
    private List<IMeasurementSchema> schemaList = new ArrayList<>(7);// tablet

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        if (checkStroageGroupExists(database)) {
            session.deleteStorageGroup(database);
        }
        for(String templateName: templateNames) {
            if (checkTemplateExists(templateName)) {
                session.dropSchemaTemplate(templateName);
            }
        }
        session.setStorageGroup(database);
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_float", new Object[]{ TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY});

        structureInfo.forEach((key,value) -> {
            measurements.add(key);
            dataTypes.add((TSDataType) value[0]);
            schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });

    }

    @AfterClass(enabled = true)
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup(database);
        cleanTemplates(verbose);
    }
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/business-insert-records.csv").getData();
    }

    private void createTemplate(int index, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException, IOException {
        int templateCount = getTemplateCount(verbose);
        Template template = new Template(templateNames[index], isAligned);
        structureInfo.forEach((key,value)->{
            MeasurementNode mNode =
                    new MeasurementNode(key, (TSDataType)value[0], (TSEncoding)value[1], (CompressionType)value[2]);
            try {
                template.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        session.createSchemaTemplate(template);
        //  IOTDB-5437 StatementExecutionException: 300: COUNT_MEASUREMENTShas not been supported.
//        assert 6 == session.countMeasurementsInTemplate(templateName) : "查看模版中sensor数目";
        session.setSchemaTemplate(templateNames[index], devices[index]);
        assert 1+templateCount == getTemplateCount(verbose) : "创建模版成功";
        assert checkTemplateContainPath(templateNames[index], devices[index]) : "挂载模版成功";
        assert 1 == getSetPathsCount(templateNames[index], verbose) : "挂载模版成功count";
    }
    private void insertTablet(int index, boolean isAligned) throws IOException, IoTDBConnectionException, StatementExecutionException {
        assert 0 == getTimeSeriesCount(devices[index]+".**", verbose) : "创建前没有符合条件的TimeSeries:"+devices[index];
        assert 0 == getRecordCount(devices[index], verbose) : "插入前record数目,"+devices[index]+" isAligned="+isAligned;
        Tablet tablet = new Tablet(devices[index], schemaList, 100);
        int rowIndex = 0;
        int col = 0;
        tablet.initBitMaps();
        Iterator<Object[]> it = getSingleNormal();
        while (it.hasNext()) {
            rowIndex = tablet.rowSize++;
            Object[] line = it.next();
            tablet.addTimestamp(rowIndex, Long.valueOf((String)line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                col = i+1;
                if (line[col] == null) {
                    tablet.bitMaps[i].mark(rowIndex);
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, null);
                    continue;
                }
                switch(schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Boolean.valueOf((String)line[col]));
                        break;
                    case INT32:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Integer.valueOf((String)line[col]));
                        break;
                    case INT64:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Long.valueOf((String)line[col]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Float.valueOf((String)line[col]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Double.valueOf((String)line[col]));
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[col]);
                        break;
                }
            }
        }
//        if (isAligned) {
//            session.insertAlignedTablet(tablet);
//            Assert.assertThrows(StatementExecutionException.class, () -> session.insertTablet(tablet));
//        } else {
            session.insertTablet(tablet);
//            Assert.assertThrows(StatementExecutionException.class, () -> session.insertAlignedTablet(tablet));
//        }
        assert 6 == getTimeSeriesCount(devices[index]+".**", verbose) : "成功创建TimeSeries";
        assert expectCount - 1 == getRecordCount(devices[index], verbose) : "插入record数目,"+devices[index]+" isAligned="+isAligned;
    }
    private void doUpdate(int index, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109398772L;
        checkQueryResult("select s_text from "+devices[index] +" where time="+timestamp+";",  0);

        List<Long> times = new ArrayList<>(1);
        List<List<String>> measurementsList = new ArrayList<>(1);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<List<TSDataType>> datatypeList = new ArrayList<>(1);
        times.add(timestamp);
        measurementsList.add(measurements);
        datatypeList.add(dataTypes);
        List<Object> values = new ArrayList<>(6);
        values.add(false);
        values.add(1);
        values.add(timestamp);
        values.add(3.0f);
        values.add(4.0);
        values.add("update_value");
        valuesList.add(values);
//        if (isAligned) {
            session.insertAlignedRecordsOfOneDevice(devices[index], times, measurementsList, datatypeList, valuesList);
//            Assert.assertThrows(StatementExecutionException.class, () ->{
//                session.insertRecordsOfOneDevice(devices[index], times, measurementsList, datatypeList, valuesList);
//            });
//        } else {
//            session.insertRecordsOfOneDevice(devices[index], times, measurementsList, datatypeList, valuesList);
//            Assert.assertThrows(StatementExecutionException.class, () ->{
//                session.insertAlignedRecordsOfOneDevice(devices[index], times, measurementsList, datatypeList, valuesList);
//            });
//        }
        checkQueryResult("select s_text from "+devices[index] +" where time="+timestamp+";", "update_value");
        checkQueryResult("select s_long from "+devices[index] +" where time="+timestamp+";", timestamp);
    }
    private void doUpdateAfterAddTS(int index, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        long timestamp1 = 1669109508000L;
        long timestamp2 = 1669109398772L;
        logger.debug("###### 前  ######");
        getCount("select count(*) from "+devices[index]+" where time="+timestamp1+";", verbose);
        getCount("select count(*) from "+devices[index]+" where time="+timestamp2+";", verbose);
        logger.debug("###### 前  ######");
        List<Long> times = new ArrayList<>(2);
        List<List<String>> valueList = new ArrayList<>(2);
        List<List<String>> measurementList = new ArrayList<>(2);
        times.add(timestamp1);
        times.add(timestamp2);
        measurementList.add(measurements);
        measurementList.add(measurements);

        for (int i=0; i<2; i++) {
            List<String> values = new ArrayList<>(7);
            values.add(String.valueOf(false));
            values.add(String.valueOf(i));
            values.add(String.valueOf(times.get(i)));
            values.add(String.valueOf((i+1)*13.33f));
            values.add(String.valueOf((i+1)*144.44));
            values.add("add/update after update TS:"+i);
            values.add(String.valueOf((i+1)*34567f));
            valueList.add(values);
        }
        if (isAligned) {
            session.insertAlignedStringRecordsOfOneDevice(devices[index], times, measurementList, valueList);
        } else {
            session.insertStringRecordsOfOneDevice(devices[index], times, measurementList, valueList);
        }
        logger.debug("###### 前  ######");
        getCount("select count(*) from "+devices[index]+" where time="+timestamp1+";", verbose);
        getCount("select count(*) from "+devices[index]+" where time="+timestamp2+";", verbose);
        logger.debug("###### 前  ######");
        checkQueryResult("select s_float from "+devices[index]+" where time="+timestamp1+";", 13.33);
        checkQueryResult("select appendFloat from "+devices[index]+" where time="+timestamp1+";", 34567.0);
        checkQueryResult("select s_long from "+devices[index]+" where time="+timestamp2+";", timestamp2);
        checkQueryResult("select appendFloat from "+devices[index]+" where time="+timestamp2+";", 69134.0);
    }

    private void doUpdateAfterAddTS2(int index, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        long timestamp1 = 1669109508000L;
        long timestamp2 = 1669109398772L;
        logger.debug("###### 前  ######");
        getCount("select count(*) from "+devices[index]+" where time="+timestamp1+";", verbose);
        getCount("select count(*) from "+devices[index]+" where time="+timestamp2+";", verbose);
        logger.debug("###### 前  ######");
        List<Long> times = new ArrayList<>(2);
        List<List<Object>> valueList = new ArrayList<>(2);
        List<List<String>> measurementList = new ArrayList<>(2);
        times.add(timestamp1);
        times.add(timestamp2);
        measurementList.add(measurements);
        measurementList.add(measurements);

        for (int i=0; i<2; i++) {
            List<Object> values = new ArrayList<>(7);
            values.add(true);
            values.add(i);
            values.add(times.get(i));
            values.add((i+1)*13.33f);
            values.add((i+1)*144.44);
            values.add("insertAlignedRecord:"+i);
            values.add((i+1)*34567f);
            valueList.add(values);
            if (isAligned) {
                session.insertAlignedRecord(devices[index], times.get(i), measurements, dataTypes, valueList.get(i));
            } else {
                session.insertRecord(devices[index], times.get(i), measurements, dataTypes, valueList.get(i));
            }
        }
        logger.debug("###### 后  ######");
        getCount("select count(*) from "+devices[index]+" where time="+timestamp1+";", verbose);
        getCount("select count(*) from "+devices[index]+" where time="+timestamp2+";", verbose);
        logger.debug("###### 后  ######");
        checkQueryResult("select s_float from "+devices[index]+" where time="+timestamp1+";", 13.33);
        checkQueryResult("select appendFloat from "+devices[index]+" where time="+timestamp1+";", 34567.0);
        checkQueryResult("select s_long from "+devices[index]+" where time="+timestamp2+";", timestamp2);
        checkQueryResult("select appendFloat from "+devices[index]+" where time="+timestamp2+";", 69134.0);
    }
    private void doUpdateAfterAddTS_origin(int index, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        Long[] times = new Long[]{1669109509000L, 1669109398772L};
        logger.debug("###### 前  ######");
        getCount("select count(*) from "+devices[index]+" where time="+times[0]+";", verbose);
        getCount("select count(*) from "+devices[index]+" where time="+times[1]+";", verbose);
        logger.debug("###### 前  ######");
        Tablet tablet = new Tablet(devices[index], schemaList, 100);
        int rowIndex = 0;
        tablet.initBitMaps();

        for (int i=0; i<2; i++) {
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, times[i]);
            tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, true);
            tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, i);
            tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, times[i]);
            tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, (i+1)*130.33f);
            tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, (i+1)*14400.44);
            tablet.addValue(schemaList.get(5).getMeasurementId(), rowIndex, "doUpdateAfterAddTS_origin:"+i);
        }
        if (isAligned) {
            session.insertAlignedTablet(tablet);
        } else {
            session.insertTablet(tablet);
        }
        logger.debug("###### 后  ######");
        getCount("select count(*) from "+devices[index]+" where time="+times[0]+";", verbose);
        getCount("select count(*) from "+devices[index]+" where time="+times[1]+";", verbose);
        logger.debug("###### 后  ######");
        checkQueryResult("select s_float from "+devices[index]+" where time="+times[0]+";", 130.33);
        checkQueryResult("select s_text from "+devices[index]+" where time="+times[0]+";", "doUpdateAfterAddTS_origin:0");
        checkQueryResult("select s_long from "+devices[index]+" where time="+times[1]+";", times[1]);
        checkQueryResult("select s_text from "+devices[index]+" where time="+times[1]+";", "doUpdateAfterAddTS_origin:1");
    }

    @Test(priority = 10)
    public void testCreateAlignedTemplate() throws IoTDBConnectionException, IOException, StatementExecutionException {
        createTemplate(1, false);
    }
    @Test(priority = 20)
    public void testInsertAlignedTemplateTS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        insertTablet(1, false);
    }
    @Test(priority = 30)
    public void testUpdateAlignedTS() throws IoTDBConnectionException, StatementExecutionException {
        doUpdate(1, false);
    }

    @Test(priority = 50)
    public void testUpdateAfterAddTS_alignedTemp() throws IoTDBConnectionException, StatementExecutionException {
        // 追加TS结构
        measurements.add("appendFloat");
        dataTypes.add(TSDataType.FLOAT);

//      // 使用 insertAlignedStringRecordsOfOneDevice
        // IOTDB-5441
//        doUpdateAfterAddTS(1, false);

        // 使用 insertAlignedRecord
        doUpdateAfterAddTS2(1, false);

        //使用tablet
//        schemaList.add(new MeasurementSchema("appendFloat", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
//        doUpdateAfterAddTS3(0, true);
    }

    @Test(priority = 60)
    public void testInsertWithoutAdd() throws IoTDBConnectionException, StatementExecutionException {
        //使用tablet
//        schemaList.add(new MeasurementSchema("appendFloat", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
        doUpdateAfterAddTS_origin(1, false);

    }

}
