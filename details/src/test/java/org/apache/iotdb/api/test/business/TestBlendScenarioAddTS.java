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

/**
 * 需要整理调试
 */
public class TestBlendScenarioAddTS extends BaseTestSuite {
    private String database = "root.blendAddTS";
    private String[] devices = new String[]{database+".alignedUsingTemp", database+".nonAlignedUsingTemp", database+".aligned", database+".nonAligned"};
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
        session.setStorageGroup(database);
        for(String templateName: templateNames) {
            if (checkTemplateExists(templateName)) {
                session.dropSchemaTemplate(templateName);
            }
        }
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
        if (checkStroageGroupExists(database)) {
            session.deleteStorageGroup(database);
        }
        for(String templateName: templateNames) {
            if (checkTemplateExists(templateName)) {
                session.dropSchemaTemplate(templateName);
            }
        }

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
        assert checkTemplateContainPath(templateNames[index], devices[index]) : "挂载模版成功:"+devices[index];
        assert 1 == getSetPathsCount(templateNames[index], verbose) : "挂载模版成功";
    }
    private void insertTablet(int index, boolean isAligned) throws IOException, IoTDBConnectionException, StatementExecutionException {
        assert 0 == getTimeSeriesCount(devices[index]+".**", true) : "创建前没有符合条件的TimeSeries:"+devices[index];
        assert 0 == getRecordCount(devices[index], true) : "插入前record数目,"+devices[index]+" isAligned="+isAligned;
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
        if (isAligned) {
            session.insertAlignedTablet(tablet);
//            Assert.assertThrows(StatementExecutionException.class, () -> session.insertTablet(tablet));
        } else {
            session.insertTablet(tablet);
//            Assert.assertThrows(StatementExecutionException.class, () -> session.insertAlignedTablet(tablet));
        }
        assert 6 == getTimeSeriesCount(devices[index]+".**", true) : "成功创建TimeSeries";
        assert expectCount - 1 == getRecordCount(devices[index], true) : "插入record数目,"+devices[index]+" isAligned="+isAligned;
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
        checkQueryResult("select s_float from "+devices[index]+" where time="+timestamp1+";", 13.33);
        checkQueryResult("select appendFloat from "+devices[index]+" where time="+timestamp1+";", 34567.0);
        checkQueryResult("select s_long from "+devices[index]+" where time="+timestamp2+";", timestamp2);
        checkQueryResult("select appendFloat from "+devices[index]+" where time="+timestamp2+";", 69134.0);
    }

    @Test(priority = 10)
    public void testCreateAlignedTemplate() throws IoTDBConnectionException, IOException, StatementExecutionException {
        createTemplate(0, true);
    }
    @Test(priority = 20)
    public void testCreateNonAlignedTemplate() throws IoTDBConnectionException, IOException, StatementExecutionException {
        createTemplate(1, false);
    }
    @Test(priority = 30)
    public void testInsertAlignedTS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        insertTablet(2, true);
    }
    @Test(priority = 40)
    public void testInsertAlignedTemplateTS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        insertTablet(0, true);
    }
    @Test(priority = 50)
    public void testInsertNonAlignedTemplateTS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        insertTablet(1, false);
    }
    @Test(priority = 60)
    public void testInsertNonAlignedTS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        insertTablet(3, false);
    }
    @Test(priority = 70)
    public void testUpdateAlignedTemplateTS() throws IoTDBConnectionException, StatementExecutionException {
        doUpdate(0, true);
    }
    @Test(priority = 80)
    public void testUpdateNonAlignedTemplateTS() throws IoTDBConnectionException, StatementExecutionException {
        doUpdate(1, false);
    }
    @Test(priority = 90)
    public void testUpdateAlignedTS() throws IoTDBConnectionException, StatementExecutionException {
        doUpdate(2, true);
    }
    @Test(priority = 100)
    public void testUpdateNonAlignedTS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        doUpdate(3, false);
    }
    @Test(priority = 101)
    public void testUpdateAfterAddTS_aligned() throws IoTDBConnectionException, StatementExecutionException {
        measurements.add("appendFloat");
        dataTypes.add(TSDataType.FLOAT);
        doUpdateAfterAddTS(2, true);
    }
    @Test(priority = 102)
    public void testUpdateAfterAddTS_nonAligned() throws IoTDBConnectionException, StatementExecutionException {
        doUpdateAfterAddTS(3, false);
    }
    @Test(priority = 103)
    public void testUpdateAfterAddTS_alignedTemp() throws IoTDBConnectionException, StatementExecutionException {
//        List<String> measurement = new ArrayList<>(1);
//        List<TSDataType> dataType = new ArrayList<>(1);
//        List<TSEncoding> encoding = new ArrayList<>(1);
//        List<CompressionType> compressor = new ArrayList<>(1);
//        List<String> alias = new ArrayList<>(1);
//        measurement.add("appendFloat");
//        dataType.add(TSDataType.FLOAT);
//        encoding.add(TSEncoding.GORILLA);
//        compressor.add(CompressionType.UNCOMPRESSED);
//        alias.add("append_float");
//        session.createAlignedTimeseries(devices[0], measurement, dataType, encoding, compressor, alias);
//        assert 7 == getTimeSeriesCount(devices[0]+".**", true) : "追加一个TS";
        doUpdateAfterAddTS(0, true);
    }
    @Test(priority = 104)
    public void testUpdateAfterAddTS_nonAlignedTemp() throws IoTDBConnectionException, StatementExecutionException {
        doUpdateAfterAddTS(1, false);
    }

    @Test(priority = 110)
    public void testDropTS() throws IoTDBConnectionException, StatementExecutionException {
        for(int index=2; index<4; index++) {
            assert 7 == getTimeSeriesCount(devices[index] + ".*", verbose) : "删除前TimeSeries为6:"+devices[index];
            session.deleteTimeseries(devices[index] + ".*");
            assert 0 == getTimeSeriesCount(devices[index] + ".*", verbose) : "删除后TimeSeries为0:"+devices[index];
        }
    }
    @Test(priority = 120)
    public void testDropTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        for (int i=0; i<templateNames.length; i++) {
            String templateName = templateNames[i];
            assert checkTemplateExists(templateName) : "模版存在";
            assert checkTemplateContainPath(templateName, devices[i]) : "挂载模版路径";
            deactiveTemplate(templateName, devices[i]);
            session.unsetSchemaTemplate(devices[i], templateName);
            assert false == checkTemplateContainPath(templateName, devices[i]) : "解除模版路径成功";

            // IOTDB-5436 StatementExecutionException: 300: Modify template has not been supported.
//        session.deleteNodeInTemplate(templateName, loadNode);
            assert 0 == getActivePathsCount(templateName, verbose) : "没有已激活路径";
            assert 0 == getSetPathsCount(templateName, verbose) : "没有已挂载路径";
            session.dropSchemaTemplate(templateName);
            assert false == checkTemplateExists(templateName) : "删除模版成功";
        }

    }

}
