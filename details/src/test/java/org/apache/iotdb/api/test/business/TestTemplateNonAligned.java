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
 * 无修改结构 aligned
 * 创建aligned template(6 sensor),插入数据，查询数据，删除数据，插入数据，删除template, 查询
 * 2022-12-29
 */
public class TestTemplateNonAligned extends BaseTestSuite {
    private String templateName = "nonAligned_template";
    private String loadNode = "root.business.nonAligned";
    private String device = loadNode+".d1";
    private String database = loadNode.substring(0,loadNode.lastIndexOf('.'));
    private int expectCount = 17;
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(6);
    private List<String> paths = new ArrayList<>(6);
    private List<String> measurements = new ArrayList<>(6);
    private List<TSDataType> dataTypes = new ArrayList<>(6);
    private List<IMeasurementSchema> schemaList = new ArrayList<>(6);// tablet


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        if (checkStroageGroupExists(database)) {
            session.deleteStorageGroup(database);
        }
        session.setStorageGroup(database);
        if (checkTemplateExists(templateName)) {
            session.dropSchemaTemplate(templateName);
        }
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int", TSDataType.INT32);
        measureTSTypeInfos.put("s_long", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);

        measureTSTypeInfos.forEach((key,value) -> {
            paths.add(device+"."+key);
            measurements.add(key);
            dataTypes.add(value);
            schemaList.add(new MeasurementSchema(key, value));
        });

    }

    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        if (checkStroageGroupExists(database)) {
            session.deleteStorageGroup(database);
        }
        if (checkTemplateExists(templateName)) {
            session.dropSchemaTemplate(templateName);
        }
    }
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/business-insert-records.csv").getData();
    }

    @Test(priority = 10)
    public void testCreateTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        assert 0 == getTemplateCount(verbose) : "创建前无模版";
        Template template = new Template(templateName, false);
        MeasurementNode mNodeS1 =
                new MeasurementNode("s_boolean", TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY);
        MeasurementNode mNodeS2 =
                new MeasurementNode("s_int", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);
        MeasurementNode mNodeS3 =
                new MeasurementNode("s_long", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        MeasurementNode mNodeS4 =
                new MeasurementNode("s_float", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY);
        MeasurementNode mNodeS5 =
                new MeasurementNode("s_double", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
        MeasurementNode mNodeS6 =
                new MeasurementNode("s_text", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY);

        template.addToTemplate(mNodeS1);
        template.addToTemplate(mNodeS2);
        template.addToTemplate(mNodeS3);
        template.addToTemplate(mNodeS4);
        template.addToTemplate(mNodeS5);
        template.addToTemplate(mNodeS6);

        session.createSchemaTemplate(template);
        // IOTDB-5437 StatementExecutionException: 300: COUNT_MEASUREMENTShas not been supported.
//        assert 6 == session.countMeasurementsInTemplate(templateName) : "查看模版中sensor数目";
        assert 6 == getTSCountInTemplate(templateName, verbose) : "查看模版中sensor数目";
        session.setSchemaTemplate(templateName, loadNode);
        assert 1 == getTemplateCount(verbose): "创建模版成功";
        assert 1 == getSetPathsCount(templateName, verbose) : "挂载模版成功count";
        assert checkTemplateContainPath(templateName, loadNode) : "挂载模版成功";
    }

    @Test(priority = 20)
    public void testInsert() throws IOException, IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 100);
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
        session.insertTablet(tablet);
        assert expectCount-1 == getRecordCount(device, verbose) : "插入record数目";
//        Assert.assertThrows(StatementExecutionException.class, ()->session.insertAlignedTablet(tablet));
    }
    @Test(priority = 30)
    public void testQuery() throws IoTDBConnectionException, StatementExecutionException {
        checkQueryResult("select s_double from "+ device +" where time=2022-11-22T17:29:58.754+08:00;", 1899.21);
    }
    @Test(priority = 40)
    public void testUpdate() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109398772L;
        checkQueryResult("select s_text from "+device +" where time="+timestamp+";",  0);

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
        values.add(2L);
        values.add(3.0f);
        values.add(4.0);
        values.add("update_value");
        valuesList.add(values);
        session.insertRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList);
        checkQueryResult("select s_text from "+device +" where time="+timestamp+";", "update_value");
        checkQueryResult("select s_long from "+device +" where time="+timestamp+";", 2);
    }

    @Test(priority = 50)
    public void testDelete() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteData(device+".*", 1669109404000L);
        assert 1 == getRecordCount(device, verbose) : "确认结果:删除后还剩一条数据";
    }
    @Test(priority = 60)
    public void testInsertAfterDelete() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109406000L;
        List<Object> values = new ArrayList<>(6);
        values.add(false);
        values.add(55);
        values.add(timestamp);
        values.add(13.33f);
        values.add(876.44);
        values.add("insert after delete");
        session.insertRecord(device, timestamp, measurements, dataTypes, values);
        assert 2 == getRecordCount(device, false) : "确认结果:删除后插入成功";
        checkQueryResult("select s_double from "+device+" where time="+timestamp+";", 876.44);
    }

    @Test(priority = 70)
    public void testDropTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        assert checkTemplateExists(templateName) : "模版存在";
        assert checkTemplateContainPath(templateName, loadNode) : "挂载模版路径";
        assert 1 == getSetPathsCount(templateName, verbose) : "挂载路径数";
        assert 1 == getActivePathsCount(templateName, verbose) : "激活路径数";
        deactiveTemplate(templateName, device);
        // IOTDB-5436 StatementExecutionException: 300: Modify template has not been supported.
//        session.deleteNodeInTemplate(templateName, loadNode);
        assert 0 == getActivePathsCount(templateName, verbose) : "解除挂载模版路径成功";
        session.unsetSchemaTemplate(loadNode, templateName);
        assert 0 == getSetPathsCount(templateName, verbose) : "卸载路径成功";
        assert false == checkTemplateContainPath(templateName, loadNode) : "挂载模版路径";
        session.dropSchemaTemplate(templateName);
        assert false == checkTemplateExists(templateName) : "删除模版成功";
    }

}
