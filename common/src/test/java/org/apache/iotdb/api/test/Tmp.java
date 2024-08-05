package org.apache.iotdb.api.test;

import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

// 模板
public class Tmp extends BaseTestSuite {
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    Logger logger = Logger.getLogger(Tmp.class);
    private List<String> devicePaths = new ArrayList<>();
    private String database = "root.db.factory";
    private String tsPrefix = "sensors_";
    private String tsName = "appendSensor";
    private String templateName = "template01";


    @BeforeClass
    public void BeforeClass() throws IoTDBConnectionException, StatementExecutionException {
        logger.warn("########### Tmp BeforeClass ####");
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        if (!checkStroageGroupExists(database))
        session.createDatabase(database);

    }
    @AfterClass
    public void afterClass () {
        logger.warn("###### Tmp afterClass #####");
    }


    @Test
    public void testINsert() throws IoTDBConnectionException, IOException, StatementExecutionException {
        session.createTimeseries("root.sg.d.s_name", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null, null, null, null);
        List<MeasurementSchema> schemaTypeList = new ArrayList<>();
        schemaTypeList.add(new MeasurementSchema("s_name",TSDataType.BOOLEAN));
        insertTabletMulti("root.sg.d", schemaTypeList, 10, true);
    }

    // TIMECHODB-124
//    @Test
    public void test() throws IoTDBConnectionException, StatementExecutionException {
        Map<String,String> props = new HashMap<>();
//        props.put("Prop1", "3");
        session.createTimeseries("root.sg.d.s_name", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, props, null, null, null);
    }

    private List<MeasurementSchema> schemaList = new ArrayList<>(7);// tablet

    private void createTemplate (String templateName, String loadNode, boolean isAligned) throws
            IoTDBConnectionException, StatementExecutionException, IOException {
//        int templateCount = countLines("show schema templates", true);
        Template template = new Template(templateName, isAligned);

        structureInfo.forEach((key, value) -> {
            schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
            MeasurementNode mNode =
                    new MeasurementNode(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
            try {
                template.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        session.createSchemaTemplate(template);
        countLines("show schema templates", verbose);
        if (!loadNode.isEmpty()) {
            session.setSchemaTemplate(templateName, loadNode);
            assert checkTemplateContainPath(templateName, loadNode) : "挂载模版成功";
            getSetPathsCount(templateName, verbose);
            insertTabletMulti(loadNode, schemaList, 10, isAligned);
            countLines("show timeseries "+loadNode+".**", verbose);
            getRecordCount(loadNode, verbose);
        }
    }

    // TIMECHODB-271
    @Test(priority = 10)
    public void test271() throws IOException, ParseException, IoTDBConnectionException, StatementExecutionException {
        createTemplate(templateName, database, true);
    }
    // TIMECHODB-271
    @DataProvider(name="getNormalNames", parallel = true)
    public Object[][] getNormalNames() throws IOException {
        return new Object[][]{{"`*`"} };
    }
    // TIMECHODB-271
    @Test(priority = 65, dataProvider = "getNormalNames")
    public void testAddTSToTemplate_normalNames(String name) throws IoTDBConnectionException, StatementExecutionException, IOException {
        schemaList.add(new MeasurementSchema(name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        List<String> paths = new ArrayList<>(1);
        paths.add(database + "." + name);
        devicePaths.add(database + "." + name);
        addTSIntoTemplate(templateName, name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null);
        getTSCountInTemplate(templateName, true);
        getSetPathsCount(templateName, true);
        getActivePathsCount(templateName, true);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert checkUsingTemplate(paths.get(0), verbose) : "激活成功";
    }

    @DataProvider(name="insertNames", parallel = true)
    public Object[][] getInsertNames() throws IOException {
        return new Object[][]{
                {"`*`"} ,
//                {"SET_STORAGE_GROUP"} ,
//                {"null"} ,
//                {"TRUE"} ,
//                {"true"} ,
//                {"FALSE"} ,
//                {"false"}
        };
    }
    //
    @Test(dataProvider = "insertNames")
    public void testInsert(String name) throws IoTDBConnectionException, StatementExecutionException {
        insertRecordSingle(database+"."+name+"."+name, TSDataType.FLOAT, false, null);
    }

    @Test
    public void test2() throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypeList = new ArrayList<>(1);
        List<String> measurement = new ArrayList<>();
        measurement.add( "`*`");
        tsDataTypeList.add(TSDataType.INT32);
        session.insertRecord("root.sg.`*`", 1L, measurement, tsDataTypeList, 32);
    }

}
