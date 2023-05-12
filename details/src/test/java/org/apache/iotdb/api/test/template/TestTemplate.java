package org.apache.iotdb.api.test.template;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.Tools;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class TestTemplate extends BaseTestSuite {
    private final String templatePrefix = "template";
    private String tName = templatePrefix +"0";
    private String databasePrefix = "root.template";
    private String database = "root.parent";
//    private final String[] databases = new String[]{"root.template.aligned", "root.template.nonAligned"};
    private int expectCount = 17;
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    private List<String> measurements = new ArrayList<>(7);
    private List<TSDataType> dataTypes = new ArrayList<>(7);
    private List<MeasurementSchema> schemaList = new ArrayList<>(7);// tablet
    private List<List<Object>> structures;
    private List<List<Object>> errStructures;

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");
        errStructures = new CustomDataProvider().parseTSStructure("data/ts-structures-error.csv");

        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY});
        structureInfo.forEach((key, value) -> {
            measurements.add(key);
            dataTypes.add((TSDataType) value[0]);
            schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
    }

    @AfterClass(enabled = true)
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
    }

    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-records.csv").getData();
    }

    @DataProvider(name="getErrorNames")
    public Iterator<Object[]> getErrorNames () throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }

    @DataProvider(name="getNormalNames")
    public Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }

    private void createTemplate (String templateName, String loadNode, boolean isAligned) throws
    IoTDBConnectionException, StatementExecutionException, IOException {
        int templateCount = countLines("show schema templates", true);
        Template template = new Template(templateName, isAligned);

        structureInfo.forEach((key, value) -> {
            MeasurementNode mNode =
                    new MeasurementNode(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
            try {
                template.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        session.createSchemaTemplate(template);
        //  IOTDB-5437 StatementExecutionException: 300: COUNT_MEASUREMENTShas not been supported.
//        assert 6 == session.countMeasurementsInTemplate(templateName) : "查看模版中sensor数目";
        if (!loadNode.isEmpty()) {
            session.setSchemaTemplate(templateName, loadNode);
            assert checkTemplateContainPath(templateName, loadNode) : "挂载模版成功";
        }
        assert 1 + templateCount == countLines("show schema templates", true) : "创建模版成功";
        insertTabletMulti(loadNode, schemaList, 10, isAligned);
    }
//////////////////////////////////////////////////////////////////////////////////
    @Test(priority = 17)
    public void createTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        createTemplate(tName, "", isAligned);
    }
    @Test(priority = 21)
    public void testSet_database() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(databasePrefix);
        session.setSchemaTemplate(tName, databasePrefix);
        assert checkTemplateContainPath(tName, databasePrefix) : "挂载模版成功";
        // 再次挂载失败
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.setSchemaTemplate(tName, databasePrefix);
        });
    }
    @Test(priority = 22, expectedExceptions = StatementExecutionException.class)
    public void testSet_childPath() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, databasePrefix+".childPath");
    }
    @Test(priority = 23, expectedExceptions = StatementExecutionException.class)
    public void testSet_parentPath() throws IoTDBConnectionException, StatementExecutionException {
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        String loadPath = database+".level1.level2.level3.level4.childPath";
        session.setSchemaTemplate(tName, loadPath);
        assert checkTemplateContainPath(tName, loadPath) : "挂载模版成功";
        session.setSchemaTemplate(tName, database);
     }
     @Test(priority = 24, expectedExceptions = StatementExecutionException.class)
     public void testSet_2template() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 同一个database挂载2个template
        createTemplate(tName, databasePrefix, isAligned);
     }

    @Test(priority = 31, dataProvider = "getNormalNames")
    public void testCreateTemplate_nameNormal (String name, String comment) throws IoTDBConnectionException, IOException, StatementExecutionException {
        if (checkStroageGroupExists(databasePrefix)) {
            session.deleteDatabase(databasePrefix);
            session.createDatabase(databasePrefix);
        }
        String loadNode = databasePrefix + "." + name;
        createTemplate(name, loadNode, isAligned);
        session.deleteDatabase(databasePrefix);
        session.dropSchemaTemplate(name);
        session.createDatabase(databasePrefix);
        // IOTDB-5469
    }

    // IOTDB-5233
    @Test(priority = 32, dataProvider = "getErrorNames", expectedExceptions = StatementExecutionException.class)
    public void testCreateTemplate_nameError(String templateName, String comment) throws IoTDBConnectionException, IOException, StatementExecutionException {
        createTemplate(templateName, "", isAligned);
    }

    @Test(priority = 40)
    public void testCreateTemplate_structNormal() throws IoTDBConnectionException, StatementExecutionException, IOException {
        String templateName = templatePrefix+"_struct";
        String database = databasePrefix;
        List<MeasurementSchema> schemaList = new ArrayList<>(structures.size());
        if (!checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        int templateCount = countLines("show schema templates", verbose);
        Template template = new Template(templateName, isAligned);
        for (int i = 0; i < structures.size() ; i++) {
            template.addToTemplate(new MeasurementNode("s_"+i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
            schemaList.add(new MeasurementSchema("s_"+i,  (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        session.createSchemaTemplate(template);
        assert 1 + templateCount == countLines("show schema templates", verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:"+templateName+" - "+database;
        List<String> paths = new ArrayList<>(1);
        paths.add(database + ".device");
        session.createTimeseriesUsingSchemaTemplate(paths);
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
    }

    @Test(priority = 42)
    public void testCreateTemplate_structError() throws StatementExecutionException, IoTDBConnectionException {
        String templateName = templatePrefix +"_err";
        int expectCount = countLines("show schema templates", verbose);
        for (int i = 0; i < errStructures.size(); i++) {
            Template template = new Template(templateName, isAligned);
            MeasurementNode mNode = new MeasurementNode("s_err", (TSDataType) errStructures.get(i).get(0),
                    (TSEncoding) errStructures.get(i).get(1), (CompressionType) errStructures.get(i).get(2));
            template.addToTemplate(mNode);
            Assert.assertThrows(StatementExecutionException.class, ()->{
                session.createSchemaTemplate(template);
            });
            assert expectCount == countLines("show schema templates", verbose) : "创建模版失败:"+i;
        }
    }

    @Test(priority = 50, expectedExceptions = StatementExecutionException.class)
    public void testCreateTemplate_0TS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String templateName = templatePrefix +"_0TS";
        Template template = new Template(templateName, isAligned);
        session.createSchemaTemplate(template);
    }
    @Test(priority = 51)
    public void testCreateTemplate_1TS() throws StatementExecutionException, IoTDBConnectionException, IOException {
        String templateName = templatePrefix +"_1TS";
        String database = databasePrefix+"_1TS";
        Template template = new Template(templateName, isAligned);
        List<Object> struct = Tools.getRandom(structures);
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        template.addToTemplate(new MeasurementNode("s_0", (TSDataType) struct.get(0),
                (TSEncoding) struct.get(1), (CompressionType) struct.get(2)));
        int templateCount = countLines("show schema templates", verbose);
        int deviceCount = countLines("show devices "+database+".**", verbose);
        session.createSchemaTemplate(template);
        assert 1 + templateCount == countLines("show schema templates", verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:"+templateName+" - "+database;
        List<String> paths = new ArrayList<>(2);
        paths.add(database + ".device1");
        paths.add(database + ".device2");
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert checkTemplateContainPath(templateName, paths.get(0)): "激活成功：查询template绑定的path1";
        assert checkTemplateContainPath(templateName, paths.get(1)): "激活成功：查询template绑定的path2";
        assert deviceCount+2 == countLines("show devices "+database+".**", verbose): "激活成功check devices："+database;
        assert deviceCount+2 == countLines("show timeseries "+database+".**", verbose): "激活成功check timeseries："+database;
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        insertTabletMulti(paths.get(1), schemaList, 10, isAligned);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.dropSchemaTemplate(templateName);
        });
        session.deleteDatabase(database);
        assert checkStroageGroupExists(database) : "database已经删除："+database;
        session.dropSchemaTemplate(templateName);
        assert checkTemplateExists(templateName) : "template已经删除:"+templateName;
    }

    @Test(priority = 52)
    public void testCreateTemplate_maxTS() throws StatementExecutionException, IoTDBConnectionException, IOException {
        String templateName = templatePrefix +"_maxTS";
        String database = databasePrefix+"_maxTS";
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        Template template = new Template(templateName, isAligned);
        int loop = 10;
        int maxLength = loop*structures.size();
        for (int i = 0; i < loop; i++) {
            for (int j = 0; j < structures.size(); j++) {
                template.addToTemplate(new MeasurementNode("s_"+(i*10+j), (TSDataType) structures.get(i).get(0),
                        (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
            }
        }
        int templateCount = countLines("show schema templates", verbose);
        int deviceCount = countLines("show devices "+database+".**", verbose);
        session.createSchemaTemplate(template);
        assert 1 + templateCount == countLines("show schema templates", verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:"+templateName+" - "+database;
        List<String> paths = new ArrayList<>(maxLength);
        for (int i = 0; i < maxLength; i++) {
            paths.add(database + ".device"+i);
        }
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert maxLength == countLines("show paths set schema template "+templateName, verbose) : "激活成功：查询template绑定path数量";
        assert deviceCount+maxLength == countLines("show devices "+database+".**", verbose): "激活成功check devices："+database;
        assert deviceCount+maxLength == countLines("show timeseries "+database+".**", verbose): "激活成功check timeseries："+database;
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        insertTabletMulti(paths.get(1), schemaList, 10, isAligned);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.dropSchemaTemplate(templateName);
        });
//        for (int i = 0; i < maxLength; i++) {
//            session.unsetSchemaTemplate(paths.get(i), templateName);
//            assert (maxLength-i-1) == countLines("show paths set schema template "+templateName, verbose) : "卸载成功：查询template绑定path数量";
//        }

        session.deleteDatabase(database);
        assert !checkStroageGroupExists(database) : "database已经删除："+database;
        session.dropSchemaTemplate(templateName);
        assert !checkTemplateExists(templateName) : "template已经删除:"+templateName;
    }

    @Test(priority = 60)
    public void testSet_max() throws IoTDBConnectionException, StatementExecutionException {
        String templateName = templatePrefix+"_struct";
        String database = databasePrefix + ".struct";
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        session.createDatabase(database);
        int maxLength = 10000000;
        List<String> paths = new ArrayList<>(maxLength);
        for (int i = 0; i < maxLength ; i++) {
            paths.add(database+".device_"+i);
            session.setSchemaTemplate(templateName, database+".device_"+i);
        }
        assert maxLength == countLines("show paths set schema template "+templateName, verbose) : "激活成功：查询template绑定path数量";
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert maxLength == countLines("show devices "+database+".**", verbose): "激活成功check devices："+database;
        assert maxLength*structures.size() == countLines("show timeseries "+database+".**", verbose): "激活成功check timeseries："+database;
    }




}
