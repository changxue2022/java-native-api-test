package org.apache.iotdb.api.test.template;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.api.test.utils.Tools;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
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
    private List<IMeasurementSchema> schemaList = new ArrayList<>(7);// tablet
    private List<List<Object>> structures;
    private List<List<Object>> errStructures;

    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        session.createDatabase(database);
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY});
        structureInfo.forEach((key, value) -> {
            schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
    }

    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/insert-records.csv").getData();
    }

    @DataProvider(name="getErrorNames", parallel = true)
    public Iterator<Object[]> getErrorNames () throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }

    @DataProvider(name="getNormalNames", parallel = true)
    public Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }

    private void createTemplate (String templateName, String loadNode, boolean isAligned, Session session) throws
    IoTDBConnectionException, StatementExecutionException, IOException {
        if (session == null) {
            session = this.session;
        }
//        int templateCount = countLines("show schema templates", true);
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
//        assert 1 + templateCount == countLines("show schema templates", true) : "创建模版成功";
        if (!loadNode.isEmpty()) {
            session.setSchemaTemplate(templateName, loadNode);
            assert checkTemplateContainPath(templateName, loadNode) : "挂载模版成功";
            getSetPathsCount(templateName, verbose);
            insertTabletMulti(loadNode, schemaList, 10, isAligned);
            getRecordCount(loadNode, verbose);
        }
    }
    @DataProvider(name="getNormalStructures", parallel = true)
    public Iterator<Object[]> getNormalStructures() throws IOException {
        CustomDataProvider provider = new CustomDataProvider();
        structures = provider.parseTSStructure("data/ts-structures.csv");
        return provider.getData();
    }
    @DataProvider(name="getErrorStructures", parallel = true)
    public Iterator<Object[]> getErrorStructures() throws IOException {
        CustomDataProvider provider = new CustomDataProvider();
        errStructures = provider.parseTSStructure("data/ts-structures-error.csv");
        return provider.getData();
    }

//////////////////////////////////////////////////////////////////////////////////
    @Test(priority = 5, dataProvider = "getNormalStructures")
    public void testCreateSingle_struct(TSDataType tsDataType, TSEncoding encoding, CompressionType compressionType, String comment, String index) throws StatementExecutionException, IoTDBConnectionException, IOException {
        final String tName = templatePrefix+index;
        String path = database + ".d"+index;
        assert false == checkTemplateExists(tName) : "没有template:"+tName;
        String name = "ts_"+index;
        Template template = new Template(tName, isAligned);
        template.addToTemplate(new MeasurementNode(name, tsDataType, encoding, compressionType));
        session.createSchemaTemplate(template);
        assert checkTemplateExists(tName) : "创建template成功";
        session.setSchemaTemplate(tName, path);
        if (!auto_create_schema) {
            List<String> paths = new ArrayList<>(1);
            paths.add(path);
            session.createTimeseriesUsingSchemaTemplate(paths);
            assert checkUsingTemplate(path, verbose) : "使用了模版";
            assert 1 == getActivePathsCount(tName, verbose): "激活成功";
        }
        insertRecordSingle(path+"."+name, tsDataType, isAligned, null);
//        deactiveTemplate(tName, path);
//        session.unsetSchemaTemplate(path, tName);
//        assert 0 == getSetPathsCount(tName, verbose):"挂载成功:挂载路径数量";
//        session.dropSchemaTemplate(tName);
    }
    @Test(priority = 6, dataProvider = "getErrorStructures", expectedExceptions = StatementExecutionException.class)
    public void testCreateSingle_errorStruct(TSDataType tsDataType, TSEncoding encoding, CompressionType compressionType, String comment, String index) throws StatementExecutionException, IoTDBConnectionException, IOException {
        final String tName = templatePrefix+"Err_"+index;
        Template template = new Template(tName, isAligned);
        template.addToTemplate(new MeasurementNode("err_struct_"+index, tsDataType, encoding, compressionType));
        Session session = PrepareConnection.getSession();
        session.createSchemaTemplate(template);
        session.close();
    }

    @Test(priority = 10, expectedExceptions = StatementExecutionException.class)
    public void testAddDuplicateNodes() throws StatementExecutionException, IoTDBConnectionException, IOException {
        String templateName = "duplicateNodesTemplate";
        Template template = new Template(templateName, isAligned);
        template.addToTemplate(new MeasurementNode("s_key", TSDataType.FLOAT,
                TSEncoding.PLAIN, CompressionType.SNAPPY));
        template.addToTemplate(new MeasurementNode("s_key", TSDataType.FLOAT,
                TSEncoding.PLAIN, CompressionType.SNAPPY));

        session.createSchemaTemplate(template);
    }
    @Test(priority = 17)
    public void createTemplate_aligned() throws IoTDBConnectionException, StatementExecutionException, IOException {
        createTemplate(tName, "", isAligned, null);
    }
    @Test(priority = 21)
    public void testSet_database() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(databasePrefix);
        getSetPathsCount(tName, verbose);
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
        createTemplate(tName, databasePrefix, isAligned, null);
        getSetPathsCount(tName, verbose);
     }

    @Test(enabled = false, priority = 31, dataProvider = "getNormalNames")
    public void testCreateTemplate_nameNormal (String name, String comment, String index) throws IoTDBConnectionException, IOException, StatementExecutionException {
        String database = databasePrefix+index;
        session.createDatabase(database);
        String loadNode = database + "." + name;
        createTemplate(name, loadNode, isAligned, null);
        // 解除
        assert 1 == getActivePathsCount(name, verbose) : "解除模版前："+name;
        deactiveTemplate(name, loadNode);
        assert 0 == getActivePathsCount(name, verbose) : "解除模版成功:"+name;
        // 卸载
        assert 1 == getSetPathsCount(name, verbose) : "解除模版前:"+name;
        session.unsetSchemaTemplate(loadNode, name);
        assert 0 == getSetPathsCount(name, verbose) : "卸载模版成功:"+name;
        // 删除模版
//        session.dropSchemaTemplate(name);
//        session.deleteDatabase(database);
//        session.createDatabase(database);
        // IOTDB-5469
    }

    // IOTDB-5233  TIMECHODB-137
    @Test(enabled = false, priority = 32, dataProvider = "getErrorNames", expectedExceptions = StatementExecutionException.class)
    public void testCreateTemplate_nameError(String templateName, String comment, String index) throws IoTDBConnectionException, IOException, StatementExecutionException {
        createTemplate(templateName, "", isAligned, PrepareConnection.getSession());
    }

    @Test(priority = 40)
    public void testCreateTemplate_structNormal() throws IoTDBConnectionException, StatementExecutionException, IOException {
        getNormalStructures();
        String databaseStr = databasePrefix+89098;
        if (!checkStroageGroupExists(databaseStr)) {
            session.createDatabase(databaseStr);
        }
        String templateName = templatePrefix+"_struct";
        List<IMeasurementSchema> schemaList = new ArrayList<>(structures.size());
        int templateCount = getTemplateCount(verbose);
        Template template = new Template(templateName, isAligned);
        for (int i = 0; i < structures.size() ; i++) {

            template.addToTemplate(new MeasurementNode("s_"+i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
            schemaList.add(new MeasurementSchema("s_"+i,  (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        session.createSchemaTemplate(template);
        assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, databaseStr);
        assert checkTemplateContainPath(templateName, databaseStr) : "挂载模版成功:"+templateName+" - "+databaseStr;
        List<String> paths = new ArrayList<>(1);
        paths.add(databaseStr + ".device");
        session.createTimeseriesUsingSchemaTemplate(paths);
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
    }

    @Test(priority = 42)
    public void testCreateTemplate_structError() throws StatementExecutionException, IoTDBConnectionException {
        String templateName = templatePrefix +"_err";
        int expectCount = getTemplateCount(verbose);
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

    @Test(priority = 50)
    public void testCreateTemplate_0TS() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String database = databasePrefix+"_0TS";
        session.createDatabase(database);
        List<String> templateNames = new ArrayList<>(2);
        List<String> paths = new ArrayList<>(1);
        boolean flg = true;
        for (int i = 0; i < 2; i++) {
            String device = database + ".d"+i;
            paths.add(device);
            templateNames.add(templatePrefix+"_0TS_"+i);
            Template template = new Template(templateNames.get(i), flg);
            session.createSchemaTemplate(template);
            assert checkTemplateExists(templateNames.get(i)) : "创建0TS模版成功对齐:"+flg;
            session.setSchemaTemplate(templateNames.get(i), device);
            assert checkTemplateContainPath(templateNames.get(i), device) : "挂载成功";
            session.createTimeseriesUsingSchemaTemplate(paths);
            assert 1 == getActivePathsCount(templateNames.get(i), verbose) : "激活成功";
            assert 0 == getTimeSeriesCount(device+".**", verbose) : "没有任何TS";
            assert 0 == getDeviceCount(device+".**", verbose) : "没有任何device";
            deactiveTemplate(templateNames.get(i), device);
            assert 0 == getActivePathsCount(templateNames.get(i), verbose) : "激活成功";
            session.unsetSchemaTemplate(device, templateNames.get(i));
            assert 0 == getSetPathsCount(templateNames.get(i), verbose) : "激活成功";
//            session.dropSchemaTemplate(templateNames.get(i));
//            assert false == checkTemplateExists(templateNames.get(i)) : "删除0TS模版成功对齐:"+flg;
            paths.clear();
            flg = false;
        }
//        session.deleteDatabase(database);
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
        int templateCount = getTemplateCount(verbose);
        int deviceCount = getDeviceCount(database+".**", verbose);
        session.createSchemaTemplate(template);
        assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:"+templateName+" - "+database;
        List<String> paths = new ArrayList<>(2);
        paths.add(database + ".device1");
        paths.add(database + ".device2");
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert checkUsingTemplate(paths.get(0), verbose): "激活成功：查询template绑定的path1";
        assert checkUsingTemplate(paths.get(1), verbose): "激活成功：查询template绑定的path2";
        assert deviceCount+2 == countLines("show devices "+database+".**", verbose): "激活成功check devices："+database;
        assert deviceCount+2 == countLines("show timeseries "+database+".**", verbose): "激活成功check timeseries："+database;
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        insertTabletMulti(paths.get(1), schemaList, 10, isAligned);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.dropSchemaTemplate(templateName);
        });
//        session.deleteDatabase(database);
//        assert false == checkStroageGroupExists(database) : "database已经删除："+database;
//        session.dropSchemaTemplate(templateName);
//        assert false == checkTemplateExists(templateName) : "template已经删除:"+templateName;
    }

    @Test(enabled = false, priority = 52) // loop=100 112s; loop=1000 hang?
    public void testCreateTemplate_maxTS() throws StatementExecutionException, IoTDBConnectionException, IOException {
        long start = System.currentTimeMillis();
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
                template.addToTemplate(new MeasurementNode("s_"+(i*structures.size()+j), (TSDataType) structures.get(j).get(0),
                        (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2)));
            }
        }
        int templateCount = getTemplateCount(verbose);
        int deviceCount = getDeviceCount(database+".**", verbose);
        session.createSchemaTemplate(template);
        assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:"+templateName+" - "+database;
        List<String> paths = new ArrayList<>(maxLength);
        for (int i = 0; i < maxLength; i++) {
            paths.add(database + ".device"+i);
        }
        session.createTimeseriesUsingSchemaTemplate(paths);
//        assert maxLength == getActivePathsCount(templateName, verbose) : "激活成功：查询template绑定path数量";
        assert deviceCount+maxLength == getDeviceCount(database+".**", verbose): "激活成功check devices："+database;
        assert maxLength*maxLength == getTimeSeriesCount(database+".**", verbose): "激活成功check timeseries："+database;
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        insertTabletMulti(paths.get(1), schemaList, 10, isAligned);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.dropSchemaTemplate(templateName);
        });
        deactiveTemplate(templateName, database+".**");
//        deactiveTemplate(templateName, paths);
        assert 0 == getActivePathsCount(templateName, verbose) : "解除成功：查询template激活path数量";
        session.unsetSchemaTemplate(database, templateName);

        session.dropSchemaTemplate(templateName);
        assert !checkTemplateExists(templateName) : "template已经删除:"+templateName;

        session.deleteDatabase(database);
        assert !checkStroageGroupExists(database) : "database已经删除："+database;
        long end = System.currentTimeMillis();
        System.out.println("########### testCreateTemplate_maxTS "+loop + " elapse time(s):"+ (end-start)/1000);
    }

    @Test(enabled = false, priority = 60) // maxLength=100 44s;
    public void testSet_max() throws IoTDBConnectionException, StatementExecutionException, IOException {
        long start = System.currentTimeMillis();
        String templateName = templatePrefix + "_setMax";
        String database = databasePrefix + "_setMax";
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        session.createDatabase(database);
        createTemplate(templateName, database+".d", isAligned, null);
        int maxLength = 10;
        List<String> paths = new ArrayList<>(maxLength);
        for (int i = 0; i < maxLength; i++) {
            paths.add(database + ".device_" + i);
            session.setSchemaTemplate(templateName, database + ".device_" + i);
        }
        assert maxLength+1 == getSetPathsCount(templateName, verbose) : "挂载成功：查询template挂载path数量";

        session.createTimeseriesUsingSchemaTemplate(paths);
        assert maxLength+1 == getActivePathsCount(templateName, verbose) : "激活成功：查询template绑定path数量";
        assert maxLength+1 == getDeviceCount( database + ".**", verbose) : "激活成功check devices：" + database;
        assert (maxLength+1) * 6 == getTimeSeriesCount( database + ".**", verbose) : "激活成功check timeseries：" + database;
        for (int i = 0; i < maxLength; i++) {
            insertTabletMulti(paths.get(i), schemaList, 10, isAligned);
        }
        deactiveTemplate(templateName, database+".**");
        session.deleteDatabase(database);
        session.dropSchemaTemplate(templateName);
        long end = System.currentTimeMillis();
        System.out.println("########### testSet_max "+maxLength + " elapse time(s):"+ (end-start)/1000);
    }

}
