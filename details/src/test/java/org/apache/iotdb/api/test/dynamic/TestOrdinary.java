package org.apache.iotdb.api.test.dynamic;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.PrepareConnection;
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
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class TestOrdinary extends BaseTestSuite {
    private Logger logger = Logger.getLogger(TestOrdinary.class);
    private List<List<Object>> structures;
    private Vector<IMeasurementSchema> schemaList = new Vector<>();
    private List<String> devicePaths = new ArrayList<>();
    private String database = "root.db.factory";
    private String tsPrefix = "sensors_";
    private String tsName = "appendSensor";
    private String templateName = "template01";
    private int loop = 10000;
    private int expectCount = 0;
    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(database);
    }
//    @AfterClass
    public void AfterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase(database);
        cleanTemplates(verbose);
    }
    @DataProvider(name="getNormalStructures", parallel = true)
    public Iterator<Object[]> getNormalStructures() throws IOException {
        CustomDataProvider provider = new CustomDataProvider();
        structures = provider.parseTSStructure("data/ts-structures.csv");
        return provider.getData();
    }

    @Test(priority = 5, dataProvider = "getNormalStructures")
    public void testCreateSingle_struct(TSDataType tsDataType, TSEncoding encoding, CompressionType compressionType, String comment, String index) throws StatementExecutionException, IoTDBConnectionException, IOException {
        final String tName = templateName+index;
        String path = database + ".d"+index;
        assert false == checkTemplateExists(tName) : "没有template:"+tName;
        String name = "ts_"+index;
        Template template = new Template(tName, isAligned);
        template.addToTemplate(new MeasurementNode(name, tsDataType, encoding, compressionType));
        session.createSchemaTemplate(template);
        assert checkTemplateExists(tName) : "创建template成功";
        session.setSchemaTemplate(tName, path);
        assert checkTemplateContainPath(tName, path):"挂载成功";
        assert 1 == getSetPathsCount(tName, verbose):"挂载成功:挂载路径数量";
        if (!auto_create_schema) {
            List<String> paths = new ArrayList<>(1);
            paths.add(path);
            session.createTimeseriesUsingSchemaTemplate(paths);
            assert checkUsingTemplate(path, verbose) : "使用了模版";
            assert 1 == getActivePathsCount(tName, verbose): "激活成功";
        }
        insertRecordSingle(path+"."+name, tsDataType, isAligned, null);
        assert 1 == getTSCountInTemplate(tName, verbose) : "修改前模版ts数量1";
        if (!auto_create_schema) {
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertRecordSingle(path + ".ts_append", tsDataType, isAligned, null);
            });
        }
        addTSIntoTemplate(tName, "ts_append", tsDataType, encoding, compressionType, null);
        assert 2 == getTSCountInTemplate(tName, verbose) : "修改前模版ts数量2";
        insertRecordSingle(path+".ts_append", tsDataType, isAligned, null);
        if (auto_create_schema) {
            insertRecordSingle(path + ".ts_append_auto", tsDataType, isAligned, null);
            assert 3 == getTSCountInTemplate(tName, verbose) : "直接插入自动修改模版ts数量3";
        }
        deactiveTemplate(tName, path);
        session.unsetSchemaTemplate(path, tName);
        assert 0 == getSetPathsCount(tName, verbose):"挂载成功:挂载路径数量";
        session.dropSchemaTemplate(tName);
    }

    @Test(priority = 10)
    public void createTemplate() throws StatementExecutionException, IoTDBConnectionException, IOException {
        int templateCount = getTemplateCount(verbose);
        Template template = new Template(templateName, isAligned);
        for (int i = 0; i < structures.size(); i++) {
            String tsName = String.format("%s%03d", tsPrefix, i);
            template.addToTemplate(new MeasurementNode(tsName, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
            schemaList.add(new MeasurementSchema(tsName, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        session.createSchemaTemplate(template);
        assert checkTemplateExists(templateName) : "创建template成功";
        assert (templateCount+1) == getTemplateCount(verbose) : "创建template成功";
        assert 0 == getSetPathsCount(templateName, verbose) : "未激活";
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database);
        assert 1 == getSetPathsCount(templateName, verbose) : "未激活";
        assert 0 == getActivePathsCount(templateName, verbose) : "未激活";
        String d = database+".d1";
        devicePaths.add(d);
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        assert 1 == getActivePathsCount(templateName, verbose) : "激活成功";
        assert structures.size() == getTSCountInTemplate(templateName, verbose) : "模版内TS数量验证: expect "+ structures.size();
        insertTabletMulti(d, schemaList, 1, isAligned);
        assert checkUsingTemplate(d, verbose) : d+"使用了模版";
    }
    @Test(priority = 20)
    // TIMECHODB-105
    public void testReAdd() throws IoTDBConnectionException, IOException, StatementExecutionException {
        expectCount = getTSCountInTemplate(templateName, verbose);
        expectCount++;
        addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列成功";
        schemaList.add(new MeasurementSchema(tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null));
        // 再次增加
        Assert.assertThrows(StatementExecutionException.class, ()->{
            addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        });
        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：再次增加同名失败";
        Assert.assertThrows(StatementExecutionException.class, ()->{
            int i=100;
            addTSIntoTemplate(templateName, tsPrefix+i, (TSDataType) structures.get(1).get(0),
                    (TSEncoding) structures.get(1).get(1), (CompressionType) structures.get(1).get(2), null);
        });
        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加不同类型同名模版列：失败";
    }
    @Test(priority = 21)
    public void testReAdd_auto() throws IoTDBConnectionException, StatementExecutionException, IOException {
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
            session.setSchemaTemplate(templateName, database);
        }
        List<String> paths = new ArrayList<>(1);
        paths.add(database+".d_0");
        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        schemaList.add(schemaList.get(schemaList.size()-1));
//        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        // 写入两列同样的失败
        Assert.assertThrows(StatementExecutionException.class, ()->{
            insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
        });
        schemaList.remove(schemaList.size() - 1);
    }
    @Test(priority = 22)
    public void testReAdd_otherType() {
        Assert.assertThrows(StatementExecutionException.class, ()->{
            addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null);
        });
    }
    @Test(priority = 23)
    public void testReAdd_inBatchDuplicate() throws IoTDBConnectionException, StatementExecutionException {
        expectCount = getTSCountInTemplate(templateName, verbose);
        List<String> names = new ArrayList<>(2);
        List<TSDataType> tsDataTypeList = new ArrayList<>(2);
        List<TSEncoding> tsEncodingList = new ArrayList<>(2);
        List<CompressionType> compressionTypeList = new ArrayList<>(2);

        for (int i = 0; i < 2; i++) {
            names.add("newOne");
            tsDataTypeList.add(TSDataType.BOOLEAN);
            tsEncodingList.add(TSEncoding.PLAIN);
            compressionTypeList.add(CompressionType.UNCOMPRESSED);
        }
        Assert.assertThrows(StatementExecutionException.class, ()->{
            addTSIntoTemplate(templateName, names, tsDataTypeList, tsEncodingList, compressionTypeList);
        });
        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：批量增加2列同名失败";
    }

    @Test(priority = 40)
    public void testInsert_noTemp() throws IoTDBConnectionException, StatementExecutionException, IOException {
        getStorageGroupCount("root.**", verbose);
        String database = this.database +"_a22";
        String d = database+".d_001";
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        if(auto_create_schema) {
            insertTabletMulti(d, schemaList, 1, isAligned);
            assert false == checkUsingTemplate(d, verbose) : "未使用模版:"+d;
        } else {
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletMulti(d, schemaList, 1, isAligned);
            });
        }
        session.deleteDatabase(database);
    }
    @Test(priority = 50)
    public void testTSAndTemplate() throws IoTDBConnectionException, StatementExecutionException {
        String databaseTmp = database+"_a22";
        String d = databaseTmp +".d_002";
        if (!checkStroageGroupExists(databaseTmp)) {
            session.createDatabase(databaseTmp);
        }
        if (isAligned) {
            List<String> tsNames = new ArrayList<>(1);
            List<TSDataType> tsDataTypes = new ArrayList<>(1);
            List<TSEncoding> tsEncodings = new ArrayList<>(1);
            List<CompressionType> compressionTypes = new ArrayList<>(1);
            tsNames.add("normalTS");
            tsDataTypes.add(TSDataType.FLOAT);
            tsEncodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.UNCOMPRESSED);
            session.createAlignedTimeseries(d, tsNames, tsDataTypes, tsEncodings, compressionTypes, null);
        } else {
            session.createTimeseries(d + ".normalTS", TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        }
        assert 0 < getTimeSeriesCount(d+".**", verbose) : "节点下已经有TS:"+d;
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.setSchemaTemplate(templateName, d);
        });
        session.deleteDatabase(databaseTmp);
    }
    @Test(priority = 51)
    public void testTemplateAndTS() throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<String> paths = new ArrayList<>(1);
        String databaseTmp = this.database +"_b11";
        Session session = PrepareConnection.getSession();
        if (!checkStroageGroupExists(databaseTmp)) {
            session.createDatabase(databaseTmp);
            session.setSchemaTemplate(templateName, databaseTmp);
        }
        String d = databaseTmp+".d_001";
        paths.add(d);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert checkUsingTemplate(d, verbose) : "已经使用模版:"+d;

        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseries(d+".normalTS", TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        });
        if (!auto_create_schema) {
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletSingle(d , "normalTS2", TSDataType.FLOAT, 1, isAligned);
            });
        }
        session.deleteDatabase(databaseTmp);
    }

    @Test(priority = 60)
    public void testAdd0() throws IoTDBConnectionException, StatementExecutionException {
        expectCount = getTSCountInTemplate(templateName, verbose);
        List<String> paths = new ArrayList<>(0);
        List<TSDataType> tsDataTypes = new ArrayList<>(0);
        List<TSEncoding> tsEncodings = new ArrayList<>(0);
        List<CompressionType> compressionTypes = new ArrayList<>(0);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            addTSIntoTemplate(templateName, paths, tsDataTypes, tsEncodings, compressionTypes);
        });
        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加0个";
    }

    @DataProvider(name="getNormalNames", parallel = true)
    public Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }
    @Test(priority = 65, dataProvider = "getNormalNames")
    public void testAddTSToTemplate_normalNames(String name, String comment, String index) throws IoTDBConnectionException, StatementExecutionException, IOException {
        schemaList.add(new MeasurementSchema(name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        List<String> paths = new ArrayList<>(1);
        paths.add(database + "." + name);
        devicePaths.add(database + "." + name);

        addTSIntoTemplate(templateName, name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, null);

//        getTSCountInTemplate(templateName, verbose);
        session.createTimeseriesUsingSchemaTemplate(paths);
//        getActivePathsCount(templateName, verbose);
        assert checkUsingTemplate(paths.get(0), verbose) : "激活成功";
    }
    @Test(priority = 67)
    public void testNormalNames_resultCheck() throws IoTDBConnectionException, StatementExecutionException, IOException {
        int actual = getTSCountInTemplate(templateName, verbose);
        assert schemaList.size() ==  actual: "并发修改模版成功：names-normal.csv, actual "+actual + ", expect "+ schemaList.size();
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletMulti(devicePaths.get(i), schemaList, 10, isAligned);
        }
    }
//    @DataProvider(name="getErrorNames", parallel = true)
//    public Iterator<Object[]> getErrorNames () throws IOException {
//        return new CustomDataProvider().load("data/names-error.csv").getData();
//    }
//    @Test(priority = 71, dataProvider = "getErrorNames", expectedExceptions = StatementExecutionException.class)
//    public void testErrorNames(String name, String comment, String index) throws IoTDBConnectionException, StatementExecutionException, IOException {
////        if (!auto_create_schema) {
//            addTSIntoTemplate(templateName, name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, PrepareConnection.getSession());
////        } else {
////            insertTabletSingle(database +".d_"+index, name, TSDataType.FLOAT, 0, isAligned);
////        }
//    }
//    @DataProvider(name="getErrorStructures", parallel = true)
//    public Iterator<Object[]> getErrorStructures () throws IOException {
//        CustomDataProvider provider =  new CustomDataProvider();
//        provider.parseTSStructure("data/ts-structures-error.csv");
//        return provider.getData();
//    }
//    @Test(priority = 80, dataProvider = "getErrorStructures", expectedExceptions = StatementExecutionException.class)
//    public void testErrorStructures(TSDataType tsDataType, TSEncoding encoding, CompressionType compressionType,String comment, String index) throws IoTDBConnectionException, StatementExecutionException, IOException {
//        addTSIntoTemplate(templateName, "ts_error" + index, tsDataType, encoding, compressionType, PrepareConnection.getSession());
//    }
//
//    @Test(enabled = false, priority = 100)
//    public void testMultiAdding() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        int appendCount = 3000;
//        int i = 0;
//        String device = database + "2.device_" + i;
//        List<String> tsNames = new ArrayList<>(appendCount);
//        List<TSDataType> tsDataTypes = new ArrayList<>(appendCount);
//        List<TSEncoding> tsEncodings = new ArrayList<>(appendCount);
//        List<CompressionType> compressionTypes = new ArrayList<>(appendCount);
//        for (int k = 0; k < appendCount; k++) {
//            tsNames.add(tsPrefix+k);
//            i = k%structures.size();
//            tsDataTypes.add((TSDataType) structures.get(i).get(0));
//            tsEncodings.add((TSEncoding) structures.get(i).get(1));
//            compressionTypes.add((CompressionType) structures.get(i).get(2));
//            schemaList.add(new MeasurementSchema(tsPrefix + k,
//                    (TSDataType) structures.get(i).get(0), (TSEncoding) structures.get(i).get(1),
//                    (CompressionType) structures.get(i).get(2)));
//        }
//        logger.debug("auto_create_schema="+auto_create_schema);
//        logger.debug("isAligned="+isAligned);
//        addTSIntoTemplate(templateName,tsNames,tsDataTypes, tsEncodings,compressionTypes);
////        int actualCount = getTSCountInTemplate(templateName, verbose);
////        int expectCount = appendCount + structures.size();
////        assert expectCount == actualCount: "append 成功:expect="+expectCount+" actual="+actualCount;
//        insertTabletMulti(device, schemaList, appendCount, isAligned);
//        session.deleteDatabase(database +2);
//
//    }
//    @Test(enabled = false, priority = 120) //189130 //3930 insert
//    public void testContinuousAdding() throws IoTDBConnectionException, StatementExecutionException, IOException {
//        String database = this.database +1;
//        int maxLength = 2;
//        int appendCount = 100;
//        List<String> devicePaths = new ArrayList<>(maxLength);
//        for (int i = 0; i < maxLength; i++) {
//            devicePaths.add(database + ".device_" + i);
//        }
//        session.createDatabase(database);
//        session.setSchemaTemplate(templateName, database);
//        session.createTimeseriesUsingSchemaTemplate(devicePaths);
//
//        int baseIndex = structures.size();
//        int index = baseIndex ;
//        for (int l = 0; l < loop; l++) {
//            for (int i = 0; i < structures.size(); i++) {
//                List<String> tsNames = new ArrayList<>(appendCount);
//                List<TSDataType> tsDataTypes = new ArrayList<>(appendCount);
//                List<TSEncoding> tsEncodings = new ArrayList<>(appendCount);
//                List<CompressionType> compressionTypes = new ArrayList<>(appendCount);
//                for (int k = 0; k < appendCount; k++) {
//                    index++;
////                    logger.debug("l="+l+" i="+i+" k="+k+" index="+index);
//                    tsNames.add(tsPrefix+index);
//                    tsDataTypes.add((TSDataType) structures.get(i).get(0));
//                    tsEncodings.add((TSEncoding) structures.get(i).get(1));
//                    compressionTypes.add((CompressionType) structures.get(i).get(2));
//                    schemaList.add(new MeasurementSchema(tsPrefix + index,
//                            (TSDataType) structures.get(i).get(0), (TSEncoding) structures.get(i).get(1),
//                            (CompressionType) structures.get(i).get(2)));
//                }
//                addTSIntoTemplate(templateName,tsNames,tsDataTypes, tsEncodings,compressionTypes);
//                for (int j = 0; j < maxLength; j++) {
//                    insertTabletMulti(devicePaths.get(j), schemaList, index, isAligned);
//                }
//            }
//        }
//        session.deleteDatabase(this.database +1);
//    }

}
