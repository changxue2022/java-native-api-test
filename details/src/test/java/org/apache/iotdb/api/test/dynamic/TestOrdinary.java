package org.apache.iotdb.api.test.dynamic;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestOrdinary extends BaseTestSuite {
    private List<List<Object>> structures;
    private List<List<Object>> errStructures;
    private List<MeasurementSchema> schemaList = new ArrayList<>();
    private String databasePrefix = "root.db.factory";
    private String templatePrefix = "template0";
    private String tsPrefix = "sensors_";
    private String tsName = "appendSensor";
    private String templateName = templatePrefix+1;
    private int loop = 10000;
    private int expectCount = 0;
    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        cleanup();
        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");
        errStructures = new CustomDataProvider().parseTSStructure("data/ts-structures-error.csv");
//        schemaList.add(new MeasurementSchema("s_append", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    }
    private void cleanup() throws IoTDBConnectionException, StatementExecutionException {
        if (checkStroageGroupExists(databasePrefix)) {
            session.deleteDatabase(databasePrefix);
        }
        if (checkTemplateExists(templateName)) {
            session.dropSchemaTemplate(templateName);
        }
    }
    @AfterClass
    public void AfterClass() throws IoTDBConnectionException, StatementExecutionException {
        cleanup();
    }

    @Test(priority = 10)
    public void createTemplate() throws StatementExecutionException, IoTDBConnectionException, IOException {
        int templateCount = countLines("show schema templates", verbose);
        System.out.println("########################"+structures.size());
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
        if (!checkStroageGroupExists(databasePrefix)) {
            session.createDatabase(databasePrefix);
        }
        session.setSchemaTemplate(templateName, databasePrefix);
        assert checkTemplateContainPath(templateName, databasePrefix);
    }
//    @Test(priority = 20)
//    // TIMECHODB-105
//    public void testReAdd() throws IoTDBConnectionException, IOException, StatementExecutionException {
//        expectCount = getTSCountInTemplate(templateName, verbose);
//        expectCount++;
//        addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列成功";
//        schemaList.add(new MeasurementSchema(tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY));
//        // 再次增加
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：再次增加同名失败";
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            int i=0;
//            addTSIntoTemplate(templateName, tsPrefix+i, (TSDataType) structures.get(i).get(0),
//                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2));
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加不同类型同名模版列：失败";
//    }
////    @Test(priority = 21)
//    public void testReAdd_auto() throws IoTDBConnectionException, StatementExecutionException {
//        if (!checkStroageGroupExists(databasePrefix)) {
//            session.createDatabase(databasePrefix);
//            session.setSchemaTemplate(templateName, databasePrefix);
//        }
//        List<String> paths = new ArrayList<>(1);
//        paths.add(databasePrefix+".d_0");
//        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
//        schemaList.add(schemaList.get(schemaList.size()-1));
//        insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
//        // 写入两列同样的失败
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            insertTabletMulti(paths.get(0), schemaList, 10, isAligned);
//        });
//        schemaList.remove(schemaList.size() - 1);
//    }
//    @Test(priority = 22)
//    public void testReAdd_otherType() {
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
//        });
//    }
//    @Test(priority = 23)
//    public void testReAdd_inBatchDuplicate() throws IoTDBConnectionException, StatementExecutionException {
//        expectCount = getTSCountInTemplate(templateName, verbose);
//        List<String> names = new ArrayList<>(2);
//        List<TSDataType> tsDataTypeList = new ArrayList<>(2);
//        List<TSEncoding> tsEncodingList = new ArrayList<>(2);
//        List<CompressionType> compressionTypeList = new ArrayList<>(2);
//
//        for (int i = 0; i < 2; i++) {
//            names.add("newOne");
//            tsDataTypeList.add(TSDataType.BOOLEAN);
//            tsEncodingList.add(TSEncoding.PLAIN);
//            compressionTypeList.add(CompressionType.UNCOMPRESSED);
//        }
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, names, tsDataTypeList, tsEncodingList, compressionTypeList);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：批量增加2列同名失败";
//    }

//    @Test(priority = 30) //目前接口未实现，不测试
//    public void testNullParams_templateName() throws IoTDBConnectionException, StatementExecutionException {
//        expectCount = getTSCountInTemplate(templateName, verbose);
//        this.tsName = "nullTest";
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(null, tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：templateName is null";
//        List<String> paths = new ArrayList<>(1);
//        List<TSDataType> tsDataTypes = new ArrayList<>(1);
//        List<TSEncoding> tsEncodings = new ArrayList<>(1);
//        List<CompressionType> compressionTypes = new ArrayList<>(1);
//        paths.add(tsName);
//        tsDataTypes.add(TSDataType.BOOLEAN);
//        tsEncodings.add(TSEncoding.PLAIN);
//        compressionTypes.add(CompressionType.UNCOMPRESSED);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(null, paths, tsDataTypes, tsEncodings, compressionTypes);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：templateName is null";
//    }
//    @Test(priority = 31)
//    public void testNullParams_tsName() throws IoTDBConnectionException, StatementExecutionException {
//        expectCount = getTSCountInTemplate(templateName, verbose);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, null, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：tsName is null";
//        List<String> paths = new ArrayList<>(1);
//        List<TSDataType> tsDataTypes = new ArrayList<>(1);
//        List<TSEncoding> tsEncodings = new ArrayList<>(1);
//        List<CompressionType> compressionTypes = new ArrayList<>(1);
//        paths.add(tsName);
//        tsDataTypes.add(TSDataType.BOOLEAN);
//        tsEncodings.add(TSEncoding.PLAIN);
//        compressionTypes.add(CompressionType.UNCOMPRESSED);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, null, tsDataTypes, tsEncodings, compressionTypes);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：nameList is null";
//    }
//    @Test(priority = 32)
//    public void testNullParams_dataType() throws IoTDBConnectionException, StatementExecutionException {
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, tsName, null, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：dataType is null";
//        List<String> paths = new ArrayList<>(1);
//        List<TSDataType> tsDataTypes = new ArrayList<>(1);
//        List<TSEncoding> tsEncodings = new ArrayList<>(1);
//        List<CompressionType> compressionTypes = new ArrayList<>(1);
//        paths.add(tsName);
//        tsDataTypes.add(TSDataType.BOOLEAN);
//        tsEncodings.add(TSEncoding.PLAIN);
//        compressionTypes.add(CompressionType.UNCOMPRESSED);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, paths, null, tsEncodings, compressionTypes);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：dataTypeList is null";
//    }
//    @Test(priority = 33)
//    public void testNullParams_encoding() throws IoTDBConnectionException, StatementExecutionException {
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, tsName, TSDataType.INT32, null, CompressionType.UNCOMPRESSED);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：encoding is null";
//        List<String> paths = new ArrayList<>(1);
//        List<TSDataType> tsDataTypes = new ArrayList<>(1);
//        List<TSEncoding> tsEncodings = new ArrayList<>(1);
//        List<CompressionType> compressionTypes = new ArrayList<>(1);
//        paths.add(tsName);
//        tsDataTypes.add(TSDataType.BOOLEAN);
//        tsEncodings.add(TSEncoding.PLAIN);
//        compressionTypes.add(CompressionType.UNCOMPRESSED);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, paths, tsDataTypes, null, compressionTypes);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：encoding is null";
//    }
//    @Test(priority = 34)
//    public void testNullParams_compress() throws IoTDBConnectionException, StatementExecutionException {
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, tsName, TSDataType.INT32, TSEncoding.PLAIN, null);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：compress is null";
//        List<String> paths = new ArrayList<>(1);
//        List<TSDataType> tsDataTypes = new ArrayList<>(1);
//        List<TSEncoding> tsEncodings = new ArrayList<>(1);
//        List<CompressionType> compressionTypes = new ArrayList<>(1);
//        paths.add(tsName);
//        tsDataTypes.add(TSDataType.BOOLEAN);
//        tsEncodings.add(TSEncoding.PLAIN);
//        compressionTypes.add(CompressionType.UNCOMPRESSED);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            addTSIntoTemplate(templateName, paths, tsDataTypes, tsEncodings, null);
//        });
//        assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加模版列：compressList is null";
//    }

    @Test(priority = 40)
    public void testInsert_noTemp() throws IoTDBConnectionException, StatementExecutionException {
        String database = databasePrefix+"_a22";
        String d = database+".d_000";
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        if(auto_create_schema) {
            insertTabletMulti(d, schemaList, 1, isAligned);
        } else {
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletMulti(d, schemaList, 1, isAligned);
            });
        }
        session.deleteDatabase(database);
    }
    @Test(priority = 50)
    public void testTSAndTemplate() throws IoTDBConnectionException, StatementExecutionException {
        String d = databasePrefix+"_a22.d_000";
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
    }
    @Test(priority = 51)
    public void testTemplateAndTS() throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        String database = databasePrefix+"_b11";
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
            session.setSchemaTemplate(templateName, database);
        }
        String d = database+".d_001";
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
        session.deleteTimeseries(database);
    }

    @Test(priority = 60)
    public void testAdd0() throws IoTDBConnectionException, IOException, StatementExecutionException {
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

    @DataProvider(name="getNormalNames")
    public Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }
    @Test(priority = 70, dataProvider = "getNormalNames")
    public void testNormalNames(String name, String comment) throws IoTDBConnectionException, StatementExecutionException, IOException {
        schemaList.add(new MeasurementSchema(name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        expectCount++;
        List<String> paths = new ArrayList<>(1);
        paths.add(databasePrefix + "." + name);
        if (!auto_create_schema) {
            addTSIntoTemplate(templateName, name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
//            assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加TS:" + name+" expectCount="+expectCount;
            session.createTimeseriesUsingSchemaTemplate(paths);
            assert checkUsingTemplate(paths.get(0), verbose) : "激活成功";
        }
        insertTabletMulti(paths.get(0), schemaList, expectCount, isAligned);
    }
    @DataProvider(name="getErrorNames")
    public Iterator<Object[]> getErrorNames () throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }
    @Test(priority = 71, dataProvider = "getErrorNames", expectedExceptions = StatementExecutionException.class)
    public void testErrorNames(String name, String comment) throws IoTDBConnectionException, StatementExecutionException, IOException {
        if (!auto_create_schema) {
            addTSIntoTemplate(templateName, name, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        } else {
            insertTabletSingle(databasePrefix+".d_2.", name, TSDataType.FLOAT, 1, isAligned);
        }
    }

    @Test(priority = 80)
    public void testStructures() throws IoTDBConnectionException, StatementExecutionException, IOException {
        String name = "tsPrefix_1";
        List<String> paths = new ArrayList<>(1);
        paths.add(databasePrefix + "." + name);
        List<String> tsNames = new ArrayList<>(structures.size());
        List<TSDataType> tsDataTypes = new ArrayList<>(structures.size());
        List<TSEncoding> tsEncodings = new ArrayList<>(structures.size());
        List<CompressionType> compressionTypes = new ArrayList<>(structures.size());
        for (int i = 0; i < structures.size(); i++) {
            tsNames.add(name+i);
            tsDataTypes.add((TSDataType) structures.get(i).get(0));
            tsEncodings.add((TSEncoding) structures.get(i).get(1));
            compressionTypes.add((CompressionType) structures.get(i).get(2));
            schemaList.add(new MeasurementSchema(name + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        expectCount += structures.size();
        if (!auto_create_schema) {
            addTSIntoTemplate(templateName, tsNames, tsDataTypes, tsEncodings, compressionTypes);
//            assert expectCount == getTSCountInTemplate(templateName, verbose) : "增加TS:" + name;
            session.createTimeseriesUsingSchemaTemplate(paths);
            assert checkUsingTemplate(paths.get(0), verbose) : "激活成功";
        }
        insertTabletMulti(paths.get(0), schemaList, expectCount, isAligned);
    }
    @Test(priority = 81)
    public void testErrorStructure() {
        for (int i = 0; i < errStructures.size(); i++) {
            final int j = i;
            Assert.assertThrows(StatementExecutionException.class, ()->{
                addTSIntoTemplate(templateName, "ts_name",
                        (TSDataType)errStructures.get(j).get(0), (TSEncoding)errStructures.get(j).get(1),
                        (CompressionType)errStructures.get(j).get(2));
            });
        }
    }

    @Test(priority = 100)
    public void testMultiAdding() throws IoTDBConnectionException, StatementExecutionException {
        int appendCount = 3000;
        int i = 0;
        String device = databasePrefix + "2.device_" + i;
        List<String> tsNames = new ArrayList<>(appendCount);
        List<TSDataType> tsDataTypes = new ArrayList<>(appendCount);
        List<TSEncoding> tsEncodings = new ArrayList<>(appendCount);
        List<CompressionType> compressionTypes = new ArrayList<>(appendCount);
        for (int k = 0; k < appendCount; k++) {
            tsNames.add(tsPrefix+k);
            i = k%structures.size();
            tsDataTypes.add((TSDataType) structures.get(i).get(0));
            tsEncodings.add((TSEncoding) structures.get(i).get(1));
            compressionTypes.add((CompressionType) structures.get(i).get(2));
            schemaList.add(new MeasurementSchema(tsPrefix + k,
                    (TSDataType) structures.get(i).get(0), (TSEncoding) structures.get(i).get(1),
                    (CompressionType) structures.get(i).get(2)));
        }
        System.out.println("auto_create_schema="+auto_create_schema);
        System.out.println("isAligned="+isAligned);
        addTSIntoTemplate(templateName,tsNames,tsDataTypes, tsEncodings,compressionTypes);
//        int actualCount = getTSCountInTemplate(templateName, verbose);
//        int expectCount = appendCount + structures.size();
//        assert expectCount == actualCount: "append 成功:expect="+expectCount+" actual="+actualCount;
        insertTabletMulti(device, schemaList, appendCount, isAligned);
        session.deleteDatabase(databasePrefix+2);

    }
    @Test(enabled = false, priority = 120) //189130 //3930 insert
    public void testContinuousAdding() throws IoTDBConnectionException, StatementExecutionException {
        String database = databasePrefix+1;
        int maxLength = 2;
        int appendCount = 100;
        List<String> devicePaths = new ArrayList<>(maxLength);
        for (int i = 0; i < maxLength; i++) {
            devicePaths.add(database + ".device_" + i);
        }
        session.createDatabase(database);
        session.setSchemaTemplate(templateName, database);
        session.createTimeseriesUsingSchemaTemplate(devicePaths);

        int baseIndex = structures.size();
        int index = baseIndex ;
        for (int l = 0; l < loop; l++) {
            for (int i = 0; i < structures.size(); i++) {
                List<String> tsNames = new ArrayList<>(appendCount);
                List<TSDataType> tsDataTypes = new ArrayList<>(appendCount);
                List<TSEncoding> tsEncodings = new ArrayList<>(appendCount);
                List<CompressionType> compressionTypes = new ArrayList<>(appendCount);
                for (int k = 0; k < appendCount; k++) {
                    index++;
//                    System.out.println("l="+l+" i="+i+" k="+k+" index="+index);
                    tsNames.add(tsPrefix+index);
                    tsDataTypes.add((TSDataType) structures.get(i).get(0));
                    tsEncodings.add((TSEncoding) structures.get(i).get(1));
                    compressionTypes.add((CompressionType) structures.get(i).get(2));
                    schemaList.add(new MeasurementSchema(tsPrefix + index,
                            (TSDataType) structures.get(i).get(0), (TSEncoding) structures.get(i).get(1),
                            (CompressionType) structures.get(i).get(2)));
                }
                addTSIntoTemplate(templateName,tsNames,tsDataTypes, tsEncodings,compressionTypes);
                for (int j = 0; j < maxLength; j++) {
                    insertTabletMulti(devicePaths.get(j), schemaList, index, isAligned);
                }
            }
        }
        session.deleteDatabase(databasePrefix+1);
    }


}
