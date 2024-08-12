package org.apache.iotdb.api.test.dynamic;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单模版，多database
 */
public class TestDynamicTemplateMin extends BaseTestSuite {
    private List<List<Object>> structures;
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    private List<IMeasurementSchema> schemaList_org = new ArrayList<>(6);
    private List<IMeasurementSchema> schemaList_err = new ArrayList<>(6);
    private List<IMeasurementSchema> schemaList_more = new ArrayList<>();
    private List<IMeasurementSchema> schemaList_less = new ArrayList<>();
    private List<String> databases = new ArrayList<>(3);
    private List<String> devicePaths = new ArrayList<>();
    private String databasePrefix = "root.db.factory";
    private String templateName = "template01";

    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});

        structureInfo.forEach((key, value) -> {
            schemaList_org.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding)value[1], (CompressionType)value[2]));
        });
        for (int i = schemaList_org.size()-1; i >0 ; i--) {
            schemaList_less.add(schemaList_org.get(i));
            schemaList_err.add(schemaList_org.get(i));
        }
        schemaList_err.add(new MeasurementSchema("s_boolean", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY));
    }

    @AfterMethod
    public void afterMethod() throws IoTDBConnectionException, StatementExecutionException {
       cleanDatabases(verbose);
       cleanTemplates(verbose);
    }

    private void business(List<Object> ... structs) throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 初始化（准备）
        List<String> paths = new ArrayList<>(3);
        for (int i = 0; i < schemaList_org.size(); i++) {
            schemaList_more.add(schemaList_org.get(i));
        }
        schemaList_more.add(new MeasurementSchema("s_append"+0,
                (TSDataType) structs[0].get(0), (TSEncoding) structs[0].get(1),
                (CompressionType) structs[0].get(2)));

        // database0 用于主要业务，database1用于清理，database2用于普通序列的测试
        for (int i = 0; i < 3; i++) {
            databases.add(databasePrefix + i);
            if (!checkStroageGroupExists(databases.get(i))) {
                session.createDatabase(databases.get(i));
            }
        }
        String database = databases.get(0);
        String device = "";
        int expectTSCountEach = 0;
        int activeDeviceCount = 0;
        // 创建template
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
        assert checkTemplateExists(templateName) : "创建模版成功";
        assert 0 == getSetPathsCount(templateName, verbose) : "未挂载，未激活";

        // 1. 未使用模版，普通序列
        device = database + ".d0.group0";
        devicePaths.add(device);
        if (auto_create_schema) { // 打开自动创建
            // 写入数据 root.db.factory1.d0.group0, 成功，未使用模版
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            assert false == checkUsingTemplate(device, verbose) : "没有使用模版:"+device;
            assert schemaList_org.size() == getTimeSeriesCount(device + ".**", verbose) : "TS数量："+schemaList_org.size();
        } else { // 关闭自动创建
            // 写入数据 root.db.factory1.d0.group0
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletMulti(database + ".d0.group0", schemaList_org, 1, isAligned);
            });
            if (isAligned) {
                List<TSDataType> dataTypes = new ArrayList<>(structureInfo.size());
                List<TSEncoding> encodings = new ArrayList<>(structureInfo.size());
                List<CompressionType> compressionTypes = new ArrayList<>(structureInfo.size());
                List<String> tsNames = new ArrayList<>(structureInfo.size());
                structureInfo.forEach((key,value)->{
                    dataTypes.add((TSDataType)value[0]);
                    encodings.add((TSEncoding) value[1]);
                    compressionTypes.add((CompressionType) value[2]);
                    tsNames.add(key);
                });
                session.createAlignedTimeseries(device, tsNames, dataTypes, encodings,compressionTypes,tsNames);
            } else {
                List<TSDataType> dataTypes = new ArrayList<>(structureInfo.size());
                List<TSEncoding> encodings = new ArrayList<>(structureInfo.size());
                List<CompressionType> compressionTypes = new ArrayList<>(structureInfo.size());
                List<String> tsNames = new ArrayList<>(structureInfo.size());
                structureInfo.forEach((key,value)->{
                    dataTypes.add((TSDataType)value[0]);
                    encodings.add((TSEncoding) value[1]);
                    compressionTypes.add((CompressionType) value[2]);
                    tsNames.add(database + ".d0.group0"+"."+key);
                });
                session.createMultiTimeseries(tsNames, dataTypes, encodings, compressionTypes, null, null, null, null);
            }
            assert false == checkUsingTemplate(device, verbose) : "没有使用模版:"+device;
            assert schemaList_org.size() == getTimeSeriesCount(device + ".**", verbose) : "TS数量："+schemaList_org.size();
            insertTabletMulti(device, schemaList_org, 10, isAligned);
        }
        // 在已经创建普通序列的节点上，挂载模版，期望失败
        Assert.assertThrows(StatementExecutionException.class, ()-> {
            session.setSchemaTemplate(templateName, devicePaths.get(0));
        });
        // 在已经创建普通序列的节点上，激活模版，期望失败
        Assert.assertThrows(StatementExecutionException.class, ()-> {
            session.createTimeseriesUsingSchemaTemplate(devicePaths);
        });

        // 2. 使用模版: 在设备上挂载和激活, 嵌套设备
        expectTSCountEach = schemaList_org.size();
        // 写入成功，使用了模版
        device = database+".d1.group1";
        devicePaths.add(device);
        session.setSchemaTemplate(templateName, device);
        assert checkTemplateContainPath(templateName, device) : "挂载模版成功:"+device;
        assert activeDeviceCount == getActivePathsCount(templateName, verbose) : "在模版上未做任何激活";
        activeDeviceCount++;
        if (auto_create_schema){
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            assert activeDeviceCount == getActivePathsCount(templateName, verbose) : "自动激活+1";
        } else { // 关闭自动创建schema
            // 不挂载/激活，直接写入
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletMulti(database + ".d1.group1", schemaList_org, 10, isAligned);
            });
            paths.add(device);
            session.createTimeseriesUsingSchemaTemplate(paths);
            assert activeDeviceCount == getActivePathsCount(templateName, verbose) : "激活成功+1";
        }
        assert checkUsingTemplate(device, verbose) : "使用了模版:"+device;
        assert expectTSCountEach == getTSCountInTemplate(templateName, verbose) : "Template内TS数量:"+expectTSCountEach;
        assert expectTSCountEach == getTimeSeriesCount(device+".**", verbose) : "确定设备上TS数量:"+expectTSCountEach;

        // 对齐非对齐错位插入数据
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            insertTabletMulti(database+".d1.group1", schemaList_org, 10, !isAligned);
//        });
        // 激活子节点(嵌套设备)
        device = database + ".d1.group1.subGroup";
        devicePaths.add(device);
        activeDeviceCount++;
        if (!auto_create_schema) {
            paths.clear();
            paths.add(device);
            session.createTimeseriesUsingSchemaTemplate(paths);
        }
        // 写入少于模版的列的数据
        insertTabletMulti(device, schemaList_less, 10, isAligned);
        assert checkUsingTemplate(device, verbose) : "使用了模版:" + device;
        assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:" + expectTSCountEach;
        assert getTimeSeriesCount(device + ".*", verbose) == expectTSCountEach : "TS数量:" + expectTSCountEach;
        assert schemaList_org.size() == getTimeSeriesCount(device + ".*", verbose) : "确定模版内TS数量:" + schemaList_org.size();
        // 写入多于模版列的数据
        device = database + ".d2.group1";
        if (auto_create_schema) {
            activeDeviceCount++;
            expectTSCountEach++;
            session.setSchemaTemplate(templateName, device);
            assert checkTemplateContainPath(templateName, device) : "挂载模版成功:" + device;
            insertTabletMulti(device, schemaList_more, 10, isAligned);
            assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:" + expectTSCountEach;

            assert getTimeSeriesCount(device + ".*", verbose) == expectTSCountEach : "TS数量:" + expectTSCountEach;
            assert checkUsingTemplate(device, verbose) : "使用了模版:" + device;
            assert schemaList_more.size() == getTimeSeriesCount(device + ".*", verbose) : "确定模版内TS数量:" + schemaList_more.size();
            devicePaths.add(device);
        } else {
            Assert.assertThrows(StatementExecutionException.class, ()->{
                insertTabletMulti(database + ".d2.group1", schemaList_more, 1, isAligned);
            });
        }
        // 写入同名不同类型列的数据
        Assert.assertThrows(StatementExecutionException.class, ()->{
            insertTabletMulti(database+".d2.group1", schemaList_err, 2, isAligned);
        });

        // 3. 使用模版：在其他 database 上挂载， 显示激活
        paths.clear();
        for (int i = 0; i < 2; i++) {
            paths.add(databasePrefix+i + ".d1.group3");
        }
        session.setSchemaTemplate(templateName,paths.get(0));
        // 在database上挂载
        session.setSchemaTemplate(templateName,databasePrefix+1);
        activeDeviceCount += paths.size();
        // 显示激活
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert activeDeviceCount == getActivePathsCount(templateName, verbose) : "激活成功:"+paths;
        device = paths.get(0);
        devicePaths.add(device);
        assert checkUsingTemplate(device, verbose) : "激活后，使用了模版:"+device;
        assert expectTSCountEach == getTSCountInTemplate(templateName, verbose) : "Template内TS数量:"+expectTSCountEach;
        assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
        // 写入数据
        insertTabletMulti(device, schemaList_org, 10, isAligned);

        device = paths.get(1);
        devicePaths.add(device);
        assert checkUsingTemplate(device, verbose) : "激活后，使用了模版:"+device;
        assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
        // 写入少列数据
        insertTabletMulti(device, schemaList_less, 10, isAligned);

        // 写入同名不同类型数据
        Assert.assertThrows(StatementExecutionException.class, ()->{
            insertTabletMulti(paths.get(0), schemaList_err, 10, isAligned);
        });

        assert expectTSCountEach == getTSCountInTemplate(templateName, verbose) : "Template内TS数量:"+expectTSCountEach;
        for (int i = 0; i < paths.size(); i++) {
            assert expectTSCountEach == getTimeSeriesCount(paths.get(i)+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
        }

        // 4. 显示追加TS到模版
        paths.clear();
        paths.add(databasePrefix + "1.d1.group1.subGroup");
        paths.add(database + ".d3.group3");
        paths.add(databasePrefix+1 + ".d3.group3");
        for (int i = 0; i < paths.size(); i++) {
            devicePaths.add(paths.get(i));
        }
        session.setSchemaTemplate(templateName, paths.get(1));

        // 修改模版: explicit追加TS
        if (structs.length == 1) {
            expectTSCountEach++;
            addTSIntoTemplate(templateName, "s_append_explicit",
                    (TSDataType) structs[0].get(0), (TSEncoding) structs[0].get(1),
                    (CompressionType) structs[0].get(2), null);
            schemaList_more.add(new MeasurementSchema("s_append_explicit",(TSDataType) structs[0].get(0),
                    (TSEncoding) structs[0].get(1), (CompressionType) structs[0].get(2) ));
        } else {
            expectTSCountEach += structs.length;
            List<String> tsNames = new ArrayList<>(structs.length);
            List<TSDataType> tsDatatypes = new ArrayList<>(structs.length);
            List<TSEncoding> tsEncodings = new ArrayList<>(structs.length);
            List<CompressionType> compressions = new ArrayList<>(structs.length);
            for (int i = 0; i < structs.length; i++) {
                tsNames.add("s_append_" + i);
                tsDatatypes.add((TSDataType)structs[i].get(0));
                tsEncodings.add((TSEncoding)structs[i].get(1));
                compressions.add((CompressionType)structs[i].get(2));
                schemaList_more.add(new MeasurementSchema("s_append_"+i,(TSDataType) structs[i].get(0),
                        (TSEncoding) structs[i].get(1), (CompressionType) structs[i].get(2) ));
            }
            addTSIntoTemplate(templateName, tsNames, tsDatatypes, tsEncodings, compressions);
        }

        // 修改后，使用新的template结构数据插入之前的设备
        insertTabletMulti(device, schemaList_more, 10, isAligned);
        assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;

        session.createTimeseriesUsingSchemaTemplate(paths);
        activeDeviceCount += paths.size();
        assert activeDeviceCount == getActivePathsCount(templateName, verbose) : "激活成功:"+paths;
        for (int i = 0; i < paths.size(); i++) {
            device = paths.get(i);
//                assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
            insertTabletMulti(device, schemaList_more, 10, isAligned);
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            insertTabletMulti(device, schemaList_less, 10, isAligned);
            assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "插入后，确定模版内TS数量 expect:"+expectTSCountEach+" actual: "+getTimeSeriesCount(device+".*", verbose) ;
            assert expectTSCountEach == getTSCountInTemplate(templateName, verbose)  : "Template内TS数量:"+expectTSCountEach;
        }

        // 对所有激活设备插入修改后的数据
        assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "对已经激活模版序列影响:Template内TS数量:"+expectTSCountEach;
        for (int i = 1; i < devicePaths.size(); i++) {
            insertTabletMulti(devicePaths.get(i), schemaList_more, 20, isAligned);
            assert expectTSCountEach == getTimeSeriesCount(devicePaths.get(i)+".*", verbose) : "对已经激活模版序列影响：确定模版内TS数量:"+expectTSCountEach;
        }

        // 删除部分数据 TIMECHODB-104
        // TIMECHODB-526 301: Declared positions (2) does not match column 0's number of entries (3)
        for (int i = 0; i < devicePaths.size(); i++) {
            int recordCount = getCount("select count(*) from "+devicePaths.get(i), verbose);
            countLines("select * from "+devicePaths.get(i), verbose);
            session.deleteData(devicePaths.get(i)+".*", 1672513196000L);
            int actual = countLines("select * from "+devicePaths.get(i), verbose);

            assert recordCount > actual : "删除部分数据成功, expect:"+recordCount+" actual: "+actual;
        }
        // 删除普通序列
        device = devicePaths.get(0);
        int tsCount = getTimeSeriesCount(database+".**", verbose);
        session.executeNonQueryStatement("delete timeseries " + device + ".**");
        assert getTimeSeriesCount(device + ".**", verbose) == 0 : "删除成功";
        assert tsCount - 6 == getTimeSeriesCount(database+".**", verbose) : "删除成功,其他不影响";
        insertTabletMulti(devicePaths.get(1), schemaList_org, 20, isAligned);
        devicePaths.remove(0);

        // 有引用，卸载
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.unsetSchemaTemplate(database, templateName);
        });
        // 有引用，删除模版
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.dropSchemaTemplate(templateName);
        });
        // 解除未使用模版序列
        Assert.assertThrows(StatementExecutionException.class, ()->{
            deactiveTemplate(templateName, database + ".d0.group0");
        });

        // 解除单个节点:最后一个
        device = devicePaths.get(devicePaths.size()-1);
        int count = getActivePathsCount(templateName, verbose);
        count--;
        deactiveTemplate(templateName, device);
        assert 0 == getTimeSeriesCount(device+ ".*", verbose) : "解除模版会删除序列";
        assert count == getActivePathsCount(templateName, verbose) : "模版引用数目-1";
        // 再次解除模版失败
        Assert.assertThrows(StatementExecutionException.class, ()->{
            deactiveTemplate(templateName, devicePaths.get(devicePaths.size()-1));
        });
        devicePaths.remove(devicePaths.size() - 1);

        // 卸载有激活的节点
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.unsetSchemaTemplate(databases.get(1), templateName);
        });

        // 解除第二个database的所有激活节点
        for (int i = 0; i < devicePaths.size(); i++) {
            if (devicePaths.get(i).startsWith(databases.get(1))) {
                deactiveTemplate(templateName, devicePaths.get(i));
            }
        }
        logger.info("database="+databases.get(1));
        session.unsetSchemaTemplate(databases.get(1), templateName);
//        for (int i = 0; i < devicePaths.size(); i++) {
//            if (devicePaths.get(i).startsWith(databases.get(0))) {
//                deactiveTemplate(templateName, devicePaths.get(i));
//            }
//        }

        // TIMECHODB-102
        deactiveTemplate(templateName, "root.db.**");
        // 卸载模版
        assert 0 == getActivePathsCount(templateName, verbose) : "模版引用数目 == 0";
        cleanTemplateNodes(templateName, databases.get(0));
        assert 0 == getSetPathsCount(templateName, verbose) : "模版挂载数目 == 0";
        // 删除database再创建，查看与模版关联是否清除
        session.deleteDatabase(database);
        session.createDatabase(database);
        session.setSchemaTemplate(templateName, devicePaths.get(0));
        session.unsetSchemaTemplate( devicePaths.get(0), templateName);
        // 删除模版
        session.dropSchemaTemplate(templateName);
        assert false == checkTemplateExists(templateName): "删除模版成功";
        // 卸载不存在的模版
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.unsetSchemaTemplate(database, templateName);
        });
        schemaList_more.clear();
        devicePaths.clear();
        databases.clear();
    }

    @Test(priority = 10)
    public void testStructureNormal_addOne() throws IoTDBConnectionException, IOException, StatementExecutionException {
        for (int i = 0; i < structures.size(); i++) {
            business(structures.get(i));
//            break;
        }
    }

    @Test(priority = 20)
    public void testStructure_add6() throws IoTDBConnectionException, IOException, StatementExecutionException {
        List<List<Object>> structs = new ArrayList<>(structureInfo.size());
        structureInfo.forEach((key,value)->{
            List<Object> each = new ArrayList<>(3);
            each.add(value[0]);
            each.add(value[1]);
            each.add(value[2]);
            structs.add(each);
        });
        business(structs.get(0),structs.get(1),structs.get(2),structs.get(3),structs.get(4),structs.get(5));
    }

}
