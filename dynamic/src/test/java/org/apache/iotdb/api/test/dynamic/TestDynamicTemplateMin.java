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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单模版，多database
 */
public class TestDynamicTemplateMin extends BaseTestSuite {
    private List<List<Object>> structures;
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    private List<MeasurementSchema> schemaList_org = new ArrayList<>(6);
    private List<MeasurementSchema> schemaList_err = new ArrayList<>(6);
    private List<MeasurementSchema> schemaList_more = new ArrayList<>();
    private List<MeasurementSchema> schemaList_less = new ArrayList<>();
    private List<String> databases = new ArrayList<>(3);
    private List<String> devicePaths = new ArrayList<>();
    private String databasePrefix = "root.db.factory";
    private String templateName = "template01";

    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        cleanup();
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
    private void cleanup() throws IoTDBConnectionException, StatementExecutionException {
        for (int i = 0; i < databases.size(); i++) {
            if (checkStroageGroupExists(databasePrefix)) {
                session.deleteDatabase(databasePrefix);
            }
        }
        if (checkTemplateExists(templateName)) {
            session.dropSchemaTemplate(templateName);
        }
    }
    @AfterMethod
    public void afterMethod() throws IoTDBConnectionException, StatementExecutionException {
        cleanup();
    }

    private void business(List<Object> ... structs) throws IoTDBConnectionException, IOException, StatementExecutionException {
        for (int i = 0; i < schemaList_org.size(); i++) {
            schemaList_more.add(schemaList_org.get(i));
        }
        schemaList_more.add(new MeasurementSchema("s_append"+0,
                (TSDataType) structs[0].get(0), (TSEncoding) structs[0].get(1),
                (CompressionType) structs[0].get(2)));
        // 创建template
        int templateCount = getTemplateCount(verbose);

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
        assert (templateCount+1) == getTemplateCount(verbose) : "创建模版成功";
        assert 0 == getSetPathsCount(templateName, verbose) : "未挂载，未激活";

        if (auto_create_schema) { // 打开自动创建
            device = database + ".d0.group0";
            devicePaths.add(device);
            // 写入数据 root.db.factory1.d0.group0, 成功，未使用模版
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            assert false == checkUsingTemplate(device, verbose) : "没有使用模版:"+device;
            assert schemaList_org.size() == getTimeSeriesCount(device + ".**", verbose) : "TS数量："+schemaList_org.size();
        } else { // 关闭自动创建
            for (int i = 0; i < 2; i++) {
                if (!checkStroageGroupExists(databases.get(i))) {
                    session.createDatabase(databases.get(i));
                }
                assert checkStroageGroupExists(databases.get(i)) : "创建database成功:"+databases.get(i);
            }
            // 写入数据 root.db.factory1.d0.group0
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletMulti(database + ".d0.group0", schemaList_org, 10, isAligned);
            });
        }
        expectTSCountEach = schemaList_org.size();
        if (auto_create_schema){
            getTimeSeriesCount(devicePaths.get(0) + ".**", verbose);
            Assert.assertThrows(StatementExecutionException.class, ()-> {
                session.setSchemaTemplate(templateName, devicePaths.get(0));
            });
            Assert.assertThrows(StatementExecutionException.class, ()-> {
                session.createTimeseriesUsingSchemaTemplate(devicePaths);
            });
            // 写入成功，使用了模版
            device = database+".d1.group1";
            devicePaths.add(device);
            session.setSchemaTemplate(templateName, device);
            assert checkTemplateContainPath(templateName, device) : "挂载模版成功:"+device;
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            assert checkUsingTemplate(device, verbose) : "使用了模版:"+device;
            assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:"+expectTSCountEach;
            assert getTimeSeriesCount(device+".*", verbose) == expectTSCountEach : "TS数量:"+expectTSCountEach;
            assert schemaList_org.size() == getTimeSeriesCount(device+".*", verbose) : "确定模版内TS数量:"+schemaList_org.size();
            Assert.assertThrows(StatementExecutionException.class, ()->{
                insertTabletMulti(database+".d1.group1", schemaList_org, 10, !isAligned);
            });
            // 写入少于模版的列的数据
            device = database+".d1.group1.subGroup";
            devicePaths.add(device);
            insertTabletMulti(device, schemaList_less, 10, isAligned);
            assert checkUsingTemplate(device, verbose) : "使用了模版:"+device;
            assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:"+expectTSCountEach;
            assert getTimeSeriesCount(device+".*", verbose) == expectTSCountEach : "TS数量:"+expectTSCountEach;
            assert schemaList_org.size() == getTimeSeriesCount(device+".*", verbose) : "确定模版内TS数量:"+schemaList_org.size();

            // 写入多于模版列的数据
            expectTSCountEach++;
            device = database+".d2.group1";
            session.setSchemaTemplate(templateName, device);
            assert checkTemplateContainPath(templateName, device) : "挂载模版成功:"+device;
            insertTabletMulti(device, schemaList_more, 10, isAligned);
            assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:"+expectTSCountEach;

            assert getTimeSeriesCount(device+".*", verbose) == expectTSCountEach : "TS数量:"+expectTSCountEach;
            assert checkUsingTemplate(device, verbose) : "使用了模版:"+device;
            assert schemaList_more.size() == getTimeSeriesCount(device+".*", verbose) : "确定模版内TS数量:"+schemaList_more.size();
            devicePaths.add(device);
            // 写入同名不同类型列的数据
            Assert.assertThrows(StatementExecutionException.class, ()->{
                insertTabletMulti(database+".d2.group1", schemaList_err, 10, isAligned);
            });
        } else {
            // 不挂载/激活，直接写入
            Assert.assertThrows(StatementExecutionException.class, () -> {
                insertTabletMulti(database + ".d1.group1", schemaList_org, 10, isAligned);
            });
        }
        // 激活
        List<String> paths = new ArrayList<>(2);
        paths.add(databasePrefix+"1" + ".d1.group3");
        for (int i = 0; i < 2; i++) {
            paths.add(databasePrefix+i + ".d1.group3");
            session.setSchemaTemplate(templateName, paths.get(i));
        }
        int tsCount = getTimeSeriesCount(paths.get(0)+".**", verbose);
        tsCount += 2 * expectTSCountEach;

        // 激活
        session.createTimeseriesUsingSchemaTemplate(paths);
        device = paths.get(0);
        devicePaths.add(device);
        assert checkUsingTemplate(device, verbose) : "激活后，使用了模版:"+device;
        assert expectTSCountEach == getTSCountInTemplate(templateName, verbose) : "Template内TS数量:"+expectTSCountEach;
        assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
        // 写入数据
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        // 写入少列数据
        insertTabletMulti(device, schemaList_less, 10, isAligned);

        device = paths.get(1);
        devicePaths.add(device);
        assert checkUsingTemplate(device, verbose) : "激活后，使用了模版:"+device;
        assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
        // 写入少列数据
        insertTabletMulti(device, schemaList_less, 10, isAligned);
        // 写入数据
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        // 写入多列不同类型数据
        Assert.assertThrows(StatementExecutionException.class, ()->{
            insertTabletMulti(paths.get(0), schemaList_err, 10, isAligned);
        });
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            insertTabletMulti(paths.get(0), schemaList_more, 10, isAligned);
//        });
        for (int i = 0; i < paths.size(); i++) {
            assert expectTSCountEach == getTimeSeriesCount(paths.get(i)+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
            assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:"+expectTSCountEach;
        }
        paths.clear();
        paths.add(database + ".d1.group2");
        paths.add(database + ".d3.group3");
        for (int i = 0; i < paths.size(); i++) {
            session.setSchemaTemplate(templateName, paths.get(i));
            devicePaths.add(paths.get(i));
        }

        // explicit追加TS
        if (structs.length == 1) {
            expectTSCountEach++;
            addTSIntoTemplate(templateName, "s_append_explicit",
                    (TSDataType) structs[0].get(0), (TSEncoding) structs[0].get(1),
                    (CompressionType) structs[0].get(2));
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

        if (auto_create_schema) {
            insertTabletMulti(device, schemaList_more, 10, isAligned);
            assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            insertTabletMulti(device, schemaList_less, 10, isAligned);
            for (int i = 0; i < 2; i++) {
                device = paths.get(i);
//                assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
                insertTabletMulti(device, schemaList_more, 10, isAligned);
                insertTabletMulti(device, schemaList_org, 10, isAligned);
                insertTabletMulti(device, schemaList_less, 10, isAligned);
                assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "插入后，确定模版内TS数量:"+expectTSCountEach;
                assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "Template内TS数量:"+expectTSCountEach;
            }
        } else {
            schemaList_more.remove(schemaList_org.size()-1);
            // 未激活
            Assert.assertThrows(StatementExecutionException.class, ()->{
                insertTabletMulti(paths.get(0), schemaList_less, 10, isAligned);
            });
            Assert.assertThrows(StatementExecutionException.class, ()->{
                insertTabletMulti(paths.get(1), schemaList_less, 10, isAligned);
            });
            session.createTimeseriesUsingSchemaTemplate(paths);
            Assert.assertThrows(StatementExecutionException.class, ()->{
                System.out.println("device="+paths.get(1));
                insertTabletMulti(paths.get(1), schemaList_more, 10, isAligned);
            });
            assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            insertTabletMulti(device, schemaList_less, 10, isAligned);
       }
        assert getTSCountInTemplate(templateName, verbose) == expectTSCountEach : "对已经激活模版序列影响:Template内TS数量:"+expectTSCountEach;
        for (int i = 1; i < devicePaths.size(); i++) {
            System.out.println(devicePaths.get(i));
            insertTabletMulti(devicePaths.get(i), schemaList_org, 20, isAligned);
            assert expectTSCountEach == getTimeSeriesCount(devicePaths.get(i)+".*", verbose) : "对已经激活模版序列影响：确定模版内TS数量:"+expectTSCountEach;
        }
        // 删除部分数据 TIMECHODB-104
//        for (int i = 0; i < devicePaths.size(); i++) {
//            System.out.println(devicePaths.get(i));
//            session.deleteData(devicePaths.get(i)+".*", 3);
//        }
        if (auto_create_schema) {
            device = database + ".d0.group0";
            tsCount = getTimeSeriesCount("root.**", verbose);
            countLines("show devices ", verbose);
            session.executeNonQueryStatement("delete timeseries " + device + ".**");
            assert getTimeSeriesCount(device + ".**", verbose) == 0 : "删除成功";
            assert tsCount - 6 == getTimeSeriesCount("root.**", verbose) : "删除成功,其他不影响";
            insertTabletMulti(devicePaths.get(1), schemaList_org, 20, isAligned);
        }
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
        // 解除某节点
        device = devicePaths.get(1);
        int count = getActivePathsCount(templateName, verbose);
        count--;
        deactiveTemplate(templateName, device);
        assert 0 == getTimeSeriesCount(device+ ".*", verbose) : "解除模版会删除序列";
        assert count == getActivePathsCount(templateName, verbose) : "模版引用数目-1";

        // 解除所有节点
        devicePaths.remove(0);
        devicePaths.remove(1);
        // TIMECHODB-103
//        deactiveTemplate(templateName, devicePaths);
        // TIMECHODB-102
//        deactiveTemplate(templateName, "root.db.**");
//        assert 0 == getSetPathsCount(templateName, verbose) : "模版引用数目 == 0";
//        session.unsetSchemaTemplate(database, templateName);
//        session.deleteDatabase(database);
//        session.createDatabase(database);
//        session.dropSchemaTemplate(templateName);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            session.unsetSchemaTemplate(database, templateName);
//        });
//        countLines("show databases;", verbose);
        for (int i = 0; i <databases.size(); i++) {
            session.deleteDatabase(databases.get(i));
        }
        session.dropSchemaTemplate(templateName);
        getTemplateCount(verbose);
        getTimeSeriesCount("", verbose);

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
