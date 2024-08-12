package org.apache.iotdb.api.test.dynamic;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestDynamicTemplateMulti extends BaseTestSuite {
    private Logger logger = Logger.getLogger(TestDynamicTemplateMulti.class);

    private List<IMeasurementSchema> schemaList = new ArrayList<>();
    private List<IMeasurementSchema> schemaList_clean = new ArrayList<>(1);

    private String databasePrefix = "root.db.factory";
    private String templatePrefix = "template0";
    private String tsPrefix = "sensors_";
    private String templateName_clean = templatePrefix + 2;

    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        session = PrepareConnection.getSession();
        cleanDatabases(verbose);
        cleanTemplates(verbose);
    }

    private void createTemplate(int databaseCount, int deviceCount, String templateName, String suffix) throws StatementExecutionException, IoTDBConnectionException, IOException {
        int templateNodeCount = 10;
        List<IMeasurementSchema> schemaList = new ArrayList<>(templateNodeCount);
        List<String> devicePaths = new ArrayList<>(deviceCount);
        int tsCount = getTimeSeriesCount("root.**", verbose);
        for (int i = 0; i < databaseCount; i++) {
            for (int j = 0; j < deviceCount; j++) {
                devicePaths.add(String.format("%s_%d.WT%05d.%sTemplate", databasePrefix, i, j, suffix));
            }
        }
        Template template = new Template(templateName, isAligned);
        switch (suffix) {
            case "int":
                for (int i = 0; i < templateNodeCount; i++) {
                    template.addToTemplate(new MeasurementNode("s_int" + i, TSDataType.INT32, TSEncoding.GORILLA, CompressionType.GZIP));
                    schemaList.add(new MeasurementSchema("s_int" + i, TSDataType.INT32, TSEncoding.GORILLA, CompressionType.GZIP));
                }
                break;
            case "long":
                for (int i = 0; i < templateNodeCount; i++) {
                    template.addToTemplate(new MeasurementNode("s_long" + i, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.GZIP));
                    schemaList.add(new MeasurementSchema("s_long" + i, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.GZIP));
                }
                break;
            case "float":
                for (int i = 0; i < templateNodeCount; i++) {
                    template.addToTemplate(new MeasurementNode("s_float" + i, TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.GZIP));
                    schemaList.add(new MeasurementSchema("s_float" + i, TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.GZIP));
                }
                break;
            case "double":
                for (int i = 0; i < templateNodeCount; i++) {
                    template.addToTemplate(new MeasurementNode("s_double" + i, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.GZIP));
                    schemaList.add(new MeasurementSchema("s_double" + i, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.GZIP));
                }
                break;
        }
        session.createSchemaTemplate(template);
        assert checkTemplateExists(templateName) : "创建template成功:" + templateName;
        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
            assert checkTemplateContainPath(templateName, devicePaths.get(i)) : "挂载成功:" + templateName + " - " + devicePaths.get(i);
        }
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        assert checkUsingTemplate(devicePaths.get(0), verbose) : "激活成功:使用了模版" + templateName;
        tsCount += deviceCount * 10;
        assert tsCount == getTimeSeriesCount("root.**", verbose) : "激活成功, ts:" + tsCount;
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletMulti(devicePaths.get(i), schemaList, 10, isAligned);
        }
    }

    @Test(priority = 30)
    public void testMultipleTemplates() throws StatementExecutionException, IoTDBConnectionException, IOException {
        int templateCount = 5;
        int databaseCount = 1;
        int deviceCount = 2;
        List<String> databases = new ArrayList<>(databaseCount);
        List<String> devicePaths = new ArrayList<>(databaseCount * deviceCount);
        List<String> templateNames = new ArrayList<>(templateCount);
        for (int i = 0; i < templateCount; i++) {
            templateNames.add(templatePrefix + "_" + i);
        }
        for (int i = 0; i < databaseCount; i++) {
            String database = databasePrefix + "_" + i;
            if (!checkStroageGroupExists(database)) {
                session.createDatabase(database);
            }
            databases.add(database);
            for (int j = 0; j < deviceCount; j++) {
                devicePaths.add(String.format("%s.WT%05d.info", database, j));
            }
        }
        // 为了测试清理
        String database = databasePrefix + 2;
        session.createDatabase(database);
        Template t = new Template(templateName_clean, isAligned);
        t.addToTemplate(new MeasurementNode("name", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.GZIP));
        schemaList_clean.add(new MeasurementSchema("name", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.GZIP));
        session.createSchemaTemplate(t);
        List<String> d = new ArrayList<>(1);
        d.add(database + ".device");
        session.setSchemaTemplate(templateName_clean, database);
        session.createTimeseriesUsingSchemaTemplate(d);
        assert 1 == getTimeSeriesCount(database + ".**", verbose) : "激活成功, ts:1";
        insertTabletMulti(d.get(0), schemaList_clean, 10, isAligned);

        int tsCount = getTimeSeriesCount(databases.get(0)+".**", verbose);
        // template1
        String templateName = templateNames.get(0);
        Template template = new Template(templateName, isAligned);
        template.addToTemplate(new MeasurementNode("name", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.GZIP));
        template.addToTemplate(new MeasurementNode("code", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.GZIP));
        template.addToTemplate(new MeasurementNode("isStart", TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.GZIP));
        template.addToTemplate(new MeasurementNode("latitude", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
        template.addToTemplate(new MeasurementNode("longitude", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
        schemaList.add(new MeasurementSchema("name", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.GZIP));
        schemaList.add(new MeasurementSchema("code", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.GZIP));
        schemaList.add(new MeasurementSchema("isStart", TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.GZIP));
        schemaList.add(new MeasurementSchema("latitude", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
        schemaList.add(new MeasurementSchema("longitude", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY));
        session.createSchemaTemplate(template);
        assert checkTemplateExists(templateName) : "创建template成功:" + templateName;
        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
            assert checkTemplateContainPath(templateName, devicePaths.get(i)) : "挂载成功:" + templateName + " - " + devicePaths.get(i);
        }
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        assert checkUsingTemplate(devicePaths.get(0), verbose) : "激活成功:使用了模版" + templateName;
        tsCount += deviceCount * schemaList.size();
        assert tsCount == getTimeSeriesCount(databases.get(0)+".**", verbose) : "激活成功, ts:" + tsCount;
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletMulti(devicePaths.get(i), schemaList, 10, isAligned);
        }

        // 清理
//        session.deleteDatabase(database);
        int setCount = getSetPathsCount(templateName, verbose);
        int activeCount = getActivePathsCount(templateName, verbose);
        int templateCount_t = getTemplateCount(verbose);
        deactiveTemplate(this.templateName_clean, d.get(0));
        assert 0 == getActivePathsCount(this.templateName_clean, verbose);
        assert 0 == getTimeSeriesCount( d.get(0) + ".**", verbose) : "解除模版会删除序列";
        session.unsetSchemaTemplate(database, this.templateName_clean);
        assert 0 == getSetPathsCount(this.templateName_clean, verbose) : "模版引用数目 == 0";
        assert 0 == getTimeSeriesCount(database+".**", verbose) : "删除成功";
        session.dropSchemaTemplate(this.templateName_clean);
        session.deleteDatabase(database);
        assert setCount == getSetPathsCount(templateName, verbose) : "对其他模版无影响：挂载数目";
        assert activeCount == getActivePathsCount(templateName, verbose) : "对其他模版无影响：激活数目";
        assert templateCount_t-1 == getTemplateCount(verbose) : "template数目减一";

        //template2
        createTemplate(databaseCount, deviceCount, templateNames.get(1), "int");
        //template3
        createTemplate(databaseCount, deviceCount, templateNames.get(2), "long");
        //template4
        createTemplate(databaseCount, deviceCount, templateNames.get(3), "float");
        //template5
        createTemplate(databaseCount, deviceCount, templateNames.get(4), "double");

        // query
        for (int i = 0; i < databaseCount; i++) {
            queryLastData(String.format("%s_%d.*.info.*", databasePrefix, i), null, verbose);
            queryLastData(String.format("%s_%d.*.intTemplate.*", databasePrefix, i), null, verbose);
            queryLastData(String.format("%s_%d.*.longTemplate.*", databasePrefix, i), null, verbose);
            queryLastData(String.format("%s_%d.*.floatTemplate.*", databasePrefix, i), null, verbose);
            queryLastData(String.format("%s_%d.*.doubleTemplate.*", databasePrefix, i), null, verbose);
        }

        //清理
        for (int i = 0; i < databases.size(); i++) {
            session.deleteDatabase(databases.get(i));
        }
        for (int i = 0; i < templateCount; i++) {
            session.dropSchemaTemplate(templateNames.get(i));
        }
    }
}
