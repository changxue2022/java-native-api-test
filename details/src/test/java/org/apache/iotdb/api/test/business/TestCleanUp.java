package org.apache.iotdb.api.test.business;

import org.apache.iotdb.api.test.BaseTestSuite;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestCleanUp extends BaseTestSuite {
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(6);
    private String templateName = "cleanUpTemplate";
    private String database = "root.cleanUp";
    private String device = database + ".deviceNo";
    private int insertCount = 10000;
    private List<String> measurements = new ArrayList<>(6);
    private List<IMeasurementSchema> schemaList = new ArrayList<>(6);// tablet
    private List<TSDataType> dataTypes = new ArrayList<>(6);
    private List<String> paths = new ArrayList<>(6);
    private List<TSEncoding> encodings = new ArrayList<>(6);
    private List<CompressionType> compressionTypes = new ArrayList<>(6);

    @BeforeClass
    public void BeforeClass() throws IoTDBConnectionException, StatementExecutionException {
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int", TSDataType.INT32);
        measureTSTypeInfos.put("s_long", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);

        measureTSTypeInfos.forEach((key,value) -> {
            paths.add(device + "." + key);
            measurements.add(key);
            dataTypes.add(value);
            schemaList.add(new MeasurementSchema(key, value));
        });

        for (int i = 0; i < dataTypes.size(); i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.UNCOMPRESSED);
        }
    }
    @BeforeMethod
    public void beforeMethod() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(database);
    }

    @Test(priority = 10)
    public void createTemplate() throws IoTDBConnectionException, IOException, StatementExecutionException {
        int count = getTemplateCount(verbose);
        Template template = new Template(templateName, true);
        measureTSTypeInfos.forEach((key,value) -> {
            try {
                template.addToTemplate(new MeasurementNode(key, value,  TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        // 创建模版
        session.createSchemaTemplate(template);
        assert count+1 == getTemplateCount(verbose) : "创建模版成功";
        // 挂载
        assert 0 == getSetPathsCount(templateName,  verbose): "挂载前";
        session.setSchemaTemplate(templateName, device);
        assert checkTemplateContainPath(templateName, device) : "挂载模版成功";
        assert 1 == getSetPathsCount(templateName,  verbose): "挂载模版成功count";
        // 激活
        assert 0 == getActivePathsCount(templateName, verbose) : "激活前";
        List<String> paths = new ArrayList<>(1);
        paths.add(device);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert schemaList.size() == getTimeSeriesCount(device+".**", verbose) : "激活后查询TS:"+schemaList.size();
        assert 1 == getActivePathsCount(templateName, verbose) : "激活成功";
        // 写入数据
        insertTabletMulti(device, schemaList, insertCount, true);
        // 解除
        deactiveTemplate(templateName, device);
        assert 0 == getTimeSeriesCount(device+".**", verbose) : "解除后查询TS:0";
        // 卸载
        session.unsetSchemaTemplate(device, templateName);
        assert 0 == getSetPathsCount(templateName,  verbose): "卸载后:0";
        // 删除模版
        session.dropSchemaTemplate(templateName);
        // 删除数据库
        session.deleteDatabase(database);
    }
    @Test(priority = 20)
    public void createTemplateNonAligned() throws IoTDBConnectionException, IOException, StatementExecutionException {
        int count = getTemplateCount(verbose);
        Template template = new Template(templateName, false);
        measureTSTypeInfos.forEach((key,value) -> {
            try {
                template.addToTemplate(new MeasurementNode(key, value,  TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        // 创建模版
        session.createSchemaTemplate(template);
        assert count+1 == getTemplateCount(verbose) : "创建模版成功";
        // 挂载
        assert 0 == getSetPathsCount(templateName,  verbose): "挂载前";
        session.setSchemaTemplate(templateName, device);
        assert checkTemplateContainPath(templateName, device) : "挂载模版成功";
        assert 1 == getSetPathsCount(templateName,  verbose): "挂载模版成功count";
        // 激活
        assert 0 == getActivePathsCount(templateName, verbose) : "激活前";
        List<String> paths = new ArrayList<>(1);
        paths.add(device);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert schemaList.size() == getTimeSeriesCount(device+".**", verbose) : "激活后查询TS:"+schemaList.size();
        assert 1 == getActivePathsCount(templateName, verbose) : "激活成功";
        // 写入数据
        insertTabletMulti(device, schemaList, insertCount, false);
        // 解除
        deactiveTemplate(templateName, device);
        assert 0 == getTimeSeriesCount(device+".**", verbose) : "解除后查询TS:0";
        // 卸载
        session.unsetSchemaTemplate(device, templateName);
        assert 0 == getSetPathsCount(templateName,  verbose): "卸载后:0";
        // 删除模版
        session.dropSchemaTemplate(templateName);
        // 删除数据库
        session.deleteDatabase(database);
    }

    @Test(priority = 30)
    public void createAlignedTS() throws IoTDBConnectionException, StatementExecutionException, IOException {
        session.createAlignedTimeseries(device, measurements, dataTypes, encodings, compressionTypes, null);
        assert  schemaList.size() == getTimeSeriesCount(device+".*", false) : "创建TS数目";
        insertTabletMulti(device, schemaList, insertCount, true);
        session.deleteTimeseries(device+".**");
        session.deleteDatabase(database);
    }
    @Test(priority = 40)
    public void createNonAlignedTS() throws IoTDBConnectionException, StatementExecutionException, IOException {
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes, null, null, null, null);
        assert  schemaList.size() == getTimeSeriesCount(device+".*", false) : "创建TS数目";
        insertTabletMulti(device, schemaList, insertCount, false);
        session.deleteTimeseries(device+".**");
        session.deleteDatabase(database);
    }
}
