package org.apache.iotdb.api.test.template;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestTemplateParams extends BaseTestSuite {
    private String tName = "t1";
    private String databasePrefix = "root.template";

    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
    }

//    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
    }
    // TIMECHODB-141
    @Test(priority = 10, expectedExceptions = StatementExecutionException.class)
    public void testCreateTemplate_null() throws IoTDBConnectionException, IOException, StatementExecutionException {
        session.createSchemaTemplate(null);
    }
    @Test(priority = 11)
    public void testCreateTemplate_empty() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String templateName = tName +"_0TS";
        Template template = new Template(templateName, isAligned);
        session.createSchemaTemplate(template);
        assert checkTemplateExists(templateName) : "创建0TS模版成功";
        session.dropSchemaTemplate(templateName);
    }
    @Test(priority = 12, expectedExceptions = IoTDBConnectionException.class)
    public void testSet_nullTemplate() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(null, databasePrefix);
    }
    @Test(priority = 13, expectedExceptions = StatementExecutionException.class)
    public void testSet_noTemp() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, databasePrefix);
    }

    @Test(priority = 14, expectedExceptions = IoTDBConnectionException.class)
    public void testSet_noTempNullPath() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, null);
    }
    @Test(priority = 15, expectedExceptions = StatementExecutionException.class)
    public void testSet_noTempEmptyPath() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, "");
    }
    @Test(priority = 16, expectedExceptions = StatementExecutionException.class)
    public void testSet_noTempRootPath() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, "root");
        countLines("show paths set schema template "+tName, verbose);
    }
    @Test(priority = 17)
    public void createTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        int templateCount = getTemplateCount(false);
        Template template = new Template(tName, isAligned);

        template.addToTemplate(new MeasurementNode("s_key", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.GZIP));

        session.createSchemaTemplate(template);
        assert templateCount + 1 == getTemplateCount(verbose) : "创建单节点模版成功";
    }
    @Test(priority = 18, expectedExceptions = IoTDBConnectionException.class)
    public void testSet_nullPath() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, null);
    }
    @Test(priority = 19, expectedExceptions = StatementExecutionException.class)
    public void testSet_emptyPath() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, "");
    }
    @Test(priority = 20, expectedExceptions = StatementExecutionException.class)
    public void testSet_noPath() throws IoTDBConnectionException, StatementExecutionException {
        // database 不存在
        session.setSchemaTemplate(tName, databasePrefix);
    }

}
