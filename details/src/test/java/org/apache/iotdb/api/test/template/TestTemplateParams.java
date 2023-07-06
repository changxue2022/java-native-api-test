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
    private String database = "root.template";

    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(database);
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase(database);
        session.dropSchemaTemplate(tName);
    }
    // TIMECHODB-141
//    @Test(priority = 10, expectedExceptions = StatementExecutionException.class)
//    public void testCreateTemplate_null() throws IoTDBConnectionException, IOException, StatementExecutionException {
//        session.createSchemaTemplate(null);
//    }
    @Test(priority = 11)
    public void testCreateTemplate_empty() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String templateName = tName +"_0TS";
        Template template = new Template(templateName, isAligned);
        session.createSchemaTemplate(template);
        assert checkTemplateExists(templateName) : "创建0TS模版成功";
        session.dropSchemaTemplate(templateName);
    }
    // TIMECHODB-149
//    @Test(priority = 12, expectedExceptions = StatementExecutionException.class)
//    public void testSet_nullTemplate() throws IoTDBConnectionException, StatementExecutionException {
//        session.setSchemaTemplate(null, database);
//    }
    @Test(priority = 13, expectedExceptions = StatementExecutionException.class)
    public void testSet_noTemp() throws IoTDBConnectionException, StatementExecutionException {
        getTemplateCount(true);
        session.setSchemaTemplate(tName, database);
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
        session.setSchemaTemplate(tName, database+"_nonExist");
    }
    @Test(priority = 30, expectedExceptions = IoTDBConnectionException.class)
    public void testUnSet_nullPath() throws IoTDBConnectionException, StatementExecutionException {
        session.unsetSchemaTemplate(null, tName);
    }
    @Test(priority = 31, expectedExceptions = StatementExecutionException.class)
    public void testUnSet_emptyPath() throws IoTDBConnectionException, StatementExecutionException {
        session.unsetSchemaTemplate("", tName);
    }
    @Test(priority = 32, expectedExceptions = StatementExecutionException.class)
    public void testUnSet_noPath() throws IoTDBConnectionException, StatementExecutionException {
        // database 不存在
        session.unsetSchemaTemplate(database+"_nonExist", tName);
    }

    @Test(priority = 40, expectedExceptions = IoTDBConnectionException.class)
    public void testDropTemplate_null() throws IoTDBConnectionException, StatementExecutionException {
        session.dropSchemaTemplate(null);
    }
    @Test(priority = 41, expectedExceptions = StatementExecutionException.class)
    public void testDropTemplate_empty() throws IoTDBConnectionException, StatementExecutionException {
        session.dropSchemaTemplate("");
    }
    @Test(priority = 42, expectedExceptions = StatementExecutionException.class)
    public void testDropTemplate_invalid() throws IoTDBConnectionException, StatementExecutionException {
        session.dropSchemaTemplate("abc.**");
    }
    @Test(priority = 44, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_wildcardPath_noActive() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(tName, database+".**");
    }

    @Test(priority = 48)
    public void testNormal() throws IoTDBConnectionException, StatementExecutionException {
        session.setSchemaTemplate(tName, database);
        assert checkTemplateExists(tName): "模版创建成功";
        List<String> paths = new ArrayList<>(1);
        paths.add(database + ".d0");
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert getActivePathsCount(tName, verbose) > 0 : "激活成功";
    }

    @Test(priority = 51, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_nullTemplate() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(null, database);
    }
    @Test(priority = 52, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_nullPath() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(tName, (String) null);
    }
    @Test(priority = 53, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_emptyTemplate() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate("", database);
    }
    @Test(priority = 54, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_emptyPath() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(tName, "");
    }
    @Test(priority = 55, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_nonExistTemplate() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(tName+"030", database);
    }
    @Test(priority = 56, expectedExceptions = StatementExecutionException.class)
    public void testDeactive_nonExistPath() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(tName, "root.d");
    }
    @Test(priority = 57, expectedExceptions = StatementExecutionException.class)
    public void testUnset_activePath() throws IoTDBConnectionException, StatementExecutionException {
        session.unsetSchemaTemplate(database, tName);
    }
    @Test(priority = 58)
    public void testDeactive_wildcardPath() throws IoTDBConnectionException, StatementExecutionException {
        deactiveTemplate(tName, database+".**");
    }
    @Test(priority = 60, expectedExceptions = StatementExecutionException.class)
    public void testUnset_wildcardPath() throws IoTDBConnectionException, StatementExecutionException {
        session.unsetSchemaTemplate(database+".**", tName);
    }

}
