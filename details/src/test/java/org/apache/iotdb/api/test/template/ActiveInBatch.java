package org.apache.iotdb.api.test.template;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.ReadConfig;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

public class ActiveInBatch extends BaseTestSuite {
    private List<String> paths = new ArrayList<>();
    private List<List<Object>> structures;
    private List<List<Object>> errStructures;
    private static final String databasePrefix = "root.template";
    private static final String templateNamePrefix = "tempGroup";
    private String[] databases = new String[]{"root.templateSingle1","root.templateSingle2"};
    private String[] loadNodePaths = new String[]{"root.templateSingle1.single", "root.templateSingle2.sub"};
    private static final String tName = "tempGroupSingle";
    private static final String tsName = "s0";
    private static int maxDatabaseLength;

    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        maxDatabaseLength = Integer.parseInt(ReadConfig.getInstance().getValue("max_database_length"));
        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");
        errStructures = new CustomDataProvider().parseTSStructure("data/ts-structures-error.csv");
    }

    @DataProvider(name="getErrorNames")
    public Iterator<Object[]> getErrorNames() throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }
    @DataProvider(name="getNormalNames")
    public Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }

    //TIMECHODB-82
    @Test(priority = 10)
    public void testNullError() {
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(null);
        });
        paths.add(null);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        paths.add(databases[0]);
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
    }

    @Test(priority = 20, expectedExceptions=StatementExecutionException.class)
    public void testEmptyError() throws IoTDBConnectionException, StatementExecutionException {
        session.createTimeseriesUsingSchemaTemplate(new ArrayList<>());
    }

    // TIMECHODB-76
    @Test(priority = 30, expectedExceptions = StatementExecutionException.class)
    public void testSingleError_databaseNotExist() throws IoTDBConnectionException, StatementExecutionException {
        paths.clear();
        paths.add(loadNodePaths[0]);
        session.createTimeseriesUsingSchemaTemplate(paths);
    }
    @Test(priority = 40, expectedExceptions = StatementExecutionException.class)
    public void testSingleError_normalPath() throws IoTDBConnectionException, StatementExecutionException {
        if (!checkStroageGroupExists(databases[0])) {
            session.setStorageGroup(databases[0]);
        }
        paths.clear();
        paths.add(loadNodePaths[0]);
        session.createTimeseriesUsingSchemaTemplate(paths);
    }

    @Test(priority = 50)
    public void createTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        if (!checkTemplateExists(tName)) {
            int templateCount = getTemplateCount(verbose);
            Template template = new Template(tName, isAligned);
            MeasurementNode mNode = new MeasurementNode(tsName, TSDataType.INT32,
                    TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
            template.addToTemplate(mNode);
            session.createSchemaTemplate(template);
            assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功:" + tName;
        }
        for(String database:databases) {
            if (!checkStroageGroupExists(database)) {
                session.createDatabase(database);
            }
            session.setSchemaTemplate(tName, database);
            assert checkTemplateContainPath(tName, database) : "挂载模版成功:"+tName+" - "+database;
        }
    }
    @Test(priority = 51)
    public void testWildcard() {
        paths.clear();
        paths.add(databases[0]+".d.*");
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        paths.clear();
        paths.add(databases[0]+".**");
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
    }

    @Test(priority = 60,expectedExceptions = IoTDBConnectionException.class)
    public void testNullAfterTemplate() throws IoTDBConnectionException, StatementExecutionException {
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(null);
        });
        paths.add(null);
        Assert.assertThrows(IoTDBConnectionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        paths.add(databases[0]);
        Assert.assertThrows(StatementExecutionException.class, ()->{
        session.createTimeseriesUsingSchemaTemplate(paths);
    });
    }
    @Test(priority = 70, expectedExceptions=StatementExecutionException.class)
    public void testEmptyAfterTemplate() throws IoTDBConnectionException, StatementExecutionException {
        session.createTimeseriesUsingSchemaTemplate(new ArrayList<>());
    }

    @Test(priority = 80)
    public void testActiveDatabaseAfterTemplate() throws IoTDBConnectionException, StatementExecutionException {
        String loadNode = databases[0];
        int expectCount = getCount("count devices "+loadNode, verbose) + 1;
        paths.clear();
        paths.add(loadNode);
        assert true == checkStroageGroupExists(loadNode): "database已经存在:"+loadNode;
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices "+loadNode, verbose) : "激活和挂载都是database:"+loadNode;
        insertRecordSingle(loadNode+"."+tsName, TSDataType.INT32, isAligned,null);
        expectCount = getCount("count devices "+loadNode+".**", verbose) + 1;
        paths.clear();
        paths.add(loadNode+".sub");
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices "+loadNode+".**", verbose) : "激活database子path:"+loadNode+".sub";
        insertRecordSingle(loadNode+".sub."+tsName, TSDataType.INT32, isAligned,null);
    }
    @Test(priority = 80)
    public void testActiveParentChild() throws IoTDBConnectionException, StatementExecutionException {
        String database = databases[1];
        String parentPath = database+".parent";
        String childPath = parentPath + ".child";
        int expectCount = getCount("count devices "+database+".**", verbose) + 2;
        if(!checkStroageGroupExists(database)){
            session.createDatabase(database);
        }
        paths.clear();
        paths.add(parentPath);
        paths.add(childPath);
        assert true == checkStroageGroupExists(database);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices "+database+".**", verbose) : "同时在parent和child路径进行激活";
        insertRecordSingle(parentPath+"."+tsName, TSDataType.INT32, isAligned,null);
        insertRecordSingle(childPath+"."+tsName, TSDataType.INT32, isAligned,null);
    }

    @Test(priority = 90)
    public void testDuplicatePath() throws IoTDBConnectionException, StatementExecutionException {
        String doublePath1 = databases[1]+".dDouble";
        String doublePath2 = databases[1]+".dDouble2";
        int expectCount = getCount("count devices "+databases[1]+".**", verbose) + 1;
        paths.clear();
        paths.add(doublePath1);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices "+databases[1]+".**", verbose) : "激活："+databases[1]+"0.dDouble";
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        assert expectCount == getCount("count devices "+databases[1]+".**", verbose) : "再次激活："+doublePath2;
        insertRecordSingle(doublePath1+"."+tsName, TSDataType.INT32, isAligned,null);
        paths.clear();
        paths.add(doublePath2);
        paths.add(doublePath2);
        session.createTimeseriesUsingSchemaTemplate(paths);
        expectCount++;
        assert expectCount == getCount("count devices "+databases[1]+".**", verbose) : "paths中有重复路径，激活："+doublePath2;
        insertRecordSingle(doublePath2+"."+tsName, TSDataType.INT32, isAligned,null);
    }
    @Test(priority = 100)
    public void testContain2loadedPath() throws IoTDBConnectionException, StatementExecutionException {
        paths.clear();
        for (int i = 0; i < loadNodePaths.length; i++) {
            paths.add(loadNodePaths[i] + ".db");
        }
        session.createTimeseriesUsingSchemaTemplate(paths);
        for (int i = 0; i < loadNodePaths.length; i++) {
            checkTemplateContainPath(tName, paths.get(i));
        }
    }
    @Test(priority = 101)
    public void testContainErrorPath() throws IoTDBConnectionException, StatementExecutionException {
        String database = databasePrefix+"_ab";
        int expectCount = getCount("count devices root.**", verbose);
        if(!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        paths.clear();
        paths.add(databases[0]+".thisIsNormal");
        paths.add(database+".notLoad");
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        assert expectCount == getCount("count devices root.**", verbose) : "含有未挂载路径";
        paths.clear();
        paths.add(null);
        paths.add(databases[0]+".thisIsNormal");
        // TIMECHODB-75
        Assert.assertThrows(StatementExecutionException.class, ()->{
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        assert expectCount == getCount("count devices root.**", verbose) : "含有null";
    }

//    TIMECHODB-79
    @Test(priority = 110, dataProvider = "getErrorNames", expectedExceptions = StatementExecutionException.class)
    public void testErrorPath(String name, String comment, String index) throws IoTDBConnectionException, StatementExecutionException {
        paths.clear();
//        paths.add(databases[1]+".dNormal");
        paths.add(databases[1]+"."+name);
        session.createTimeseriesUsingSchemaTemplate(paths);
    }
    @Test(priority = 111)
    public void testValidPath() throws IOException, IoTDBConnectionException, StatementExecutionException {
        paths.clear();
        List<String> names = new CustomDataProvider().getFirstColumns("data/names-normal.csv");
        for (int i = 0; i < names.size(); i++) {
            paths.add(databases[1]+"."+names.get(i));
        }
        int expectCount = getCount("count devices "+databases[1]+".**", verbose) + paths.size();
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices "+databases[1]+".**", verbose): "激活 validPath";
        for (int i = 0; i < paths.size() ; i++) {
            insertRecordSingle(paths.get(i)+"."+tsName, TSDataType.INT32, isAligned,null);
        }
    }

    @Test(priority = 120, dataProvider = "getNormalNames")
    public void testNormalOne(String name, String comment, String index) throws StatementExecutionException, IoTDBConnectionException, IOException {
        String database = databasePrefix+index;
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        paths.clear();
        int templateCount = getTemplateCount(verbose);
        Template template = new Template(name, isAligned);
        List<Object> struct = Tools.getRandom(structures);
        TSDataType tsDataType = (TSDataType) struct.get(0);
        System.out.println(struct);
        MeasurementNode mNode = new MeasurementNode(tsName, tsDataType,
                (TSEncoding) struct.get(1), (CompressionType) struct.get(2));
        template.addToTemplate(mNode);
        session.createSchemaTemplate(template);
        assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功";
        session.setSchemaTemplate(name, database);
        assert checkTemplateContainPath(name, database) : "挂载模版成功";
        int deviceCount = getCount("count devices "+database+".**", verbose);
        paths.add(database+"."+name);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert (paths.size() + deviceCount) == getTimeSeriesCount(database + ".**", verbose) :"批量激活模版timeseries:"+paths.size();
        assert (paths.size() + deviceCount) == getCount("count devices "+database+".**", verbose) : "批量激活模版devices："+paths.size();
        insertRecordSingle(database+"."+name+"."+name, tsDataType, isAligned, null);
        session.deleteStorageGroup(database);
        session.dropSchemaTemplate(name);
        session.createDatabase(database);
    }
//    @Test(priority = 130)
    public void createTemplateBig() throws IoTDBConnectionException, StatementExecutionException, IOException {
        String templateName = templateNamePrefix;
        String database = databasePrefix;
        List<MeasurementSchema> schemaList = new ArrayList<>(structures.size());
        int templateCount = getTemplateCount(verbose);
        Template template = new Template(templateName, isAligned);
        if(!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        for (int i = 0; i < structures.size() ; i++) {
            MeasurementNode mNode = new MeasurementNode("s_"+i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2));
            template.addToTemplate(mNode);
            schemaList.add(new MeasurementSchema("s_"+i,  (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        session.createSchemaTemplate(template);
        assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功:"+templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:"+templateName+" - "+database;
        int i = 0;
        StringJoiner sj = new StringJoiner(".");
        sj.add(database);
        while (true) {
            sj.add("level_"+i);
            i++;
            paths.add(sj.toString());
            if (i > 547) {
                int expectCount = getCount("count devices " + database + ".**", verbose) + paths.size();
                session.createTimeseriesUsingSchemaTemplate(paths);
                assert expectCount == getCount("count devices " + database + ".**", verbose) : "激活" + i;
                for (int j = 0; j < paths.size(); j++) {
                    insertTabletMulti(paths.get(j), schemaList, 10, isAligned);
                }
                session.deleteDatabase(database);
                session.createDatabase(database);
            }
        }
    }

}
