package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;

public class MixCondition {
    private static List<MeasurementSchema> schemaList;
    private static List<String> hostList = new ArrayList<>(3);
    private static int clientCount = 5;
    private static int insertCount = 10000;
    private static int appendCount = 10000;
    private static SessionPool session;
    private static List<List<Object>> structures;
    private static String templateName = "ConcurrentUpdate";
    private static String database = "root.db.factory_30";
    private static boolean isAligned = true;
    private static boolean verbose = true;


    static {
        hostList.add("127.0.0.1:6667");
//        hostList.add("iotdb-44:6667");
//        hostList.add("iotdb-45:6667");
//        hostList.add("iotdb-46:6667");
    }
    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        out.println("################");
        out.println(hostList);
        session = new SessionPool.Builder()
                .nodeUrls(hostList)
                .user("root")
                .password("root")
                .maxSize(clientCount*1000)
                .build();
        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");

        cleanDatabases(verbose);
        cleanTemplates(verbose);

        session.createDatabase(database);
        List<String> paths = new ArrayList<>(10);
        for (int i = 0; i < 2; i++) {
            paths.add(database + ".device_" + i + ".group");
        }
        out.println(paths);
        schemaList = createTemplate(session, templateName, 30, isAligned, paths);
        insertData(paths);
        for (int i = 0; i < paths.size(); i++) {
            query(paths.get(i));
        }
        testConcurrent_sameNameDiffType();
//        testConcurrent_diffNameDiffType();
        insertData(paths);
        for (int i = 0; i < paths.size(); i++) {
            query(paths.get(i));
        }
        session.close();

    }

    private static boolean checkTemplateExists(String templateName) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSetWrapper dataSet = session.executeQueryStatement("show schema templates ");
        SessionDataSet.DataIterator records = dataSet.iterator();
        while (dataSet.hasNext()) {
            records.next();
            if (templateName.equals(records.getString(1))) {
                return true;
            }
        }
        return false;
    }
    public static void cleanDatabases(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSetWrapper records = session.executeQueryStatement("show databases");
        List<String> databases = new ArrayList<>();
        while (records.hasNext()) {
            databases.add(String.valueOf(records.next().getFields().get(0)));
        }
        if (!databases.isEmpty()) {
            session.deleteDatabases(databases);
        }
        if (verbose) {
            out.println(databases.toString());
            out.println("drop databases: "+databases.size());
        }
    }
    public static void cleanTemplates(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSetWrapper records = session.executeQueryStatement("show schema templates");
        int count = 0;
        while (records.hasNext()) {
            count++;
            session.dropSchemaTemplate("`"+String.valueOf(records.next().getFields().get(0))+"`");
        }
        if (verbose) {
            out.println("drop templates:" + count);
        }
    }
    public static List<MeasurementSchema> createTemplate(SessionPool session, String templateName,
                                                         int tsCount, boolean isAligned, List<String> paths) throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<MeasurementSchema> schemaList = new ArrayList<>(tsCount);
        Template template = new Template(templateName, isAligned);
        for (int i = 0; i < tsCount; i++) {
            List<Object> struct = structures.get(i);
            template.addToTemplate(new MeasurementNode("s_" + i, (TSDataType) struct.get(0), (TSEncoding) struct.get(1), (CompressionType) struct.get(2)));
            schemaList.add(new MeasurementSchema("s_" + i, (TSDataType) struct.get(0), (TSEncoding) struct.get(1), (CompressionType) struct.get(2)));
        }
        if (checkTemplateExists(templateName)) {
            return schemaList;
        }
        session.createSchemaTemplate(template);
        session.setSchemaTemplate(templateName, database);
        out.println(paths);
        session.createTimeseriesUsingSchemaTemplate(paths);
        SessionDataSetWrapper dataSet = session.executeQueryStatement("show paths using schema template " + templateName);
        SessionDataSet.DataIterator records = dataSet.iterator();
        while(records.next()) {
            out.println(records.getString(1));
        }
        return schemaList;
    }

    private static void query(String device) {
        long startTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        for (int i = 0; i < clientCount; i++) {
            pool.execute(new QueryClient(session, device));
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
                break;
            }
        }
    }
    private static void insertData(List<String> paths) {
        long startTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        for (int i = 0; i < paths.size(); i++) {
            pool.execute(new InsertData(session, paths.get(i), insertCount, schemaList, isAligned));
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
                break;
            }
        }
    }
    private static void testConcurrent_sameNameDiffType() {
        long startTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        String tsName = "concurrentAppendTS";
        for (int i = 0; i < clientCount; i++) {
            pool.execute(new updateTemplate(session, templateName, tsName, structures.get(i)));
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
                break;
            }
        }
    }
    private static void testConcurrent_diffNameDiffType() {
        long startTime = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        String tsName = "concurrentAppendTS_";
        for (int i = 0; i < appendCount; i++) {
            int j = i % structures.size();
            pool.execute(new updateTemplate(session, templateName, tsName+i, structures.get(j)));
            schemaList.add(new MeasurementSchema(tsName + i, (TSDataType) structures.get(j).get(0),
                    (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2)));
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                out.printf("创建, 共耗时: %f s ", (System.currentTimeMillis() - startTime) / 1000.0);
                break;
            }
        }
    }

 }

class InsertData implements Runnable {
    private SessionPool session;
    private Tablet tablet;
    private boolean isAligned;

    public InsertData(SessionPool session, String device, int insertCount, List<MeasurementSchema> schemaList, boolean isAligned) {
        this.session = session;
        this.isAligned = isAligned;

        this.tablet = new Tablet(device, schemaList, insertCount);
        int rowIndex = 0;
        for (int row = 0; row < insertCount; row++) {
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, System.currentTimeMillis()+row*10000);
            for (int i = 0; i < schemaList.size(); i++) {
                switch(schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, i%2 == 0 ?true:false);
                        break;
                    case INT32:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, i);
                        break;
                    case INT64:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, System.currentTimeMillis());
                        break;
                    case FLOAT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, i*1.2f);
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, i*1.3456789);
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, "System.currentTimeMillis()");
                        break;
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            if (isAligned) {
                session.insertAlignedTablet(tablet);
            } else {
                session.insertTablet(tablet);
            }
        } catch (Exception e) {
            out.println(e);
        }
    }
}

class updateTemplate implements Runnable {
    private SessionPool session;
    private String templateName;
    private String tsName;
    private List<Object> struct;

    public updateTemplate(SessionPool session, String templateName, String tsName, List<Object> struct) {
        this.session = session;
        this.templateName = templateName;
        this.tsName = tsName;
        this.struct = struct;
    }
    public void addTSIntoTemplate(String templateName, String tsName, TSDataType tsDataType, TSEncoding tsEncoding, CompressionType compressionType) throws IoTDBConnectionException, StatementExecutionException {
        StringJoiner sb = new StringJoiner(" ");
        sb.add("alter schema template ");
        sb.add(templateName);
        sb.add(" add (");
        sb.add(tsName);
        sb.add(tsDataType.toString());
        sb.add("encoding=");
        sb.add(tsEncoding.toString());
        sb.add("compression=");
        sb.add(compressionType.toString());
        sb.add(");");
        session.executeNonQueryStatement(sb.toString());
    }
    public void run(){
        try {
            addTSIntoTemplate(templateName, tsName, (TSDataType)struct.get(0), (TSEncoding)struct.get(1), (CompressionType)struct.get(2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
class QueryClient implements Runnable {
    private SessionPool session;
    private List<String> sqlList = new ArrayList<>();
    public QueryClient(SessionPool session, String device) {
        this.session = session;
        long start = new Date().getTime()-600000;
        long end = new Date().getTime();
        sqlList.add("select * from "+device);
        sqlList.add("select * from "+device+" where s_5=true");
        sqlList.add("select * from "+device+" limit 10;");
        sqlList.add("select * from "+device+" limit 10 offset 100;");
        sqlList.add("select * from root.** limit 10 offset 100 slimit 2 soffset 10;");
        sqlList.add("select * from "+device+" order by time;");
        sqlList.add("select * from root.db.** align by device;");
        sqlList.add("select last * from "+device);
        sqlList.add("select last * from "+device+" order by timeseries desc;");
        sqlList.add("select last * from "+device+" where time < "+end);
        sqlList.add("select min_value(s_10), max_value(s_10), count(s_10) from "+device);
        sqlList.add("select avg(s_10) from "+device+" group by (["+start+","+end+"),10s);");
//        sqlList.add("select count(*) from "+device+" group by (["+start+","+end+"),10s), level=1,3;");
//        sqlList.add("select count(*) from root.** group by (["+start+","+end+"),10s), level=1,3 having count(s_0) > 2;");
//        sqlList.add("select * into "+device+" from "+device);
//        sqlList.add("select * into "+device+"_11(::), "+device+"_111(${3})"+" from "+device);
//        sqlList.add("select count(*) into "+device+"_22"+" from "+device);
    }
    @Override
    public void run() {
        for (int i = 0; i < sqlList.size(); i++) {
            try {
                SessionDataSetWrapper dataset = session.executeQueryStatement(sqlList.get(i));
                SessionDataSet.DataIterator records = dataset.iterator();
                out.println("###############");
                out.println(sqlList.get(i));
                out.println(dataset.getColumnNames());
                while(records.next()) {
                    for (int j = 1; j <= dataset.getColumnNames().size(); j++) {
                        out.printf(records.getString(j)+",");
                    }
                    out.println();
                }
                out.println("###############");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}