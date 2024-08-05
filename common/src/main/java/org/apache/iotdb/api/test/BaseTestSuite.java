package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 基础测试套件
 */
public class BaseTestSuite {
    public Logger logger = Logger.getLogger(BaseTestSuite.class);
    public Session session = null;
    // 是对齐/非对齐序列。dynamic module. 动态模版相关
    protected boolean isAligned;
    // 是否打印查询结果
    protected boolean verbose;
    // 自动创建元数据开关。 dynamic module. 动态模版相关
    protected boolean auto_create_schema;
    protected long baseTime;

    @BeforeClass
    public void beforeSuite() throws IoTDBConnectionException, ParseException, IOException {
        session = PrepareConnection.getSession();
//        logger.warn("############ BaseTestSuite BeforeClass ##########" );
        verbose = Boolean.parseBoolean(ReadConfig.getInstance().getValue("verbose"));
        isAligned = Boolean.parseBoolean(ReadConfig.getInstance().getValue("isAligned"));
        auto_create_schema = Boolean.parseBoolean(ReadConfig.getInstance().getValue("auto_create_schema"));
        baseTime = parseDate();
    }

    @AfterClass
    public void afterSuie() throws IoTDBConnectionException, StatementExecutionException {
//        logger.warn("############ BaseTestSuite AfterClass ##########" );
        cleanDatabases(verbose);
        cleanTemplates(verbose);
        session.close();
    }

    private long parseDate() throws IOException, ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.parse(ReadConfig.getInstance().getValue("time_base")).getTime();
    }

    public boolean checkStroageGroupExists(String storageGroupId) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet records = session.executeQueryStatement("show storage group " + storageGroupId)) {
            return records.hasNext();
        }
    }

    /**
     * 统计sql获取的行数
     *
     * @param sql
     * @param verbose
     * @return
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    public int countLines(String sql, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet records = session.executeQueryStatement(sql)) {
            if (verbose) {
                logger.info(sql);
                logger.info("******** start ********");
            }
            int count = 0;
            while (records.hasNext()) {
                count++;
                if (verbose) {
                    logger.info(records.next());
                } else {
                    records.next();
                }
            }
            if (verbose) {
                logger.info("******** end ********" + count);
            }
            return count;
        }
    }


    /**
     * 获取某列实际有效记录条数
     *
     * @param sql
     * @param verbose
     * @return
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    public int getCount(String sql, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement(sql);
        SessionDataSet.DataIterator recordsIter = records.iterator();
        int count = 0;
        if (verbose) {
            logger.info(sql);
            logger.info("******** start ********");
        }
        while (recordsIter.next()) {
            count = recordsIter.getInt(1);
            if (verbose) {
                logger.info(count);
            }
        }
        if (verbose) {
            logger.info("******** end ********");
        }
        return count;
    }

    public int getStorageGroupCount(String storageGroupId) throws IoTDBConnectionException, StatementExecutionException {
        return getStorageGroupCount(storageGroupId, false);
    }

    public int getStorageGroupCount(String storageGroupId, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("count databases " + storageGroupId, verbose);
    }

    public void assertTSExists(String path, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = session.executeQueryStatement("show timeseries " + path)) {
            SessionDataSet.DataIterator recordsIter = dataSet.iterator();
            while (recordsIter.next()) {
                if (verbose) {
                    logger.info(recordsIter.getString(1));
                    logger.info(recordsIter.getString(4));
                    logger.info(recordsIter.getString(5));
                    logger.info(recordsIter.getString(6));
                }
                assert path.equals(recordsIter.getString(1)) : "Timeseries exists: " + path;
            }
        }
    }

    public int getTimeSeriesCount(String timeSeries, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("count timeseries " + timeSeries, verbose);
    }

    public int getDeviceCount(String device, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("count devices " + device, verbose);
    }

    public int getRecordCount(String device, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("select count(*) from " + device, verbose);
    }

    public void checkQueryResult(String sql, Object expectValue) throws IoTDBConnectionException, StatementExecutionException {
        logger.debug("sql=" + sql);
        try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
            if (expectValue instanceof Number) {
                Double expect = Double.valueOf(expectValue.toString());
                while (dataSet.hasNext()) {
                    RowRecord records = dataSet.next();
                    Double actualValue = Double.valueOf(records.getFields().get(0).toString());
                    assert actualValue.equals(expect) : "确认结果" + dataSet.getColumnTypes().get(0) + "值: expect " + expectValue + ", actual " + records.getFields().get(0).toString();
                }
            } else {
                while (dataSet.hasNext()) {
                    RowRecord records = dataSet.next();
                    String actualValue = records.getFields().get(0).toString();
                    assert actualValue.equals(expectValue.toString()) : "确认结果" + dataSet.getColumnTypes().get(0) + "值: expect " + expectValue + ", actual " + actualValue;
                }
            }
        }
    }

    public boolean checkTemplateExists(String templateName) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = session.executeQueryStatement("show schema templates ")) {
            SessionDataSet.DataIterator records = dataSet.iterator();
            while (records.next()) {
                if (templateName.equals(records.getString(1))) {
                    return true;
                }
            }
            return false;
        }
    }

    public boolean checkTemplateContainPath(String templateName, String path) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = PrepareConnection.getSession().executeQueryStatement("show paths set schema template " + templateName)) {
            while (dataSet.hasNext()) {
                String result = dataSet.next().getFields().get(0).toString();
                if (result.equals(path)) {
                    return true;
                }
            }
            return false;
        }
    }

    public boolean checkUsingTemplate(String device, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = PrepareConnection.getSession().executeQueryStatement("show child nodes " + device)) {
            boolean result = dataSet.hasNext();
            if (verbose) {
                while (dataSet.hasNext()) {
                    RowRecord record = dataSet.next();
                    logger.info(record.toString());
                }
            }
            return !result;
        }
    }

    public void cleanDatabases(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        int count = getCount("count databases", verbose);
        if (verbose) {
            logger.info("drop databases: " + count);
        }
        if (count > 0) {
            session.executeNonQueryStatement("drop database root.**");
        }
    }

    public void cleanTemplates(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show schema templates");
        int count = 0;
        while (records.hasNext()) {
            count++;
            session.dropSchemaTemplate("`" + records.next().getFields().get(0) + "`");
        }
        if (verbose) {
            logger.info("drop templates:" + count);
        }
    }

    public void insertRecordSingle(String path, TSDataType tsDataType, boolean isAligned, String alias) throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypeList = new ArrayList<>(1);
        List<String> tsNames = new ArrayList<>(1);

        int index = 0;
        if (path.endsWith("`")) {
            int endIndex = path.length() - 2;
            while (true) {
                index = path.lastIndexOf("`", endIndex) - 1;
                if (path.charAt(index) == '`') {
                    index = index - 2;
                } else {
                    break;
                }
                endIndex = index - 1;
            }
        } else {
            index = path.lastIndexOf('.');
        }
        String device = path.substring(0, index);
        String tsName = path.substring(index + 1);
//        System.out.println("device: "+device);
//        System.out.println("tsName: "+tsName);

        tsDataTypeList.add(tsDataType);
        tsNames.add(tsName);
        if (alias == null) {
            insertRecordMulti(device, tsNames, tsDataTypeList, baseTime, isAligned, null);
        } else {
            List<String> aliasList = new ArrayList<>(1);
            aliasList.add(alias);
            insertRecordMulti(device, tsNames, tsDataTypeList, baseTime, isAligned, aliasList);
        }
    }

    public void insertRecordMulti(String device, List<String> tsNames, List<TSDataType> tsDataTypeList, long timestamp, boolean isAligned, List<String> aliasList) throws IoTDBConnectionException, StatementExecutionException {
//        System.out.println("######## insertRecordMulti device = "+device);
        List<Object> values = new ArrayList<>(tsDataTypeList.size());
        for (int i = 0; i < tsDataTypeList.size(); i++) {
            TSDataType tsDataType = tsDataTypeList.get(i);
            switch (tsDataType) {
                case BOOLEAN:
                    values.add(GenerateValues.getBoolean());
                    break;
                case INT32:
                    values.add(GenerateValues.getInt());
                    break;
                case INT64:
                    values.add(GenerateValues.getLong(8));
                    break;
                case FLOAT:
                    values.add(GenerateValues.getFloat(2, 100, 2000));
                    break;
                case DOUBLE:
                    values.add(GenerateValues.getDouble(2, 2000, 5000));
                    break;
                case TEXT:
                    values.add(GenerateValues.getCombinedCode());
                    break;
            }
        }
//        System.out.println("-----------");
//        System.out.println(tsDataTypeList);
//        System.out.println(values);
//        System.out.println("-----------");
        if (isAligned) {
            session.insertAlignedRecord(device, timestamp, tsNames, tsDataTypeList, values);
        } else {
            session.insertRecord(device, timestamp, tsNames, tsDataTypeList, values);
        }
        for (int i = 0; i < tsNames.size(); i++) {
            checkQueryResult("select " + tsNames.get(i) + " from "
                    + device + " where time=" + timestamp + ";", values.get(i));
            if (aliasList != null) {
                checkQueryResult("select " + aliasList.get(i) + " from " + device + " where time=" + timestamp + ";", values.get(i));
            }
        }
    }

    public void insertTabletSingle(String device, String tsName, TSDataType tsDataType, int insertCount, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema(tsName, tsDataType));
        insertTabletMulti(device, schemaList, insertCount, isAligned);
    }

    public void insertTabletMulti(String device, List<MeasurementSchema> schemaList, int insertCount, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        Session session = this.session;
        if (insertCount == 0) {
            PrepareConnection.getSession();
        }
        if (verbose) {
            logger.info("insertTabletMulti device=" + device + " schema=" + schemaList.size() + " insertCount=" + insertCount);
        }
        Tablet tablet = new Tablet(device, schemaList, insertCount);
        int rowIndex = 0;
        long timestamp = baseTime;
        for (int row = 0; row < insertCount; row++) {
            rowIndex = tablet.rowSize++;
            timestamp += 3600000; //+1小时
//            System.out.println("row="+row+" rowIndex="+rowIndex);
            tablet.addTimestamp(rowIndex, timestamp);
//            tablet.addTimestamp(rowIndex, row);
            for (int i = 0; i < schemaList.size(); i++) {
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getBoolean());
                        break;
                    case INT32:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getInt());
                        break;
                    case INT64:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getLong(10));
                        break;
                    case FLOAT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getFloat(2, 100, 200));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getDouble(2, 500, 1000));
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getChinese());
                        break;
                }
            }
        }
        if (isAligned) {
            session.insertAlignedTablet(tablet);
        } else {
            session.insertTablet(tablet);
        }
        checkQueryResult("select count(" + schemaList.get(0).getMeasurementId() + ") from "
                + device + ";", insertCount);
        if (insertCount == 0) {
            session.close();
        }
    }

    public void queryLastData(String tsPath, String expectValue, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        paths.add(tsPath);
        if (expectValue != null && !expectValue.isEmpty()) {
            List<String> expectValues = new ArrayList<>(1);
            expectValues.add(expectValue);
            queryLastData(paths, expectValues, verbose, null);
        } else {
            queryLastData(paths, null, verbose, null);
        }
    }

    public void queryLastData(List<String> paths, List<String> expectValues, boolean verbose, Long gtTime) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet dataSet;
        Session session = PrepareConnection.getSession();
        if (gtTime != null) {
            dataSet = session.executeLastDataQuery(paths, gtTime, 1000L);
        } else {
            dataSet = session.executeLastDataQuery(paths);
        }
        if (verbose) {
            logger.info(paths + " expect=" + expectValues);
            logger.info(dataSet.getColumnNames());
        }
        int i = 0;
        SessionDataSet.DataIterator records = dataSet.iterator();
        while (records.next()) {
            if (verbose) {
                for (int j = 1; j <= dataSet.getColumnNames().size(); j++) {
                    logger.info(records.getString(j) + ",");
                }
                logger.info("");
            }
            if (expectValues != null && !expectValues.isEmpty()) {
                assert expectValues.get(i).equals(records.getString(1)) : paths.get(i) + " :" + expectValues.get(i) + " == " + records.getString(1);
            }
            i++;
        }
        session.close();
    }

    public int getTemplateCount(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return countLines("show schema templates", verbose);
    }

    public int getTSCountInTemplate(String templateName, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show nodes in schema template " + templateName;
        return countLines(sql, verbose);
    }

    public int getSetPathsCount(String templateName, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show paths set schema template " + templateName;
        return countLines(sql, verbose);
    }

    public int getActivePathsCount(String templateName, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show paths using schema template " + templateName;
        return countLines(sql, verbose);
    }

    public void deactiveTemplate(String templateName, String path) throws IoTDBConnectionException, StatementExecutionException {
        // delete timeseries of schema template t1 from root.sg1.d1
        // deactivate schema template t1 from root.sg1.d1
        String sql = "deactivate schema template " + templateName + " from " + path;
        logger.debug(sql);
        Session session = PrepareConnection.getSession();
        session.executeNonQueryStatement(sql);
        session.close();
    }

    public void deactiveTemplate(String templateName, @NotNull List<String> paths) throws IoTDBConnectionException, StatementExecutionException {
        int count = getActivePathsCount(templateName, verbose);
        count -= paths.size();
        // delete timeseries of schema template t1 from root.sg1.d1
        // deactivate schema template t1 from root.sg1.d1
        for (int i = 0; i < paths.size(); i++) {
            checkUsingTemplate(paths.get(i), true);//: paths.get(i)+"使用了模版";
            String sql = "deactivate schema template " + templateName + " from " + paths.get(i);
            logger.debug(sql);
            session.executeNonQueryStatement(sql);
        }
        assert count == getActivePathsCount(templateName, verbose) : "解除成功";
    }

    public void addTSIntoTemplate(String templateName, List<String> tsNameList, List<TSDataType> tsDataTypeList, List<TSEncoding> tsEncodingList, List<CompressionType> compressionTypeList) throws IoTDBConnectionException, StatementExecutionException {
        int beforeCount = getTSCountInTemplate(templateName, false);
        int expectCount = beforeCount + tsNameList.size();
        StringJoiner sb = new StringJoiner(" ");
        sb.add("alter schema template ");
        sb.add(templateName);
        sb.add(" add (");
        for (int i = 0; i < tsNameList.size(); i++) {
            if (i > 0) {
                sb.add(",");
            }
            sb.add(tsNameList.get(i));
            sb.add(tsDataTypeList.get(i).toString());
            sb.add("encoding=");
            sb.add(tsEncodingList.get(i).toString());
            sb.add("compression=");
            sb.add(compressionTypeList.get(i).toString());
        }
        sb.add(");");
//        System.out.println(sb.toString());
        session.executeNonQueryStatement(sb.toString());
        int actualCount = getTSCountInTemplate(templateName, false);
        logger.debug("beforeCount=" + beforeCount);
        assert expectCount == actualCount : "成功修改模版 expect=" + expectCount + " actual=" + actualCount;
    }

    public void addTSIntoTemplate(String templateName, String tsName, TSDataType tsDataType, TSEncoding tsEncoding, CompressionType compressionType, Session conn) throws IoTDBConnectionException, StatementExecutionException {
//        int count = getTSCountInTemplate(templateName, false);
//        count ++;
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
        if (conn == null) {
            session.executeNonQueryStatement(sb.toString());
        } else {
            conn.executeNonQueryStatement(sb.toString());
            conn.close();
        }
//        assert count == getTSCountInTemplate(templateName, false) : "成功修改模版";
    }

    public void cleanTemplateNodes(String templateName, String prefix) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show paths using schema template " + templateName;
        Session session = PrepareConnection.getSession();
        SessionDataSet records = session.executeQueryStatement(sql);
        SessionDataSet.DataIterator recordsIter = records.iterator();
        while (recordsIter.next()) {
            if (recordsIter.getString(1).startsWith(prefix)) {
                deactiveTemplate(templateName, recordsIter.getString(1));
            }
        }
        sql = "show paths set schema template " + templateName;
        records = session.executeQueryStatement(sql);
        recordsIter = records.iterator();
        while (recordsIter.next()) {
            if (recordsIter.getString(1).startsWith(prefix)) {
                session.unsetSchemaTemplate(recordsIter.getString(1), templateName);
            }
        }
        session.close();
    }

}
