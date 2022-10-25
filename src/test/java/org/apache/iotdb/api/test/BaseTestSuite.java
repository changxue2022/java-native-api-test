package org.apache.iotdb.api.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.test.utils.PrepareConnection;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;

import static java.lang.System.out;

public class BaseTestSuite {
    Session session = null;

    @BeforeClass
    public void beforeSuite() throws IoTDBConnectionException, IOException {
        session = PrepareConnection.getSession();
    }
    @AfterClass
    public void afterSuie() throws IoTDBConnectionException {
        session.close();
    }
    public boolean checkStroageGroupExists(String storageGroupId) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show storage group "+storageGroupId);
        return records.hasNext();
    }
    public int myQuery(String sql, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement(sql);
        if (verbose) {
            out.println("******** start ********");
        }
        int count = 0;
        while (records.hasNext()) {
            count++;
            if (verbose) {
                out.println(records.next());
            } else {
                records.next();
            }
        }
        if (verbose) {
            out.println("******** end ********"+count);
        }
        return count;
    }
    public int getStorageGroupCount(String storageGroupId) throws IoTDBConnectionException, StatementExecutionException {
        return getStorageGroupCount(storageGroupId, false);
    }
    public int getStorageGroupCount(String storageGroupId, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return myQuery("show storage group "+storageGroupId, verbose);
    }

    public int getTimeSeriesCount(String timeSeries, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return myQuery("show timeseries "+timeSeries, verbose);
    }

    public int getRecordCount(String timeSeries, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return myQuery("select count(*) from "+timeSeries, verbose);
    }

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, IOException {
        BaseTestSuite bs = new BaseTestSuite();
//        bs.beforeSuite();
//        out.println(bs.checkStroageGroupExists("root.**"));
        bs.session.deleteStorageGroup("root.abc.b");
    }
}
