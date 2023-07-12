package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

public class PipeNameTest extends BaseTestSuite {

//    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        clean_pipes();
    }
//    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        clean_pipes();
    }
    @DataProvider(name = "normalNames", parallel = true)
    private Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }
    @DataProvider(name = "sameNames", parallel = true)
    private Iterator<Object[]> getSameNames() throws IOException {
        return new CustomDataProvider().load("data/same-name-concurrent.csv").getData();
    }
    @DataProvider(name = "errorNames", parallel = true)
    private Iterator<Object[]> getErrorNames() throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }

    private void check_pipe_status(String name, String status) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show pipe " + name);
        while(records.hasNext()) {
            RowRecord row = records.next();
            assert status.equals(row.getFields().get(2).toString()) : "pipe状态检查:expect ["+status+"], actual ["+row.getFields().get(2).toString()+"]";
//            assert name.equals(row.getFields().get(0).toString()) : "show pipe中的名称检查:expect ["+name+"], actual ["+row.getFields().get(0).toString()+"]";
        }
    }
    private void clean_pipes() throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show pipes;");
        int index = 0;
        while(records.hasNext()) {
            index++;
            RowRecord row = records.next();
            if (verbose) {
                System.out.println("drop pipe " + row.getFields().get(0));
            }
            session.executeNonQueryStatement("drop pipe " + row.getFields().get(0));
        }
        if (verbose) {
            System.out.println("drop pipes :"+index);
        }
    }
    @Test(priority = 10, dataProvider = "normalNames")
    public void testPipe_normal(String name, String comment, String Index) throws IoTDBConnectionException, StatementExecutionException, IOException {
        String sql = "create pipe " +name+
                " with connector ('connector'='iotdb-thrift-connector', 'connector.ip'='127.0.0.1', 'connector.port'='6667');";
        Session s = PrepareConnection.getSession();
        s.executeNonQueryStatement(sql);
        check_pipe_status(name, "STOPPED");
        s.executeNonQueryStatement("start pipe "+name);
        check_pipe_status(name, "RUNNING");
        s.executeNonQueryStatement("drop pipe "+name);
        s.close();
    }
    @Test(priority = 20, dataProvider = "errorNames", expectedExceptions = StatementExecutionException.class)
    public void testPipe_error(String name, String comment, String Index) throws IoTDBConnectionException, StatementExecutionException, IOException {
        String sql = "create pipe " +name+
                " with connector ('connector'='iotdb-thrift-connector', 'connector.ip'='127.0.0.1', 'connector.port'='6667');";
        System.out.println(sql);
        Session s = PrepareConnection.getSession();
        s.executeNonQueryStatement(sql);
        s.executeNonQueryStatement("drop pipe "+name);
        s.close();
    }

    @Test(priority = 30, dataProvider = "sameNames")
    public void testConcurrent_sameName(String name, String comment, String Index) throws IoTDBConnectionException, StatementExecutionException, IOException {
        String sql = "create pipe " +name+
                " with connector ('connector'='iotdb-thrift-connector', 'connector.ip'='127.0.0.1', 'connector.port'='6667');";
        Session s = PrepareConnection.getSession();
        s.executeNonQueryStatement(sql);
        check_pipe_status(name, "STOPPED");
        s.executeNonQueryStatement("start pipe "+name);
        check_pipe_status(name, "RUNNING");
        s.executeNonQueryStatement("drop pipe "+name);
        s.close();
    }

}
