package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestDatabaseParams extends BaseTestSuite {
    private String database = "root.c";
    private String database1 = "root.nonExistedPath";


    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        if (checkStroageGroupExists(database1)) {
            session.deleteDatabase(database1);
        }
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase(database);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testSetStorageGroupNull() throws IoTDBConnectionException, StatementExecutionException {
        session.setStorageGroup(null);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateDatabaseNull() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(null);
    }
    // TIMECHODB-122
    @Test(enabled = false, expectedExceptions = StatementExecutionException.class)
    public void testDeleteSgNull() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup(null);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteSgsNull() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroups(null);
    }
    // TIMECHODB-122
    @Test(enabled = false, expectedExceptions = StatementExecutionException.class)
    public void testDeleteDatabaseNull() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase(null);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteDatabasesNull() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabases(null);
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testSetStroageGroupEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.setStorageGroup("");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateDatabaseEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase("");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteSgEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup("");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteSgsEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroups(new ArrayList<>(1));
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteDatabaseEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase("");
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteDatabasesEmpty() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabases(new ArrayList<>(1));
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDuplicateCreate() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(database);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testCreateChild() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase(database+".c1");
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testSgNonExist() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup(database1);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteDatabaseNonExist() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteDatabase(database1);
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteSgsNonExist() throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        paths.add(database1);
        session.deleteStorageGroups(paths);
    }
    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteDatabasesNonExist() throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        paths.add(database1);
        session.deleteDatabases(paths);
    }

    @Test
    public void testDuplicateDelete() throws IoTDBConnectionException, StatementExecutionException {
        String database2 = "root.duplicate";
        if (!checkStroageGroupExists(database2)) {
            session.createDatabase(database2);
        }
        int count = getStorageGroupCount("", false);
        assert checkStroageGroupExists(database2) : "database 已经存在："+database2;
        List<String> paths = new ArrayList<>(2);
        paths.add(database2);
        paths.add(database2);
        session.deleteDatabases(paths);
        assert count -1 == getStorageGroupCount("", false);
        assert checkStroageGroupExists(database2) == false : "database 已删除:"+database2;
    }

}
