package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestDataBase extends BaseTestSuite {
    private static List<String> normalSGs = new ArrayList<>();
    private static List<String> deleteSGs = new ArrayList<>();

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        cleanDatabases(verbose);
        normalSGs = new CustomDataProvider().getFirstColumns("data/storage-group.csv");
        deleteSGs = new CustomDataProvider().getFirstColumns("data/storage-group-data-for-delete.csv");
        if (checkStroageGroupExists("")) {
            session.executeNonQueryStatement("drop database root.**");
        }
    }

    @DataProvider(name="deleteStorageGroupNormalMatch")
    private Iterator<Object[]> getDeleteStorageGroupNormalMatch() throws IOException {
        return new CustomDataProvider().load("data/storage-group-delete.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupNormalExact")
    private Iterator<Object[]> getDeleteStorageGroupNormalExact() throws IOException {
        return new CustomDataProvider().load("data/storage-group.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupError", parallel = true)
    private Iterator<Object[]> getDeleteStorageGroupError() throws IOException {
        return new CustomDataProvider().load("data/storage-group-error.csv").load("data/storage-group-delete-error.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupsError")
    private Iterator<Object[]> getDeleteStorageGroupsError() throws IOException {
        return new CustomDataProvider().load("data/storage-group-deleteG-error.csv").getData();
    }
    @DataProvider(name="storageGroupNormal", parallel = true)
    private Iterator<Object[]> getStorageGroupNormal() throws IOException {
        return new CustomDataProvider().load("data/storage-group.csv").getData();
    }
    @DataProvider(name="storageGroupError", parallel = true)
    private Iterator<Object[]> getStorageGroupError() throws IOException {
        return new CustomDataProvider().load("data/storage-group-error.csv").getData();
    }
    //##################################################################################################################
    @Test(priority=10, dataProvider = "storageGroupNormal")
    public void testSetStorageGroup_normal(String storageGroupId, String msg) throws IoTDBConnectionException, StatementExecutionException {
//        out.println(storageGroupId + "," + msg);
        session.setStorageGroup(storageGroupId);
        assert checkStroageGroupExists(storageGroupId) == true : storageGroupId+" , "+msg ;
    }

    // TIMECHODB-84  TIMECHODB-123
    @Test(priority=20, dataProvider = "storageGroupError", expectedExceptions = StatementExecutionException.class)
    public void testSetStorageGroup_error(String storageGroupId, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.setStorageGroup(storageGroupId);
        // 失败了打印
        System.out.println(storageGroupId + "," + msg);
    }

    @Test(priority=30, dataProvider = "deleteStorageGroupNormalExact")
    public void testDeleteStorageGroup_exact_normal(String storageGroupId, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        assert checkStroageGroupExists(storageGroupId) == true : "exists: " + storageGroupId;
        session.deleteStorageGroup(storageGroupId);
        assert checkStroageGroupExists(storageGroupId) == false : "already deleted: " + storageGroupId;
    }
    @Test(priority=40, dataProvider = "deleteStorageGroupNormalMatch")
    public void testDeleteStorageGroup_match_normal(String storageGroupId, String msg) throws IoTDBConnectionException, StatementExecutionException {
        //session.deleteStorageGroup("root.**");
        // TIMECHODB-50
        int expect_count = getStorageGroupCount("") - getStorageGroupCount(storageGroupId);
        session.deleteStorageGroup(storageGroupId);
        assert expect_count == getStorageGroupCount("") : storageGroupId+ ", " + msg ;
    }
    @Test(priority=50,dataProvider = "deleteStorageGroupError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteStorageGroup_error(String storageGroupId, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup(storageGroupId);
        //失败打印
        System.out.println(storageGroupId + " ," + msg);
    }

    @Test(priority = 50, expectedExceptions = StatementExecutionException.class)
    public void testDeleteStorageGroups_empty() throws IoTDBConnectionException, StatementExecutionException {
        String database = "root.abd.xx";
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        int beforeCount = getStorageGroupCount("");
        List<String> sgs = new ArrayList<>();
        session.deleteStorageGroups(sgs);
        int afterCount = getStorageGroupCount("");
        sgs = null;
        assert afterCount == beforeCount : "删除空list before=" + beforeCount + ", after=" + afterCount;
    }
    @Test(priority = 60)
    public void testDeleteStorageGroups_min() throws IoTDBConnectionException, StatementExecutionException {
        String storageGroupId = "root.c";
        session.setStorageGroup(storageGroupId);
        assert checkStroageGroupExists(storageGroupId) == true : "exists: "+storageGroupId;

        List<String> sgs = new ArrayList<>(1);
        sgs.add(storageGroupId);
        session.deleteStorageGroups(sgs);
        assert checkStroageGroupExists(storageGroupId) == false : "already deleted: "+storageGroupId;
        sgs = null;
    }
    @Test(priority=80,dataProvider = "deleteStorageGroupsError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteStorageGroups_error(String errStorageGroupId, String total_str, String position_str, String msg) throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
        int total = Integer.parseInt(total_str);
        int position = Integer.parseInt(position_str);
        List<String> sgs = new ArrayList<>(total);
        List<String> sgsExists = new ArrayList<>(total-1);
        if (position == 1) {
            sgs.add(errStorageGroupId);
            for (int i = 1; i < total; i++) {
                sgs.add(normalSGs.get(i-1));
                sgsExists.add(normalSGs.get(i-1));
            }
        } else if (position == total) {
            for (int i = 0; i < total-1; i++) {
                sgs.add(normalSGs.get(i));
                sgsExists.add(normalSGs.get(i));
            }
            sgs.add(errStorageGroupId);
        } else {
            int i = 0;
            for (; i < position; i++) {
                sgs.add(normalSGs.get(i));
                sgsExists.add(normalSGs.get(i));
            }
            sgs.add(errStorageGroupId);
            i++;
            for (; i < total; i++) {
                sgs.add(normalSGs.get(i-1));
                sgsExists.add(normalSGs.get(i-1));
            }
        }
        System.out.println(sgs);
        int expectCount = getStorageGroupCount("", verbose) - total + 1 ;
        session.deleteStorageGroups(sgs);
        int actualCount = getStorageGroupCount("", verbose);
        System.out.println(msg);
        Thread.sleep(1000);
        assert expectCount == actualCount: "删除后 actual=:" + actualCount + ", expect=" +expectCount + ", total="+total;
    }
    @Test(priority=100,enabled = false) // OOM in my computer
    public void testDeleteStorageGroups_max() throws IoTDBConnectionException, StatementExecutionException {
        Integer maxValue = Integer.MAX_VALUE;
        List<String> sgs = new ArrayList<>(maxValue);
        for (int i = 0; i < maxValue; i++) {
            String storageGroupId = "root.testMax" + i;
            session.setStorageGroup(storageGroupId);
            sgs.add(storageGroupId);
        }
        assert getStorageGroupCount("root.**") >= maxValue : "Exists storage group : "+maxValue;
        session.deleteStorageGroups(sgs);
        assert checkStroageGroupExists("root.testMax*") == true : maxValue +" storage groups deleted";
    }

}
