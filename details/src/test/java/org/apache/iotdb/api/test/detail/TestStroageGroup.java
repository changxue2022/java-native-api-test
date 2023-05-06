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

public class TestStroageGroup extends BaseTestSuite {
    private static List<String> normalSGs = new ArrayList<>();
    private static List<String> deleteSGs = new ArrayList<>();


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        normalSGs = new CustomDataProvider().getFirstColumns("data/storage-group.csv");
        deleteSGs = new CustomDataProvider().getFirstColumns("data/storage-group-data-for-delete.csv");
        if (checkStroageGroupExists("")) {
            session.executeNonQueryStatement("drop database root.**");
        }
    }
    @DataProvider(name="storageGroupNormal")
    public Iterator<Object[]> getStorageGroupNormal() throws IOException {
        return new CustomDataProvider().load("data/storage-group.csv").getData();
    }
    @DataProvider(name="storageGroupError")
    public Iterator<Object[]> getStorageGroupError() throws IOException {
        return new CustomDataProvider().load("data/storage-group-error.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupNormalMatch")
    public Iterator<Object[]> getDeleteStorageGroupNormalMatch() throws IOException {
        return new CustomDataProvider().load("data/storage-group-delete.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupNormalExact")
    public Iterator<Object[]> getDeleteStorageGroupNormalExact() throws IOException {
        return new CustomDataProvider().load("data/storage-group.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupError")
    public Iterator<Object[]> getDeleteStorageGroupError() throws IOException {
        return new CustomDataProvider().load("data/storage-group-error.csv").load("data/storage-group-delete-error.csv").getData();
    }
    @DataProvider(name="deleteStorageGroupsError")
    public Iterator<Object[]> getDeleteStorageGroupsError() throws IOException {
        return new CustomDataProvider().load("data/storage-group-deleteG-error.csv").getData();
    }

    /**
     * 创建storage groups, 忽略错误
     * @throws IoTDBConnectionException
     */
    public void createStorageGroupsIgnoreError(List<String> sgs) throws IoTDBConnectionException {
        for (String storageGroupId : sgs) {
            try {
                if (storageGroupId.equals("root.abc.bbbb")) {
                    System.out.println("############ "+storageGroupId);
                } else {
                    session.setStorageGroup(storageGroupId);
                }
            } catch (StatementExecutionException e) {
            }
        }
    }
    public void createStorageGroupIgnoreError(String storageGroupId) throws IoTDBConnectionException {
        try {
            session.setStorageGroup(storageGroupId);
        } catch (StatementExecutionException e) {
        }
    }

    @Test(priority=1,dataProvider = "storageGroupNormal")
    public void testSetStroageGroup_normal(String storageGroupId, String expect, String msg) throws IoTDBConnectionException, StatementExecutionException {
//        out.println(storageGroupId + "," + msg);
        session.setStorageGroup(storageGroupId);
        assert checkStroageGroupExists(storageGroupId) == true : storageGroupId+" , "+msg ;
    }

    // length:65 characters with only character and dot 没有pao
    @Test(priority=2,dataProvider = "storageGroupError", expectedExceptions = StatementExecutionException.class)
    public void testSetStorageGroup_error(String storageGroupId, String expect, String msg) throws IoTDBConnectionException, StatementExecutionException {
        if (storageGroupId.startsWith("root.c")) {
            String sgId = "root.c";
            List<String> sg = new ArrayList<>(1);
            sg.add(sgId);
            createStorageGroupsIgnoreError(sg);
        }
        session.setStorageGroup(storageGroupId);
        // 失败了打印
        System.out.println(storageGroupId + "," + msg);
    }

    @Test(priority=3,dataProvider = "deleteStorageGroupNormalExact")
    public void testDeleteStorageGroup_exact_normal(String storageGroupId, String expect, String msg) throws IoTDBConnectionException, StatementExecutionException, IOException {
        createStorageGroupsIgnoreError(normalSGs);
        assert checkStroageGroupExists(storageGroupId) == true : "exists: " + storageGroupId;
        session.deleteStorageGroup(storageGroupId);
        assert checkStroageGroupExists(storageGroupId) == false : "already deleted: " + storageGroupId;
    }
    @Test(priority=4,dataProvider = "deleteStorageGroupNormalMatch")
    public void testDeleteStorageGroup_match_normal(String storageGroupId, String expect, String msg) throws IoTDBConnectionException, StatementExecutionException {
        //session.deleteStorageGroup("root.**");
        // TIMECHODB-50
        createStorageGroupsIgnoreError(deleteSGs);
        int expect_count = getStorageGroupCount("") - getStorageGroupCount(storageGroupId);
        session.deleteStorageGroup(storageGroupId);
        assert expect_count == getStorageGroupCount("") : storageGroupId+" , expect " + expect + "," + msg ;
    }
    @Test(priority=5,dataProvider = "deleteStorageGroupError", expectedExceptions = StatementExecutionException.class)
    public void testDeleteStorageGroup_error(String storageGroupId, String expect, String msg) throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup(storageGroupId);
        //失败打印
        System.out.println(storageGroupId + ", expect " + expect + " ," + msg);
    }

    @Test(expectedExceptions = StatementExecutionException.class)
    public void testDeleteStorageGroups_empty() throws IoTDBConnectionException, StatementExecutionException {
        createStorageGroupIgnoreError("root.abd.xx");
        int beforeCount = getStorageGroupCount("");
        List<String> sgs = new ArrayList<>();
        session.deleteStorageGroups(sgs);
        int afterCount = getStorageGroupCount("");
        sgs = null;
        assert afterCount == beforeCount : "删除空list before=" + beforeCount + ", after=" + afterCount;
    }
    @Test(priority = 6)
    public void testDeleteStorageGroups_min() throws IoTDBConnectionException, StatementExecutionException {
        String storageGroupId = "root.c";
        try {
            session.setStorageGroup(storageGroupId);
        } catch (IoTDBConnectionException e) {
        } catch (StatementExecutionException e) {
        }
        assert checkStroageGroupExists(storageGroupId) == true : "exists: "+storageGroupId;

        List<String> sgs = new ArrayList<>(1);
        sgs.add(storageGroupId);
        session.deleteStorageGroups(sgs);
        assert checkStroageGroupExists(storageGroupId) == false : "already deleted: "+storageGroupId;
        sgs = null;
    }
    @Test(priority=7,enabled = false) // OOM in my computer
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

    @Test(priority=8,dataProvider = "deleteStorageGroupsError", expectedExceptions = StatementExecutionException.class)
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
        createStorageGroupsIgnoreError(sgsExists);
        int expectCount = getStorageGroupCount("", true) - total + 1 ;
        session.deleteStorageGroups(sgs);
        int actualCount = getStorageGroupCount("", true);
        System.out.println(msg);
        Thread.sleep(1000);
        assert expectCount == actualCount: "删除后 actual=:" + actualCount + ", expect=" +expectCount + ", total="+total;

    }
}
