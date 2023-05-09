package org.apache.iotdb.api.test.namecheck;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestDatabase extends BaseTestSuite {
    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        session.createDatabase("root.c");
        session.createDatabase("root.c.c1");
    }
    public void afterClass() {

    }

}
