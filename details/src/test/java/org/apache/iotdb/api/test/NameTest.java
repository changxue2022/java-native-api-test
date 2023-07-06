package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.testng.annotations.DataProvider;

import java.io.IOException;
import java.util.Iterator;

public class NameTest extends BaseTestSuite {
    @DataProvider(name = "normalNames", parallel = true)
    private Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }
    @DataProvider(name = "errorNames", parallel = true)
    private Iterator<Object[]> getErrorNames() throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }

}
