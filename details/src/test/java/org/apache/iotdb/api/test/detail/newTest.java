package org.apache.iotdb.api.test.detail;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

public class newTest extends BaseTestSuite {
    private static Logger logger = Logger.getLogger(newTest.class);
    private String database = "root.cx3.`www.Timecho.com`";
    private String device = database + ".`multi(1)`";
    private static final int maxLevel = 256;

    List<List<String>> normalNames;
    List<List<String>> normalNames_keyword;
    List<List<Object>> structures;

    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
        normalNames = new CustomDataProvider().loadString("data/names-normal.csv",',');
        normalNames_keyword = new CustomDataProvider().loadString("data/keyword-normal.csv",',');
        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");
//        List<Map<String, String>> props = new CustomDataProvider().loadProps("data/props-normal.csv");
//        List<Map<String, String>> attributes = new CustomDataProvider().loadProps("data/props-normal.csv");
//        List<Map<String, String>> tags = new CustomDataProvider().loadProps("data/tag-normal.csv");

    }
    @DataProvider(name="storageGroupNormal", parallel = true)
    public Iterator<Object[]> getStorageGroupNormal() throws IOException {
        return new CustomDataProvider().load("data/storage-group.csv").getData();
    }

    @Test(enabled = true, priority = 10, dataProvider = "storageGroupNormal")
    public void testNormalTS_nonAligned(String databaseName, String comment) throws IoTDBConnectionException, StatementExecutionException, IOException {
        session.createDatabase(databaseName);
        int count = normalNames.size()+normalNames_keyword.size();
        System.out.println(normalNames.size());
        List<String> paths = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String path = "";
            String device = "";
            String tsName = "";
            if (i < normalNames.size()) {
                device = databaseName+"."+normalNames.get(i).get(0);
                tsName = normalNames.get(i).get(0);
            } else {
                device = databaseName + "." + normalNames_keyword.get(i - normalNames.size()).get(0);
                tsName = normalNames_keyword.get(i - normalNames.size()).get(0);
            }
            path = device + "." + tsName;
            paths.add(path);
            List<Object> struct = structures.get(i%structures.size());
            TSDataType tsDataType = (TSDataType) struct.get(0);
            TSEncoding tsEncoding = (TSEncoding) struct.get(1);
            CompressionType compressionType = (CompressionType) struct.get(2);
            String alias = GenerateValues.getCombinedCode();
            session.createTimeseries(path, tsDataType, tsEncoding, compressionType, null, null, null, alias);
            assertTSExists(path, false);
            insertRecordSingle(path, tsDataType, false, alias);
            session.deleteData(path, 102);
            insertTabletSingle(device, tsName, tsDataType, 1,false);
        }
        session.deleteTimeseries(paths);
    }
    @Test(enabled = true, priority = 20)
    public void testNormalTS_aligned() throws IoTDBConnectionException, StatementExecutionException, IOException {
        session.createDatabase(database);
        int count = normalNames.size()+normalNames_keyword.size();
        List<String> paths = new ArrayList<>(count);
        List<TSDataType> tsDataTypeList = new ArrayList<>(count);
        List<TSEncoding> tsEncodingList = new ArrayList<>(count);
        List<CompressionType> compressionTypeList = new ArrayList<>(count);
        List<String> aliasList = new ArrayList<>(count);
        List<IMeasurementSchema> schemaList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String path = "";
            if (i > normalNames.size() -1) {
                path = normalNames.get(i).get(0);
            } else {
                path = normalNames.get(i).get(0);
            }
            paths.add(path);
            List<Object> struct = structures.get(i%structures.size());
            tsDataTypeList.add((TSDataType) struct.get(0));
            tsEncodingList.add((TSEncoding) struct.get(1));
            compressionTypeList.add((CompressionType) struct.get(2));
            aliasList.add(GenerateValues.getCombinedCode());
            schemaList.add(new MeasurementSchema(path, (TSDataType) struct.get(0), (TSEncoding) struct.get(1), (CompressionType) struct.get(2)));
        }
        session.createAlignedTimeseries(device, paths, tsDataTypeList, tsEncodingList, compressionTypeList, aliasList);
        insertTabletMulti(device, schemaList, 10, true);
        session.deleteTimeseries(paths);
    }



    /**
     * mem:64m level=81 oom
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    @Test(enabled = false, priority = 100)
    public void testLevel() throws IoTDBConnectionException, StatementExecutionException, IOException {
        TSDataType tsDataType = TSDataType.FLOAT;
        TSEncoding tsEncoding = TSEncoding.GORILLA;
        CompressionType compressionType = CompressionType.SNAPPY;
        StringJoiner joiner = new StringJoiner(".");
        joiner.add("root");
        joiner.add("maxLevelTest");
        logger.info("create database: "+joiner.toString());
        session.setStorageGroup(joiner.toString());
        for (int i = 2; i < maxLevel; i++) {
            joiner.add("level_"+i);
            String path = joiner.toString()+".sensor_0";
            session.createTimeseries(path, tsDataType, tsEncoding, compressionType);
            assertTSExists(path, verbose);
            insertTabletSingle(joiner.toString(), "sensor_0", tsDataType, 10, verbose);
            // session.deleteTimeseries(path);
        }
    }

}
