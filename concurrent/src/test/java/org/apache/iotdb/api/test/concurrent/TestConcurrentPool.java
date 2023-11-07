package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;

public class TestConcurrentPool {
    private static int databaseCount = 1;
//    private static int deviceCount = 40000000;
    private static int deviceCount = 1000000;
    private static int sensorCount = 100;
    private static boolean isAligned = false;
    private static int clientCount = 100;
    private static int fromIndex = 0;
    private static SessionPool sessionPool;
    private static String databasePrefix = "root.test.g_";
    private static String devicePrefix = "d_";
    private static String tsPrefix = "s_";
    private static int insertCount = 2000;
    private static int insertBatchSize = 1000;
    private static int deviceLoop = 10;

    private static List<TSDataType> tsDataTypes = new ArrayList<>(sensorCount);
    private static List<TSEncoding> tsEncodings = new ArrayList<>(sensorCount);
    private static List<CompressionType> compressionTypes = new ArrayList<>(sensorCount);

    private static List<String> hostList = new ArrayList<>(3);

    static {
        hostList.add("iotdb-44:6667");
        hostList.add("iotdb-45:6667");
        hostList.add("iotdb-46:6667");
    }
    public static void getStruct(List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes) {
//        for (int i = 0; i < sensorCount; i++) {
//            tsDataTypes.add(TSDataType.INT32);
//            tsEncodings.add(TSEncoding.GORILLA);
//            compressionTypes.add(CompressionType.SNAPPY);
//        }
        int i =0;
        for(;i<9;i++){
            tsDataTypes.add(TSDataType.BOOLEAN);
            tsEncodings.add(TSEncoding.RLE);
            compressionTypes.add(CompressionType.SNAPPY);
        }
        for(;i<29;i++){
            tsDataTypes.add(TSDataType.INT32);
            tsEncodings.add(TSEncoding.RLE);
            compressionTypes.add(CompressionType.SNAPPY);
        }
        for(;i<49;i++){
            tsDataTypes.add(TSDataType.INT64);
            tsEncodings.add(TSEncoding.RLE);
            compressionTypes.add(CompressionType.SNAPPY);
        }
        for(;i<74;i++){
            tsDataTypes.add(TSDataType.FLOAT);
            tsEncodings.add(TSEncoding.GORILLA);
            compressionTypes.add(CompressionType.SNAPPY);
        }
        for(;i<99;i++){
            tsDataTypes.add(TSDataType.DOUBLE);
            tsEncodings.add(TSEncoding.GORILLA);
            compressionTypes.add(CompressionType.SNAPPY);
        }
        tsDataTypes.add(TSDataType.TEXT);
        tsEncodings.add(TSEncoding.DICTIONARY);
        compressionTypes.add(CompressionType.SNAPPY);
    }

    public static void main(String[] args) throws IoTDBConnectionException, InterruptedException, IOException {
        sessionPool = new SessionPool.Builder()
                        .nodeUrls(hostList)
                        .user("root")
                        .password("root")
                        .maxSize(clientCount)
                        .build();

        out.println("#######################");
        out.println("database="+databaseCount);
        out.println("deviceCount="+deviceCount);
        out.println("sensorCount="+sensorCount);
        out.println("clinetCount="+ clientCount);
        out.println("fromIndex="+ fromIndex);
        out.println("isAligned="+isAligned);
        out.println("#######################");
        getStruct(tsDataTypes, tsEncodings, compressionTypes);
//        out.println(tsDataTypes);

//        createMetaData();
        int loop = insertCount/insertBatchSize;
        if (loop == 0) {
            loop++;
            insertBatchSize = insertCount;
        }
        for (int i = 0; i < loop; i++) {
            insertData();
        }
    }
    public static void createMetaData() throws IoTDBConnectionException, IOException {
        long startTime = System.currentTimeMillis();

        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        int deviceBatchSize = deviceCount / deviceLoop ;
        int deviceIndex = 0;
        for (int l = 0; l < deviceLoop ; l++) {
            for (int j = 0; j < databaseCount; j++) {
                for (int i = 0; i < deviceBatchSize; i++) {
                    deviceIndex = l* deviceBatchSize + i+fromIndex;
                    pool.execute(new SessionClientCreateRunnable(sessionPool,
                            databasePrefix + j + "." + devicePrefix + deviceIndex + "." + tsPrefix,
                            isAligned, tsDataTypes, tsEncodings, compressionTypes));

                }
            }
            pool.shutdown();
            while (true) {//等待所有任务都执行结束
                if (pool.isTerminated()) {//所有的子线程都结束了
                    System.out.printf("创建TS %d, 共耗时: %f s ", databaseCount * deviceCount * sensorCount, (System.currentTimeMillis() - startTime) / 1000.0);
                    break;
                }
            }
        }
        sessionPool.close();
    }
    public static void insertData() {
        long startTime = System.currentTimeMillis();
        int endDeviceIndex = deviceCount + fromIndex;

        List<MeasurementSchema> schemaList = new ArrayList<>();
        for (int i = 0; i < tsDataTypes.size(); i++) {
            schemaList.add(new MeasurementSchema(tsPrefix+i, tsDataTypes.get(i),
                    tsEncodings.get(i), compressionTypes.get(i)));
        }

        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        int deviceBatchSize = deviceCount / deviceLoop ;
        int deviceIndex = 0;
        for (int l = 0; l < deviceLoop ; l++) {
            for (int j = 0; j < databaseCount; j++) {
                for (int i = 0; i < deviceBatchSize; i++) {
                    deviceIndex = l* deviceBatchSize + i+fromIndex;
                    pool.execute(new SessionClientInsertRunnable(sessionPool,
                            databasePrefix+j+"."+devicePrefix+deviceIndex, insertBatchSize, isAligned, schemaList));
                }
            }
        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                System.out.printf("insert %d devices %d lines, 共耗时: %f s ", databaseCount*deviceCount, insertCount, (System.currentTimeMillis()-startTime)/1000.0);
                break;
            }
        }
        sessionPool.close();
    }
}



