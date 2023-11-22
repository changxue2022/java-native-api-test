package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;

public class TestConcurrentNodeUrls {
    private static int databaseCount = 1;
    private static int deviceCount = 1000000;
    private static int sensorCount = 100;
    private static boolean isAligned = false;
    private static int clinetCount = 102;
    private static String sensorTypePolicy = "0"; // random, default, allInOrder, [specifiedIntValue]
    private static String sensorTypeOrder = "3,22,45,69,89,101"; // sensorTypePolicy = default

    private static int writeCountInBatch = 0;
//    private static String tsFormat = "root.sg2_testConcurrent_%d.device中文abcdefghijklmnopqrstuvwxyz1ABCDEFGHIJKLMNOPQRSTUVWXYZ_%07d.s_abcdefghijklmnopqrstuvwxyz12345ABCDEFGHIJKLMN_%03d";



    public static void getStruct(List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes) {
        for (int i = 0; i < sensorCount; i++) {
            tsDataTypes.add(TSDataType.BOOLEAN);
            tsEncodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.UNCOMPRESSED);
        }
    }

    public static void main(String[] args) throws IoTDBConnectionException, InterruptedException, IOException {

        long startTime = System.currentTimeMillis();
        out.println("database="+databaseCount);
        out.println("deviceCount="+deviceCount);
        out.println("sensorCount="+sensorCount);
        out.println("clinetCount="+clinetCount);
        out.println("isAligned="+isAligned);

        List<TSDataType> tsDataTypes = new ArrayList<>(sensorCount);
        List<TSEncoding> tsEncodings = new ArrayList<>(sensorCount);
        List<CompressionType> compressionTypes = new ArrayList<>(sensorCount);
        getStruct(tsDataTypes, tsEncodings, compressionTypes);

        ExecutorService pool = Executors.newFixedThreadPool(clinetCount);
        for (int j = 0; j < databaseCount; j++) {
            for (int i = 0; i < deviceCount ; i++) {
                pool.execute(new SessionClientRunnable3( j, i, sensorCount, isAligned, tsDataTypes, tsEncodings, compressionTypes));
            }

        }
        pool.shutdown();
        while (true) {//等待所有任务都执行结束
            if (pool.isTerminated()) {//所有的子线程都结束了
                System.out.printf("创建TS %d, 共耗时: %f s", databaseCount*deviceCount*sensorCount, (System.currentTimeMillis()-startTime)/1000.0);
                break;
            }
        }
    }
}

class SessionClientRunnable3 implements Runnable {
    private Session session;
    private int sensorCount = 0;
    private int databaseIndex = 0;
    private int deviceIndex = 0;
    private boolean isAligned;
    private List<TSDataType> tsDataTypes;
    private List<TSEncoding> tsEncodings;
    private List<CompressionType> compressionTypes;
//    private String databasePrefix;
    private String devicePrefix = "root.sg1.d_";
    private String tsPrefix = "s_";


    public SessionClientRunnable3(int databaseIndex, int deviceIndex, int sensorCount, boolean isAligned, List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes) throws IoTDBConnectionException, IOException {
        this.databaseIndex = databaseIndex;
        this.deviceIndex = deviceIndex;
        this.sensorCount = sensorCount;
        this.isAligned = isAligned;
        this.tsDataTypes = tsDataTypes;
        this.tsEncodings = tsEncodings;
        this.compressionTypes = compressionTypes;
        List nodeUrls = new ArrayList(3);
        nodeUrls.add("iotdb-44:6667");
        nodeUrls.add("iotdb-45:6667");
        nodeUrls.add("iotdb-46:6667");
        session = new Session.Builder().nodeUrls(nodeUrls).build();
    }
    @Override
    public void run()  {
        try {
            session.open(false);
            session.setFetchSize(1000);
            List<String> paths = new ArrayList<>(this.sensorCount);
            if (this.isAligned) {
                for (int i = 0; i < this.sensorCount; i++) {
                    paths.add("s_"+i);
                }
                long startTime = System.currentTimeMillis();
                session.createAlignedTimeseries (devicePrefix+deviceIndex, paths, tsDataTypes, tsEncodings, compressionTypes, null);
                long elapseTime = System.currentTimeMillis()-startTime;
                out.println(Thread.currentThread().getName()+" "+ deviceIndex + " cost:"+elapseTime);
            } else {
                for (int i = 0; i < this.sensorCount; i++) {
                    paths.add(this.devicePrefix+deviceIndex+".s_"+i);
                }
                long startTime = System.currentTimeMillis();
                session.createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, null, null, null);
                long elapseTime = System.currentTimeMillis()-startTime;
                out.println(Thread.currentThread().getName()+" "+ deviceIndex + " cost:"+elapseTime);
            }
        } catch (Exception e) {
            out.println(Thread.currentThread().getName() +"  "+ this.deviceIndex +" " +e);
        } finally {
            try {
                session.close();
            } catch (IoTDBConnectionException e) {
                out.printf("[%s] %s", Thread.currentThread().getName(), e);
            }
        }
    }
}