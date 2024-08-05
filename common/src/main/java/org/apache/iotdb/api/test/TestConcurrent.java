package org.apache.iotdb.api.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;

// 使用session 多线程创建元数据，每个TS类型固定。

// 定义一个公共类TestConcurrent，可能用于执行并发测试。
public class TestConcurrent {
    // 定义数据库的数量。
    private static int databaseCount = 1;
    // 定义设备的数量。
    private static int deviceCount = 1000000;
    // 定义传感器的数量。
    private static int sensorCount = 100;
    // 定义是否使用对齐的时间序列数据。
    private static boolean isAligned = false;
    // 定义客户端的数量。
    private static int clinetCount = 102;

    // 定义一个列表，用于存储IoTDB服务的主机名。
    private static List<String> hostList = new ArrayList<>(3);

    // 静态初始化块，用于初始化hostList。
    static {
        hostList.add("iotdb-44");
        hostList.add("iotdb-45");
        hostList.add("iotdb-46");
    }

    // 一个工具方法，用于构建时间序列数据类型、编码和压缩类型的结构。
    public static void getStruct(List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes) {
        for (int i = 0; i < sensorCount; i++) {
            tsDataTypes.add(TSDataType.INT32);   // 为每个传感器添加数据类型INT32。
            tsEncodings.add(TSEncoding.PLAIN);   // 为每个传感器添加编码方式PLAIN。
            compressionTypes.add(CompressionType.UNCOMPRESSED);  // 为每个传感器添加压缩类型UNCOMPRESSED。
        }
    }

    // 主方法，程序的入口点。
    public static void main(String[] args) throws IoTDBConnectionException, InterruptedException, IOException {
        // 记录开始时间。
        long startTime = System.currentTimeMillis();
        // 使用out对象打印日志信息。
        out.println("database="+databaseCount);
        out.println("deviceCount="+deviceCount);
        out.println("sensorCount="+sensorCount);
        out.println("clinetCount="+clinetCount);
        out.println("isAligned="+isAligned);

        // 创建列表，存储时间序列的数据类型、编码和压缩类型。
        List<TSDataType> tsDataTypes = new ArrayList<>(sensorCount);
        List<TSEncoding> tsEncodings = new ArrayList<>(sensorCount);
        List<CompressionType> compressionTypes = new ArrayList<>(sensorCount);
        // 调用getStruct方法填充列表。
        getStruct(tsDataTypes, tsEncodings, compressionTypes);

        // 创建一个固定大小的线程池。
        ExecutorService pool = Executors.newFixedThreadPool(clinetCount);
        // 循环创建数据库和设备。
        for (int j = 0; j < databaseCount; j++) {
            for (int i = 0; i < deviceCount ; i++) {
                // 将SessionClientRunnable2任务提交给线程池执行。
                pool.execute(new SessionClientRunnable2(
                        hostList.get(i%3),  // 根据索引获取主机名。
                        j,                   // 数据库索引。
                        i,                   // 设备索引。
                        sensorCount,         // 传感器数量。
                        isAligned,           // 是否对齐。
                        tsDataTypes,         // 时间序列数据类型列表。
                        tsEncodings,         // 时间序列编码列表。
                        compressionTypes     // 压缩类型列表。
                ));
            }
        }
        // 关闭线程池。
        pool.shutdown();
        // 循环等待所有任务执行结束。
        while (true) {
            if (pool.isTerminated()) {
                // 如果所有子线程都结束了，打印创建时间序列的数量和总耗时，然后退出循环。
                System.out.printf("创建TS %d, 共耗时: %f s", databaseCount*deviceCount*sensorCount, (System.currentTimeMillis()-startTime)/1000.0);
                break;
            }
        }
    }
}

// 定义一个名为SessionClientRunnable2的类，它实现了Runnable接口，因此可以在一个线程中运行。
class SessionClientRunnable2 implements Runnable {
    // 定义会话对象，用于与IoTDB服务器进行交互。
    private Session session;
    // 定义传感器的数量。
    private int sensorCount;
    // 定义数据库索引。
    private int databaseIndex;
    // 定义设备索引。
    private int deviceIndex;
    // 定义是否对齐时间序列数据的标志。
    private boolean isAligned;
    // 定义时间序列数据类型的列表。
    private List<TSDataType> tsDataTypes;
    // 定义时间序列编码方式的列表。
    private List<TSEncoding> tsEncodings;
    // 定义时间序列数据压缩类型的列表。
    private List<CompressionType> compressionTypes;
    // 定义IoTDB服务的主机地址。
    private String host;
    // 注释掉的数据库前缀，可能用于构造数据库的路径。
    // private String databasePrefix;
    // 定义设备的前缀，用于构造时间序列的路径。
    private String devicePrefix = "root.sg1.d_";
    // 定义时间序列的前缀。
    private String tsPrefix = "s_";

    // SessionClientRunnable2类的构造方法，初始化类的属性，并创建与IoTDB服务器的会话。
    public SessionClientRunnable2(String host, int databaseIndex, int deviceIndex, int sensorCount, boolean isAligned, List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes) throws IoTDBConnectionException, IOException {
        this.host = host;
        this.databaseIndex = databaseIndex;
        this.deviceIndex = deviceIndex;
        this.sensorCount = sensorCount;
        this.isAligned = isAligned;
        this.tsDataTypes = tsDataTypes;
        this.tsEncodings = tsEncodings;
        this.compressionTypes = compressionTypes;
        // 创建一个Session对象，用于与IoTDB服务器建立连接。
        session = new Session.Builder().host(host).port(6667).build();
        session.open(false); // 打开会话。
        session.setFetchSize(1000); // 设置会话的获取数据大小。
    }

    // 实现Runnable接口的run方法，定义线程执行的逻辑。
    @Override
    public void run()  {
        try {
            // 创建一个列表，用于存储时间序列的路径。
            List<String> paths = new ArrayList<>(this.sensorCount);
            // 如果是使用对齐的时间序列数据。
            if (this.isAligned) {
                // 为每个传感器创建一个时间序列路径，并添加到列表中。
                for (int i = 0; i < this.sensorCount; i++) {
                    paths.add("s_"+i);
                }
                // 记录开始时间。
                long startTime = System.currentTimeMillis();
                // 创建对齐的时间序列。
                session.createAlignedTimeseries(devicePrefix+deviceIndex, paths, tsDataTypes, tsEncodings, compressionTypes, null);
                // 计算创建时间序列所花费的时间。
                long elapseTime = System.currentTimeMillis()-startTime;
                // 打印线程名称、设备索引、主机地址和花费时间。
                out.println(Thread.currentThread().getName()+" "+ deviceIndex + " "+ this.host+ " cost:"+elapseTime);
            } else {
                // 如果不是使用对齐的时间序列数据，为每个传感器创建一个时间序列路径，并添加到列表中。
                for (int i = 0; i < this.sensorCount; i++) {
                    paths.add(this.devicePrefix+deviceIndex+".s_"+i);
                }
                // 记录开始时间。
                long startTime = System.currentTimeMillis();
                // 创建非对齐的时间序列。
                session.createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, null, null, null);
                // 计算创建时间序列所花费的时间。
                long elapseTime = System.currentTimeMillis()-startTime;
                // 打印线程名称、设备索引、主机地址和花费时间。
                out.println(Thread.currentThread().getName()+" "+ deviceIndex + " "+ this.host+ " cost:"+elapseTime);
            }
        } catch (Exception e) {
            // 如果发生异常，打印线程名称、主机地址、设备索引和异常信息。
            out.println(Thread.currentThread().getName() +" "+this.host +" "+ this.deviceIndex +" " +e);
        } finally {
            // 无论是否发生异常，都尝试关闭会话。
            try {
                session.close();
            } catch (IoTDBConnectionException e) {
                // 如果关闭会话时发生异常，打印线程名称和异常信息。
                out.printf("[%s] %s", Thread.currentThread().getName(), e);
            }
        }
    }
}