package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;

public class SessionClientCreateRunnable implements Runnable {
    private SessionPool session;
    private String prefix;
    private boolean isAligned;
    private List<TSDataType> tsDataTypes;
    private List<TSEncoding> tsEncodings;
    private List<CompressionType> compressionTypes;
    private List<String> paths;
    public SessionClientCreateRunnable(SessionPool session, String prefix, boolean isAligned,
                                       List<TSDataType> tsDataTypes, List<TSEncoding> tsEncodings, List<CompressionType> compressionTypes) throws IoTDBConnectionException, IOException {
        this.session = session;
        this.prefix = prefix;
        this.isAligned = isAligned;
        this.tsDataTypes = tsDataTypes;
        this.tsEncodings = tsEncodings;
        this.compressionTypes = compressionTypes;
        int sensorCount = tsDataTypes.size();
        paths = new ArrayList<>(sensorCount);
        for (int i = 0; i < sensorCount; i++) {
            paths.add(prefix+i);
        }
//        out.println("##############");
//        out.println("deviceFromIndex="+deviceFromIndex);
//        out.println("deviceCount="+deviceCount);
//        out.println("##############");
    }
    @Override
    public void run()  {
        try {
            long startTime = System.currentTimeMillis();
            session.createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, null, null, null);
            long elapseTime = System.currentTimeMillis()-startTime;
            out.println(Thread.currentThread().getName()+" prefix="+ this.prefix +" cost:"+elapseTime);
        } catch (Exception e) {
            out.println(Thread.currentThread().getName()+" prefix=" + this.prefix +" " +e);
        }
    }
}