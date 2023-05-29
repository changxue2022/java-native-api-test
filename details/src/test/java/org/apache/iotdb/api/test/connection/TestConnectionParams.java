package org.apache.iotdb.api.test.connection;

import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;

public class TestConnectionParams {
    private static ReadConfig config;
    @BeforeClass
    public void beforeClass() throws IOException {
        config = ReadConfig.getInstance();
    }
    // https://issues.apache.org/jira/browse/IOTDB-5932
    @Test
    public void testTimeout() throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
        Session session = new Session.Builder()
                .host(config.getValue("host"))
                .port(Integer.parseInt(config.getValue("port")))
                .username(config.getValue("user"))
                .password(config.getValue("password"))
                .enableRedirection(false)
                .fetchSize(100000)
                .timeOut(1)
                .zoneId(ZoneId.of("GMT+8"))
                .thriftDefaultBufferSize(100)
                .thriftMaxFrameSize(100)
                .build();
        session.open(false);
        String device = "root.sg.d2";
        int maxLength = 100;
        List<String> tsNames = new ArrayList<>(maxLength);
        List<Object> values = new ArrayList<>(maxLength);
        List<TSDataType> dataTypes = new ArrayList<>(maxLength);
        for (int i = 0; i < maxLength; i++) {
            tsNames.add("s_int_"+i);
            dataTypes.add(TSDataType.INT32);
            values.add(i);
        }

        for (int i = 0; i < 1000000; i++) {
            session.insertRecord(device, i, tsNames, dataTypes, values);
        }
        long start = System.currentTimeMillis();
        SessionDataSet records = session.executeQueryStatement("select * from "+device);
        long end = System.currentTimeMillis();
        while (records.hasNext()) {
            out.println(records.next());
        }
        out.println("elapse time:"+(end-start));
        session.close();
    }
}
