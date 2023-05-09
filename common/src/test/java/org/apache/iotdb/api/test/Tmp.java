package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static java.lang.System.out;

public class Tmp extends BaseTestSuite {
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
//    @BeforeClass
    public void BeforeClass() {
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
    }

//    @Test
    public void test2() throws IoTDBConnectionException, StatementExecutionException, IOException {
        boolean isAligned = true;
        String templateName = "t1";
        String database = "root.test";
        Template t1 = new Template(templateName, isAligned);
        structureInfo.forEach((key, value) -> {
            MeasurementNode mNode =
                    new MeasurementNode(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
            try {
                t1.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        session.createSchemaTemplate(t1);
        session.setSchemaTemplate(templateName, database);

        List<String> paths = new ArrayList<>(1);
        paths.add(database + ".d_0");
        session.executeQueryStatement("count timeseries " + paths.get(0)+".**");
        session.createTimeseriesUsingSchemaTemplate(paths);
        session.executeQueryStatement("count timeseries " + paths.get(0)+".**");

        session.deleteDatabase(database);
        session.dropSchemaTemplate(templateName);
    }
    // TIMECHODB-124
    @Test
    public void test() throws IoTDBConnectionException, StatementExecutionException {
        Map<String,String> props = new HashMap<>();
        props.put("Prop1", "3");
        session.createTimeseries("root.sg.d.s_name", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, props, null, null, null);
    }


}
