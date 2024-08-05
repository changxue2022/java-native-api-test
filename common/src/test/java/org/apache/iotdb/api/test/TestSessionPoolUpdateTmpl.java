package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.out;

//iotdb-5233
public class TestSessionPoolUpdateTmpl {
    private static SessionPool session = new SessionPool.Builder()
                                        .host("iotdb-45")
                                        .port(6667)
                                        .user("root")
                                        .password("root")
                                        .maxSize(3)
                                        .build();
    private static final Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    private static final List<MeasurementSchema> schemaList = new ArrayList<>(7);// tablet
   static {
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY});
        structureInfo.forEach((key,value)-> {
            schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
    }

    public void test5233() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String templateName = "`IoTDB-5233`";
        Template template = new Template(templateName, true);

        structureInfo.forEach((key, value) -> {
            MeasurementNode mNode =
                    new MeasurementNode(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
            try {
                template.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        session.createSchemaTemplate(template);
        // iotdb-5233 not supported
//        session.addAlignedMeasurementInTemplate(templateName,
//                "223", TSDataType.INT32, TSEncoding.GORILLA, CompressionType.SNAPPY);


    }
    private static void insertTablet (String device) throws
            IOException, IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 1);
        int rowIndex = 0;
        rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, System.currentTimeMillis());
        for (int i = 0; i < schemaList.size(); i++) {
            switch (schemaList.get(i).getType()) {
                case BOOLEAN:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getBoolean());
                    break;
                case INT32:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getInt());
                    break;
                case INT64:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getLong(10));
                    break;
                case FLOAT:
//                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getFloat(2, 1000, 20000));
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Float.valueOf(GenerateValues.getDouble(2, 1000, 20000)+""));
                    break;
                case DOUBLE:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getDouble(3, 40000, 60000));
                    break;
                case TEXT:
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getChinese());
                    break;
                }
        }
        session.insertAlignedTablet(tablet);
//        assert expectCount - 1 == getRecordCount(device, true) : "插入record数目";
    }
    private static void prepare5240(int maxBind, boolean createTemplate) throws IoTDBConnectionException, IOException, StatementExecutionException {
        String templateName = "`IoTDB-5240`";
        Template template = new Template(templateName, true);
        if (createTemplate) {
            structureInfo.forEach((key, value) -> {
                MeasurementNode mNode =
                        new MeasurementNode(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
                schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
                try {
                    template.addToTemplate(mNode);
                } catch (StatementExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.println(templateName);

            session.createSchemaTemplate(template);
            session.createDatabase("root.iotdb5240");
        }
        String baseNode = "root.iotdb5240.";
        for (int i = 0; i < maxBind; i++) {
            String device = baseNode+GenerateValues.getString(4)+"_"+i;
            System.out.println(device);
            session.setSchemaTemplate(templateName, device);
            insertTablet(device);
        }
    }

    private static void test5112() throws IoTDBConnectionException, StatementExecutionException, IOException, InterruptedException {
       String database = "root.iotdb5112";
       String device = database + ".d5112d";
       session.createDatabase(database);
        for (int i = 0; i < 10; i++) {
            insertTablet(device);
            Thread.sleep(13000);
        }
    }
    public static void main(String[] args) throws IoTDBConnectionException, IOException, StatementExecutionException, InterruptedException {
//        prepare5240(1000, false);
//
        // verify 5240
//        String[] deviceList = new String[]{"root.iotdb5240.1z3a_992" ,
//                "root.iotdb5240.iGJR_993" ,
//                "root.iotdb5240._uRy_994" ,
//                "root.iotdb5240.U0V0_995"};
//
//        for (String d: deviceList) {
//            insertTablet(d);
//        }
        test5112();
    }

    public static class TestTmp extends BaseTestSuite {
        private static final String database = "root.manualTest";
        private static final int maxlen = 1000;
        @Test
        public void test() throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
            session.createDatabase(database);
            Thread.sleep(10000);
            String path = database+".d1.sensor_";
            for (int i = 0; i < maxlen; i++) {
                insertRecordSingle(path+i, TSDataType.INT32, false, GenerateValues.getCombinedCode());
    //            Thread.sleep(10000);
            }
        }

    //    @Test
        public void test222() throws IoTDBConnectionException, StatementExecutionException {
    //        IoTDB> select last(*) from root.ln.wf01.wt01
    //                +-----------------------------+-----------------------------+-----+--------+
    //                |                         Time|                   Timeseries|Value|DataType|
    //                +-----------------------------+-----------------------------+-----+--------+
    //                |1970-01-01T08:00:00.001+08:00|root.ln.wf01.wt01.temperature|  2.6|   FLOAT|
    //|1970-01-01T08:00:00.001+08:00|     root.ln.wf01.wt01.status| true| BOOLEAN|
    //                +-----------------------------+-----------------------------+-----+--------+
    //                        Total line number = 2
    //        It costs 0.016s
    //        IoTDB> select * from root.ln.wf01.wt01;
    //        +-----------------------------+-----------------------------+------------------------+
    //                |                         Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
    //                +-----------------------------+-----------------------------+------------------------+
    //                |1970-01-01T08:00:00.001+08:00|                          2.6|                    true|
    //                +-----------------------------+-----------------------------+------------------------+
            String sql = "select last(*) from root.ln.wf01.wt01";
            SessionDataSet dataSet = session.executeQueryStatement(sql);
            while (dataSet.hasNext()) {
                RowRecord records = dataSet.next();
                out.println(records.getFields().get(0).getStringValue());
                out.println(records.getFields().get(1).getStringValue());
                // useless, return the default initialized value
                out.println(records.getFields().get(2).getStringValue());
    //            out.println(records.getFields().get(1).toString());
            }
        }


    }
}
