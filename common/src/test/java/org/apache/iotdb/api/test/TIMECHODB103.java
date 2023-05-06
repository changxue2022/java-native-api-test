package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;

import java.io.IOException;
import java.util.*;

public class TIMECHODB103 {
    private static Session session = null;
    private static Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    private static List<MeasurementSchema> schemaList_org = new ArrayList<>(6);
    private static List<MeasurementSchema> schemaList_err = new ArrayList<>(6);
    private static List<MeasurementSchema> schemaList_more = new ArrayList<>();
    private static List<MeasurementSchema> schemaList_less = new ArrayList<>();
    private static List<String> devicePaths = new ArrayList<>();
    private static String databasePrefix = "root.db.factory";
    private static String templatePrefix = "template0";
    private static boolean isAligned = true;
    private static boolean verbose = true;


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, IOException {
        session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .enableRedirection(false).build();
        session.open(false);
        // set session fetchSize
        session.setFetchSize(10000);
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});

        structureInfo.forEach((key, value) -> {
            schemaList_org.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding)value[1], (CompressionType)value[2]));
        });
        for (int i = schemaList_org.size()-1; i >0 ; i--) {
            schemaList_less.add(schemaList_org.get(i));
            schemaList_err.add(schemaList_org.get(i));
        }
        schemaList_err.add(new MeasurementSchema("s_boolean", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY));


        for (int i = 0; i < schemaList_org.size(); i++) {
            schemaList_more.add(schemaList_org.get(i));
        }
        List<Object> structs = new ArrayList<>();
        structs.add(TSDataType.BOOLEAN);
        structs.add(TSEncoding.PLAIN);
        structs.add(CompressionType.UNCOMPRESSED);
        schemaList_more.add(new MeasurementSchema("s_append" + 0,
                (TSDataType) structs.get(0), (TSEncoding) structs.get(1),
                (CompressionType) structs.get(2)));
        // 创建template
        String templateName = templatePrefix + "1";

        // database 用于主要业务，database2用于清理，database3用于普通序列的测试
        List<String> databases = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            databases.add(databasePrefix + i);
            session.createDatabase(databases.get(i));
        }
        String database = databases.get(0);
        String device = "";
        int expectTSCountEach = 0;
        Template template = new Template(templateName, isAligned);
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

        // 写入数据 root.db.factory1.d0.group0, 成功，未使用模版
        device = database + ".d0.group0";
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        devicePaths.add(device);

        expectTSCountEach = schemaList_org.size();
        // 写入成功，使用了模版
        device = database + ".d1.group1";
        devicePaths.add(device);
        session.setSchemaTemplate(templateName, device);
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        Assert.assertThrows(StatementExecutionException.class, () -> {
            insertTabletMulti(database + ".d1.group1", schemaList_org, 10, !isAligned);
        });
        // 写入少于模版的列的数据
        device = database + ".d1.group1.subGroup";
        devicePaths.add(device);
        insertTabletMulti(device, schemaList_less, 10, isAligned);

        // 写入多于模版列的数据
        expectTSCountEach++;
        device = database + ".d2.group1";
        session.setSchemaTemplate(templateName, device);
        insertTabletMulti(device, schemaList_more, 10, isAligned);

        devicePaths.add(device);
        // 写入同名不同类型列的数据
        Assert.assertThrows(StatementExecutionException.class, () -> {
            insertTabletMulti(database + ".d2.group1", schemaList_err, 10, isAligned);
        });

        // 激活
        List<String> paths = new ArrayList<>(2);
        paths.add(databasePrefix + "1" + ".d1.group3");
        for (int i = 0; i < 2; i++) {
            paths.add(databasePrefix + i + ".d1.group3");
            session.setSchemaTemplate(templateName, paths.get(i));
        }
        // 激活
        session.createTimeseriesUsingSchemaTemplate(paths);
        device = paths.get(0);
        devicePaths.add(device);
        // 写入数据
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        // 写入少列数据
        insertTabletMulti(device, schemaList_less, 10, isAligned);

        device = paths.get(1);
        devicePaths.add(device);
        // 写入少列数据
        insertTabletMulti(device, schemaList_less, 10, isAligned);
        // 写入数据
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        // 写入多列不同类型数据
        Assert.assertThrows(StatementExecutionException.class, () -> {
            insertTabletMulti(paths.get(0), schemaList_err, 10, isAligned);
        });
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            insertTabletMulti(paths.get(0), schemaList_more, 10, isAligned);
//        });
        paths.clear();
        paths.add(database + ".d1.group2");
        paths.add(database + ".d3.group3");
        for (int i = 0; i < paths.size(); i++) {
            session.setSchemaTemplate(templateName, paths.get(i));
            devicePaths.add(paths.get(i));
        }

        // explicit追加TS
        addTSIntoTemplate(templateName, "s_append_explicit",
                (TSDataType) structs.get(0), (TSEncoding) structs.get(1),
                (CompressionType) structs.get(2));
        schemaList_more.add(new MeasurementSchema("s_append_explicit", (TSDataType) structs.get(0),
                (TSEncoding) structs.get(1), (CompressionType) structs.get(2)));

        insertTabletMulti(device, schemaList_more, 10, isAligned);
        insertTabletMulti(device, schemaList_org, 10, isAligned);
        insertTabletMulti(device, schemaList_less, 10, isAligned);
        for (int i = 0; i < 2; i++) {
            device = paths.get(i);
//                assert expectTSCountEach == getTimeSeriesCount(device+".*", verbose) : "激活后，确定模版内TS数量:"+expectTSCountEach;
            insertTabletMulti(device, schemaList_more, 10, isAligned);
            insertTabletMulti(device, schemaList_org, 10, isAligned);
            insertTabletMulti(device, schemaList_less, 10, isAligned);
        }

        for (int i = 1; i < devicePaths.size(); i++) {
            System.out.println(devicePaths.get(i));
            insertTabletMulti(devicePaths.get(i), schemaList_less, 20, isAligned);
        }
        // 删除部分数据
        for (int i = 0; i < devicePaths.size(); i++) {
            System.out.println(devicePaths.get(i));
            session.deleteData(devicePaths.get(i) + ".*", 3);
        }
        session.executeNonQueryStatement("delete timeseries " + devicePaths.get(0) + ".*");
        insertTabletMulti(devicePaths.get(1), schemaList_org, 20, verbose);
        // 有引用，卸载
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.unsetSchemaTemplate(database, templateName);
        });
        // 有引用，删除模版
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.dropSchemaTemplate(templateName);
        });
        // 解除未使用模版序列
        Assert.assertThrows(StatementExecutionException.class, () -> {
            deactiveTemplate(templateName, devicePaths.get(0));
        });
        // 解除某节点
        device = devicePaths.get(1);
        deactiveTemplate(templateName, device);

        // 解除所有节点
        devicePaths.remove(0);
        devicePaths.remove(1);
        // TIMECHODB-103
//        deactiveTemplate(templateName, devicePaths);
        // TIMECHODB-102
//        deactiveTemplate(templateName, "root.db.**");
//        assert 0 == getSetPathsCount(templateName, verbose) : "模版引用数目 == 0";
//        session.unsetSchemaTemplate(database, templateName);
        session.deleteDatabase(database);
        session.createDatabase(database);
        session.dropSchemaTemplate(templateName);
//        Assert.assertThrows(StatementExecutionException.class, ()->{
//            session.unsetSchemaTemplate(database, templateName);
//        });
        session.deleteDatabase(databases.get(1));

        schemaList_more.clear();
    }
    public static void addTSIntoTemplate(String templateName, String tsName, TSDataType tsDataType, TSEncoding tsEncoding, CompressionType compressionType) throws IoTDBConnectionException, StatementExecutionException {
        StringJoiner sb = new StringJoiner(" ");
        sb.add("alter schema template ");
        sb.add(templateName);
        sb.add(" add (");
        sb.add(tsName);
        sb.add(tsDataType.toString());
        sb.add("encoding=");
        sb.add(tsEncoding.toString());
        sb.add("compression=");
        sb.add(compressionType.toString());
        sb.add(");");
        session.executeNonQueryStatement(sb.toString());
    }

    public static void addTSIntoTemplate(String templateName, List<String> tsNameList, List<TSDataType> tsDataTypeList, List<TSEncoding> tsEncodingList, List<CompressionType> compressionTypeList) throws IoTDBConnectionException, StatementExecutionException {
        StringJoiner sb = new StringJoiner(" ");
        sb.add("alter schema template ");
        sb.add(templateName);
        sb.add(" add (");
        for (int i = 0; i < tsNameList.size(); i++) {
            if (i > 0) {
                sb.add(",");
            }
            sb.add(tsNameList.get(i));
            sb.add(tsDataTypeList.get(i).toString());
            sb.add("encoding=");
            sb.add(tsEncodingList.get(i).toString());
            sb.add("compression=");
            sb.add(compressionTypeList.get(i).toString());
        }
        sb.add(");");
        session.executeNonQueryStatement(sb.toString());
    }
    public static void insertTabletMulti(String device, List<MeasurementSchema> schemaList, int insertCount, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, insertCount);
        int rowIndex = 0;
        for (int row = 0; row < insertCount; row++) {
            rowIndex = tablet.rowSize++;
//            System.out.println("row="+row+" rowIndex="+rowIndex);
//            tablet.addTimestamp(rowIndex, System.currentTimeMillis());
            tablet.addTimestamp(rowIndex, row);
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
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getFloat(2, 100, 200));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getDouble(2, 500, 1000));
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, GenerateValues.getChinese());
                        break;
                }
            }
        }
        if (isAligned) {
            session.insertAlignedTablet(tablet);
        } else {
            session.insertTablet(tablet);
        }
    }

    public static void deactiveTemplate(String templateName, String path) throws IoTDBConnectionException, StatementExecutionException {
        // delete timeseries of schema template t1 from root.sg1.d1
        // deactivate schema template t1 from root.sg1.d1
        session.executeNonQueryStatement("deactivate schema template " + templateName + " from " + path);
    }

}
