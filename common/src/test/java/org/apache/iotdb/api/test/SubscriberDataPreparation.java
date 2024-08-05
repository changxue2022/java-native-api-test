package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于为IoTDB数据库测试准备数据
 */
public class SubscriberDataPreparation extends BaseTestSuite {
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);

    private String database;
    private String device;
    private Template template;
    private List<String> paths = new ArrayList<>(6);
    private List<String> aliasList = new ArrayList<>(6);
    private List<TSDataType> dataTypes = new ArrayList<>(6);
    private List<TSEncoding> encodings = new ArrayList<>(6);
    private List<CompressionType> compressionTypes = new ArrayList<>(6);

    private List<List<Object>> structures;

    @BeforeClass
    public void beforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        cleanDatabases(true);
        cleanTemplates(true);
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});

        structures = new CustomDataProvider().parseTSStructure("data/ts-structures.csv");
    }

    @Test(priority = 10, groups = {"unAligned", "all"})
    public void nonAlignedData() throws IoTDBConnectionException, StatementExecutionException, IOException {
        isAligned = false;
        database = "root.nonAligned";
        session.createDatabase(database);

        TSDataType dataType = TSDataType.FLOAT;
        device = database + ".1TS";
        String path = device + ".s_float";
        session.createTimeseries(path, dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        insertRecordSingle(path, dataType, isAligned, "FLOAT_alias");

        device = database + ".`1(TS)`";
        path = device + ".`1`";
        List<MeasurementSchema> schemas_1 = new ArrayList<>(1);
        schemas_1.add(new MeasurementSchema("`1`", dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        session.createTimeseries(path, dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        insertTabletMulti(device, schemas_1, 100, isAligned);
        schemas_1.clear();

        device = database + ".6TS";
        List<MeasurementSchema> schemas_6 = new ArrayList<>(6);
        structureInfo.forEach((key, value) -> {
            paths.add(device + "." + key);
            aliasList.add(key);
            dataTypes.add((TSDataType) value[0]);
            encodings.add((TSEncoding) value[1]);
            compressionTypes.add((CompressionType) value[2]);
            schemas_6.add(new MeasurementSchema("`" + key + "_(1)`", (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes, null, null, null, aliasList);
        insertRecordMulti(device, aliasList, dataTypes, 10, isAligned, aliasList);

        device = device + ".`6`";
        paths.clear();
        structureInfo.forEach((key, value) -> {
            paths.add(device + ".`" + key + "_(1)`");
        });
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes, null, null, null, aliasList);
        insertTabletMulti(device, schemas_6, 1000, isAligned);
        schemas_6.clear();

        device = database + ".100TS";
        int max = 1000;
        List<MeasurementSchema> schemas = new ArrayList<>(max);
        for (int i = 0; i < structures.size(); i++) {
            session.createTimeseries(device + ".s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2));
            schemas.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        insertTabletMulti(device, schemas, 10000, isAligned);

        device = database + ".1000TS";
        for (int i = structures.size(); i < max; i++) {
            int j = i % structures.size();
            session.createTimeseries(device + ".s_" + i, (TSDataType) structures.get(j).get(0),
                    (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2));
            schemas.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(j).get(0),
                    (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2)));
        }
        insertTabletMulti(device, schemas, 100, isAligned);
    }

    @Test(priority = 20, groups = {"aligned", "all"})
    public void alignedData() throws IoTDBConnectionException, StatementExecutionException, IOException {
        isAligned = true;
        database = "root.aligned";
        session.createDatabase(database);

        List<MeasurementSchema> schemas = new ArrayList<>();
        TSDataType dataType = TSDataType.FLOAT;

        dataTypes.clear();
        encodings.clear();
        compressionTypes.clear();
        aliasList.clear();

        device = database + ".1TS";
        String path = device + ".s_float";
        aliasList.add("s_float");
        dataTypes.add(dataType);
        encodings.add(TSEncoding.PLAIN);
        compressionTypes.add(CompressionType.UNCOMPRESSED);
        session.createAlignedTimeseries(device, aliasList, dataTypes, encodings, compressionTypes, null);
        insertRecordSingle(path, dataType, isAligned, "FLOAT_alias");

        device = database + ".`1(TS)`";
        aliasList.clear();
        aliasList.add("`1`");
        schemas.add(new MeasurementSchema("`1`", dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        session.createAlignedTimeseries(device, aliasList, dataTypes, encodings, compressionTypes, aliasList);
        insertTabletMulti(device, schemas, 100, isAligned);

        schemas.clear();
        aliasList.clear();
        dataTypes.clear();
        encodings.clear();
        compressionTypes.clear();

        device = database + ".6TS";
        structureInfo.forEach((key, value) -> {
            aliasList.add(key);
            dataTypes.add((TSDataType) value[0]);
            encodings.add((TSEncoding) value[1]);
            compressionTypes.add((CompressionType) value[2]);
            schemas.add(new MeasurementSchema("`" + key + "_(1)`", (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
        session.createAlignedTimeseries(device, aliasList, dataTypes, encodings, compressionTypes, aliasList);
        insertRecordMulti(device, aliasList, dataTypes, 10, isAligned, aliasList);

        device = device + ".`6`";
        aliasList.clear();
        structureInfo.forEach((key, value) -> {
            aliasList.add("`" + key + "_(1)`");
        });
        session.createAlignedTimeseries(device, aliasList, dataTypes, encodings, compressionTypes, aliasList);
        insertTabletMulti(device, schemas, 1000, isAligned);
        schemas.clear();


        aliasList.clear();
        dataTypes.clear();
        encodings.clear();
        compressionTypes.clear();
        schemas.clear();

        device = database + ".100TS";
        int max = 1000;
        for (int i = 0; i < structures.size(); i++) {
            aliasList.add("s_" + i);
            dataTypes.add((TSDataType) structures.get(i).get(0));
            encodings.add((TSEncoding) structures.get(i).get(1));
            compressionTypes.add((CompressionType) structures.get(i).get(2));
            schemas.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        session.createAlignedTimeseries(device, aliasList, dataTypes, encodings, compressionTypes, aliasList);
        insertTabletMulti(device, schemas, 10000, isAligned);

        device = database + ".1000TS";
        for (int i = structures.size(); i < max; i++) {
            int j = i % structures.size();
            aliasList.add("s_" + i);
            dataTypes.add((TSDataType) structures.get(j).get(0));
            encodings.add((TSEncoding) structures.get(j).get(1));
            compressionTypes.add((CompressionType) structures.get(j).get(2));

            schemas.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(j).get(0),
                    (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2)));
        }
        insertTabletMulti(device, schemas, 100, isAligned);
    }

    @Test(priority = 40, groups = {"alignedTemplate", "all"})
    public void alignedTemplateData() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String databasePrefix = "root.template.aligned";
        String templatePrefix = "template_aligned_";
        generateTemplateData(databasePrefix, templatePrefix, true);

    }

    @Test(priority = 60, groups = {"unAlignedTemplate", "all"})
    public void unAlignedTemplateData() throws IoTDBConnectionException, IOException, StatementExecutionException {
        String databasePrefix = "root.template.nonAligned";
        String templatePrefix = "template_nonAligned_";
        generateTemplateData(databasePrefix, templatePrefix, false);

    }


    //    @Test(priority = 70, groups = {"blend", "all"})
    public void blendScenario() throws IoTDBConnectionException, StatementExecutionException, IOException {
        String database = "root.blend.maxLength64_maxLength64_maxLength64_maxLength64_maxLe";
        session.createDatabase(database);

        List<String> devicePaths = new ArrayList<>();
        List<MeasurementSchema> schemas = new ArrayList<>();

        // 对齐序列
        TSDataType dataType = TSDataType.TEXT;
        isAligned = true;
        String tsName = "s_text";
        device = database + ".aligned.1TS";
        String path = device + "." + tsName;
        aliasList.add(tsName);
        dataTypes.add(dataType);
        encodings.add(TSEncoding.PLAIN);
        compressionTypes.add(CompressionType.UNCOMPRESSED);
        session.createAlignedTimeseries(device, aliasList, dataTypes, encodings, compressionTypes, null);
        insertRecordSingle(path, dataType, isAligned, "text_alias");

        String templateName = "blendAligned_1TS_template";
        template = new Template(templateName, isAligned);
        template.addToTemplate(new MeasurementNode(tsName, dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        session.createSchemaTemplate(template);
        devicePaths.clear();
        devicePaths.add(database + ".aligned.template.singleTS");
        devicePaths.add(database + ".aligned.template.`TS#1`");
        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
        }
        devicePaths.add(database + ".aligned.template.singleTS.`1`");
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletSingle(devicePaths.get(i), tsName, dataType, 10, isAligned);
        }
        devicePaths.clear();
        templateName = "blendAligned_6TS_template";
        template = new Template(templateName, isAligned);
        structureInfo.forEach((key, value) -> {
            try {
                template.addToTemplate(new MeasurementNode("`" + key + "_(1)`", (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
            schemas.add(new MeasurementSchema("`" + key + "_(1)`", (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
        session.createSchemaTemplate(template);
        devicePaths.add(database + ".aligned.template.6TS");
        devicePaths.add(database + ".aligned.template.`TS#6`");

        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
        }
        devicePaths.add(database + "..aligned.template.`TS#6`.`6(TS)`");
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletMulti(devicePaths.get(i), schemas, 1000, isAligned);
        }

        // 非对齐
        isAligned = false;
        dataType = TSDataType.INT64;
        device = database + ".nonAligned.singleTS";
        tsName = "`s_long(1)`";
        path = device + "." + tsName;
        session.createTimeseries(path, dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        insertRecordSingle(path, dataType, isAligned, "long_alias");

        templateName = "blendNonAligned_1TS_template";
        template = new Template(templateName, isAligned);
        template.addToTemplate(new MeasurementNode(tsName, dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        session.createSchemaTemplate(template);
        devicePaths.clear();
        devicePaths.add(database + ".nonAligned.template.1TS");
        devicePaths.add(database + ".nonAligned.template.`TS#1`");
        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
        }
        devicePaths.add(database + ".nonAligned_1TS.template.`TS#1`.`1`");
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletSingle(devicePaths.get(i), tsName, dataType, 10, isAligned);
        }
    }

    private void generateTemplateData(String databasePrefix, String templatePrefix, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException, IOException {
        List<String> devicePaths = new ArrayList<>(10);
        List<MeasurementSchema> schemas = new ArrayList<>();

        String database = databasePrefix + ".template";
        List<String> databases = new ArrayList<>();
        databases.add(databasePrefix);
        databases.add(databasePrefix + "_1TS");
        databases.add(databasePrefix + "_6TS");
        databases.add(databasePrefix + "_100TS");
        databases.add(databasePrefix + "_1000TS");
        for (int i = 0; i < databases.size(); i++) {
            session.createDatabase(databases.get(i));
        }

        String templateName = templatePrefix + "1TS";
        template = new Template(templateName, isAligned);
        template.addToTemplate(new MeasurementNode("s_boolean", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        session.createSchemaTemplate(template);
        devicePaths.clear();
        devicePaths.add(database + ".1TS");
        devicePaths.add(databasePrefix + "_1TS");
        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
        }
        devicePaths.add(database + ".1TS.`1`");
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletSingle(devicePaths.get(i), "s_boolean", TSDataType.BOOLEAN, 10, isAligned);
        }

        devicePaths.clear();
        templateName = templatePrefix + "_6TS";
        template = new Template(templateName, isAligned);
        structureInfo.forEach((key, value) -> {
            try {
                template.addToTemplate(new MeasurementNode("`" + key + "_(1)`", (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
            schemas.add(new MeasurementSchema("`" + key + "_(1)`", (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
        });
        session.createSchemaTemplate(template);
        devicePaths.add(database + ".6TS");
        devicePaths.add(databasePrefix + "_6TS");

        for (int i = 0; i < devicePaths.size(); i++) {
            session.setSchemaTemplate(templateName, devicePaths.get(i));
        }
        devicePaths.add(database + ".6TS.`6(TS)`");
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        for (int i = 0; i < devicePaths.size(); i++) {
            insertTabletMulti(devicePaths.get(i), schemas, 1000, isAligned);
        }

        templateName = templatePrefix + "100TS";
        schemas.clear();
        devicePaths.clear();
        template = new Template(templateName, isAligned);
        for (int i = 0; i < structures.size(); i++) {
            template.addToTemplate(new MeasurementNode("s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
            schemas.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        devicePaths.add(database + ".100TS");
        session.createSchemaTemplate(template);
        session.setSchemaTemplate(templateName, devicePaths.get(0));
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        insertTabletMulti(devicePaths.get(0), schemas, 1000, isAligned);

        templateName = templatePrefix + "1000TS";
        int max = 1000;
        schemas.clear();
        devicePaths.clear();
        template = new Template(templateName, isAligned);
        for (int i = 0; i < max; i++) {
            int j = i % structures.size();
            template.addToTemplate(new MeasurementNode("s_" + i, (TSDataType) structures.get(j).get(0),
                    (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2)));
            schemas.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(j).get(0),
                    (TSEncoding) structures.get(j).get(1), (CompressionType) structures.get(j).get(2)));
        }
        devicePaths.add(database + ".1000TS");
        session.createSchemaTemplate(template);
        session.setSchemaTemplate(templateName, devicePaths.get(0));
        session.createTimeseriesUsingSchemaTemplate(devicePaths);
        insertTabletMulti(devicePaths.get(0), schemas, 100, isAligned);
    }

}
