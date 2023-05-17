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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
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
//    @Test
    public void test() throws IoTDBConnectionException, StatementExecutionException {
        Map<String,String> props = new HashMap<>();
        props.put("Prop1", "3");
        session.createTimeseries("root.sg.d.s_name", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, props, null, null, null);
    }
//    @Test
    public void test111() throws IOException {
        Iterator<Object[]> i = new CustomDataProvider().load("data/timeseries-multi.csv").getData();
        while (i.hasNext()) {
            for(Object r: i.next()) {
                out.println(r.toString());
            }
            out.println("--------------");
        }
    }
    @Test
    public void testAligned() throws IoTDBConnectionException, StatementExecutionException {
        String device = "root.sg.d1.max.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUV.d";
        List<String> tsList = new ArrayList<>(1);
        List<String> alias = new ArrayList<>(1);
        List<TSDataType> tsDataTypes = new ArrayList<>(1);
        List<TSEncoding> tsEncodings = new ArrayList<>(1);
        List<CompressionType> compressionTypes = new ArrayList<>(1);
        tsList.add("s_text1");
        tsDataTypes.add(TSDataType.TEXT);
        tsEncodings.add(TSEncoding.PLAIN);
        compressionTypes.add(CompressionType.UNCOMPRESSED);
        alias.add(null);
        session.createAlignedTimeseries(device,tsList, tsDataTypes,tsEncodings,compressionTypes,alias);
    }
    private List<MeasurementSchema> schemaList = new ArrayList<>(7);// tablet

    private void createTemplate (String templateName, String loadNode, boolean isAligned) throws
            IoTDBConnectionException, StatementExecutionException, IOException {
//        int templateCount = countLines("show schema templates", true);
        Template template = new Template(templateName, isAligned);

        structureInfo.forEach((key, value) -> {
            schemaList.add(new MeasurementSchema(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]));
            MeasurementNode mNode =
                    new MeasurementNode(key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
            try {
                template.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        session.createSchemaTemplate(template);
        countLines("show schema templates", verbose);
        if (!loadNode.isEmpty()) {
            session.setSchemaTemplate(templateName, loadNode);
            assert checkTemplateContainPath(templateName, loadNode) : "挂载模版成功";
            getSetPathsCount(templateName, verbose);
            insertTabletMulti(loadNode, schemaList, 10, isAligned);
            countLines("show timeseries "+loadNode+".**", verbose);
            getRecordCount(loadNode, verbose);
        }
    }
    @Test
    public void test232() throws IoTDBConnectionException, IOException, StatementExecutionException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
        session.createDatabase("root.template");
//        String name = "`123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789.12345678912345678912345678912345678912345678912345678912345678912345678912345678912356`";
        String name = "c21";
        String loadNode = "root.template."+name;
        createTemplate(name, loadNode, isAligned);
    }
}
