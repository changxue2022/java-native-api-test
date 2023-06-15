package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.System.out;

public class Tmp extends BaseTestSuite {
    private Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    Logger logger = Logger.getLogger(Tmp.class);

    @BeforeClass
    public void BeforeClass() throws IoTDBConnectionException, StatementExecutionException {
        logger.warn("########### Tmp BeforeClass ####");
        structureInfo.put("s_boolean", new Object[]{TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_int", new Object[]{TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_long", new Object[]{TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_float", new Object[]{TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_double", new Object[]{TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});
        structureInfo.put("s_text", new Object[]{TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED});


    }
    @AfterClass
    public void afterClass () {
        logger.warn("###### Tmp afterClass #####");
    }

//    @Test
    public void test82() throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths=new ArrayList<>();

        paths.add("root.test.single");
        session.createTimeseriesUsingSchemaTemplate(paths);
    }

    // TIMECHODB-124
//    @Test
    public void test() throws IoTDBConnectionException, StatementExecutionException {
        Map<String,String> props = new HashMap<>();
        props.put("Prop1", "3");
        session.createTimeseries("root.sg.d.s_name", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED, props, null, null, null);
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
    public void test3() throws IOException, ParseException {
        String mydate = "2022-12-31 23:59:56";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(format.parse(mydate).getTime());
    }


}
