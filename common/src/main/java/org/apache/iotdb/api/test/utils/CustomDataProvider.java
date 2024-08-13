package org.apache.iotdb.api.test.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.jetbrains.annotations.NotNull;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 读取csv文件，返回除第一行header外的数据，供给给测试方法使用
 * Author: xue.chang@timecho.com
 */
public class CustomDataProvider {
    private Logger logger = Logger.getLogger(CustomDataProvider.class);
    private List<Object[]> testCases = new ArrayList<>();
    private Reader reader;

    public Iterable<CSVRecord> readCSV(String filepath, char delimiter) throws IOException {
//        String path = CustomDataProvider.class.getClassLoader().getResource(filepath).getPath();
//        if (path.charAt(0) == '/') {
//            path = path.substring(1);
//        }
//        logger.info("read csv:" + path);
//        this.reader = Files.newBufferedReader(Paths.get(path));
        logger.info("read csv:"+CustomDataProvider.class.getClassLoader().getResource(filepath).getPath());
        this.reader = Files.newBufferedReader(Paths.get(CustomDataProvider.class.getClassLoader().getResource(filepath).getPath()));
        CSVFormat csvformat = CSVFormat.DEFAULT.withDelimiter(delimiter).withEscape('\\').withQuote('"').withIgnoreEmptyLines(true);
        Iterable<CSVRecord> records = csvformat.parse(reader);
        // 去除header
        records.iterator().next();
        return records;
    }

    /**
     * 处理map
     *
     * @param cols
     * @return
     */
    private Map<String, String> processMapField(@NotNull String cols) {
        if (cols.equals("null")) {
            return null;
        }
        Map<String, String> cols_map = new HashMap<>();
        if (cols.equals("empty")) {
            return cols_map;
        }
        for (String item: cols.split("[|]")) {
            if (item.isEmpty()) {
                continue;
            }
            String[] values = item.split(":");
            if (values.length == 1) {
                cols_map.put(values[0], "");
            } else {
                cols_map.put(values[0], values[1]);
            }
        }
        return cols_map;
    }

    /**
     * 处理list
     * @param cols
     * @return
     */
    private @NotNull List processListField(@NotNull String cols) {
        if (cols.equals("null")) {
            return null;
        } else if (cols.startsWith("m:")) {
            List<Map<String, String>> result = new ArrayList<Map<String, String>>();
            for (String item_l: cols.split(",")) {
                item_l = item_l.substring(2);
                Map<String, String> cols_map = new HashMap<>();
                if (item_l.equals("empty")) {
                    result.add(cols_map);
                } else {
                    for (String item_m: item_l.split("[|]")) {
                        item_m = item_m.substring(2);
                        if (item_m.isEmpty()) {
                            continue;
                        }
                        String[] values = item_m.split(":");
                        cols_map.put(values[0],values[1]);
                    }
                }
            }
            return result;
        } else {
            List<String> result = new ArrayList<>();
            if (!cols.equals("empty")) {
                for (String item_l : cols.split(",")) {
                    result.add(item_l);
                }
            }
            return result;
        }
    }

    /**
     * 解析包括map,list在内的自定义格式
     * @param filepath
     * @param delimiter
     * @return
     * @throws IOException
     */
    public CustomDataProvider load(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV(filepath, delimiter);
        for (CSVRecord record : records) {
            // 解析每一行
            List<Object> columns_arr = new ArrayList<>();
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String cols = record_iter.next();
            boolean breakFlag = false;
            if (!cols.startsWith("#")) {
                while (true) {
                    if (cols.startsWith("m:")) {
                        cols = cols.substring(2);
                        columns_arr.add(processMapField(cols));
                    } else if (cols.startsWith("l:")) {
                        cols = cols.substring(2);
                        columns_arr.add(processListField(cols));
                    } else if (cols.equals("null")) {
                        columns_arr.add(null);
                    } else {
                        columns_arr.add(cols);
                    }
                    if (record_iter.hasNext()) {
                        cols = record_iter.next();
                    } else {
                        breakFlag = true;
                    }
                    if (breakFlag) break;
                }
//                out.println(columns_arr);
                this.testCases.add(columns_arr.stream().toArray());
            }
        }
        return this;
    }

    /**
     * 加载csv,直接返回String类型
     * @param filepath
     * @param delimiter
     * @return
     * @throws IOException
     */
    public List<List<String>> loadString(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV(filepath, delimiter);
        List<List<String>> result = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            List<String> eachLine = new ArrayList<>();
            Iterator<String> recordIter = record.iterator();
            // 如果以#开头，那么跳过
            String firstCols = recordIter.next();
            if (!firstCols.startsWith("#")) {
                eachLine.add(firstCols);
                while(recordIter.hasNext()) {
                    eachLine.add(recordIter.next());
                }
                result.add(eachLine);
            }
        }
        this.reader.close();
        return result;
    }

    /**
     * 固定解析 ts-structures.csv
     * 第一列 TSDataType
     * 第二列 TSEncoding
     * 第三列 CompressionType
     * @param filepath
     * @return
     * @throws IOException
     */
    public List<List<Object>> parseTSStructure(String filepath) throws IOException {
        List<List<Object>> result = new ArrayList<>();
        Iterable<CSVRecord> records = this.readCSV(filepath, ',');
        for (CSVRecord record : records) {
            Iterator<String> recordIter = record.iterator();
            // 如果以#开头，那么跳过
            String firstCols = recordIter.next();
            if (!firstCols.startsWith("#")) {
                // 解析每一行
                List<Object> eachLine = new ArrayList<>();
                eachLine.add(parseDataType(firstCols));
                eachLine.add(parseEncoding(recordIter.next()));
                eachLine.add(parseCompressionType(recordIter.next()));
                eachLine.add(recordIter.next());
                eachLine.add(recordIter.next());
                result.add(eachLine);
                testCases.add(eachLine.toArray());
            }
        }
        this.reader.close();
        return result;
    }

    public TSDataType parseDataType(String datatypeStr) {
        switch (datatypeStr.toLowerCase()) {
            case "boolean":
                return TSDataType.BOOLEAN;
            case "int":
                return TSDataType.INT32;
            case "long":
                return TSDataType.INT64;
            case "float":
                return TSDataType.FLOAT;
            case "double":
                return TSDataType.DOUBLE;
            case "vector":
                return TSDataType.VECTOR;
            case "text":
                return TSDataType.TEXT;
            case "null":
                return null;
            default:
                throw new RuntimeException("bad input");
        }
    }
    public TSEncoding parseEncoding(String encodingStr){
        switch (encodingStr.toUpperCase()) {
            case "PLAIN":
                return TSEncoding.PLAIN;
            case "DICTIONARY":
                return TSEncoding.DICTIONARY;
            case "RLE":
                return TSEncoding.RLE;
            case "DIFF":
                return TSEncoding.DIFF;
            case "TS_2DIFF":
                return TSEncoding.TS_2DIFF;
            case "BITMAP":
                return TSEncoding.BITMAP;
            case "GORILLA_V1":
                return TSEncoding.GORILLA_V1;
            case "REGULAR":
                return TSEncoding.REGULAR;
            case "GORILLA":
                return TSEncoding.GORILLA;
            case "ZIGZAG":
                return TSEncoding.ZIGZAG;
            case "FREQ":
                return TSEncoding.FREQ;
            case "SPRINTZ":
                return TSEncoding.SPRINTZ;
            case "RLBE":
                return TSEncoding.RLBE;
            default:
                throw new RuntimeException("bad input:"+encodingStr);
        }
    }
    public CompressionType parseCompressionType(String compressStr) {
        compressStr.toUpperCase();
        switch (compressStr) {
            case "UNCOMPRESSED":
                return CompressionType.UNCOMPRESSED;
            case "SNAPPY":
                return CompressionType.SNAPPY;
            case "GZIP":
                return CompressionType.GZIP;
//            case "LZO":
//                return CompressionType.LZO;
//            case "SDT":
//                return CompressionType.SDT;
//            case "PAA":
//                return CompressionType.PAA;
//            case "PLA":
//                return CompressionType.PLA;
            case "LZ4":
                return CompressionType.LZ4;
            case "ZSTD":
                return CompressionType.ZSTD;
            case "LZMA2":
                return CompressionType.LZMA2;
            default:
                throw new RuntimeException("bad input："+compressStr);
        }
    }

    /**
     * 获取第一列
     * @param filepath
     * @param delimiter
     * @return
     * @throws IOException
     */
    public List<String> getFirstColumns(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV(filepath, delimiter);
        List<String> columns_arr = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = record_iter.next();
            if (!first_cols.startsWith("#")) {
                //out.println("####### "+first_cols);
                columns_arr.add(first_cols);
            }
        }
        this.reader.close();
        return columns_arr;
    }

    /**
     * 解析csv文件，将第一列device和第二列tsName拼接为完整的path,返回path list
     * @param filepath
     * @return
     * @throws IOException
     */
    public List<String> getDeviceAndTs(String filepath) throws IOException {
        Iterable<CSVRecord> records = this.readCSV(filepath, ',');
        List<String> columns_arr = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = record_iter.next();
            if (!first_cols.startsWith("#")) {
                //out.println("####### "+first_cols);
                columns_arr.add(first_cols+"."+record_iter.next());
            }
        }
        this.reader.close();
        return columns_arr;
    }
    public List<String> getFirstColumns(String filepath) throws IOException {
        return getFirstColumns(filepath, ',');
    }

    /**
     * 加载 props, attributes and tags 格式csv
     * @param filepath
     * @return
     * @throws IOException
     */
    public List<Map<String, String>> loadProps(String filepath) throws IOException {
        Iterable<CSVRecord> records = this.readCSV(filepath, ',');
        List<Map<String, String>> result = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            Iterator<String> recordIter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = recordIter.next();
            if (!first_cols.startsWith("#")) {
                //out.println("####### "+first_cols);
                if (first_cols.startsWith("m:")) {
                    first_cols = first_cols.substring(2);
                }
                result.add(processMapField(first_cols));
            }
        }
        return result;
    }

    public CustomDataProvider load(String filepath) throws IOException {
        return load(filepath, ',');
    }

    public Iterator<Object[]> getData() throws IOException {
        return (Iterator<Object[]>) testCases.iterator();
    }

//    public static void main(String[] args) throws IOException {
//
//        Iterator<Object[]> i = new CustomDataProvider().load("data/timeseries-multi.csv").getData();
//        while (i.hasNext()) {
//            for(Object r: i.next()) {
//                out.println(r.toString());
//            }
//            out.println("--------------");
//        }
//
//    }
}
