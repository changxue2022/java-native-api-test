package org.apache.iotdb.test.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.System.out;

/**
 * 读取csv文件，返回除第一行header外的数据，供给给测试方法使用
 * Author: xue.chang@timecho.com
 */
public class CustomDataProvider {
    private List<Object[]> testCases = new ArrayList<>();

    public Iterable<CSVRecord> readCSV(String filepath, char delimiter) throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(this.getClass().getClassLoader().getResource(filepath).getPath()));
        CSVFormat csvformat = CSVFormat.DEFAULT.withDelimiter(delimiter).withEscape('\\').withQuote('"').withIgnoreEmptyLines(true);
        Iterable<CSVRecord> records = csvformat.parse(reader);
        // 去除header
        records.iterator().next();
        return records;
    }
    private @NotNull Map<String,String> processMapField(@NotNull String cols) {
        Map<String, String> cols_map = new HashMap<>();
        if (cols.equals("empty")) {
            return cols_map;
        }
        for (String item: cols.split("[|]")) {
            if (item.isEmpty()) {
                continue;
            }
            String[] values = item.split(":");
            cols_map.put(values[0],values[1]);
        }
        return cols_map;
    }
    private @NotNull List processListField(@NotNull String cols) {
        if (cols.startsWith("m:")) {
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
            List<String> result = new ArrayList<String>();
            if (!cols.equals("empty")) {
                for (String item_l : cols.split(",")) {
                    result.add(item_l);
                }
            }
            return result;
        }
    }
    public CustomDataProvider load(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV(filepath, delimiter);
        for (CSVRecord record : records) {
            // 解析每一行
            List<Object> columns_arr = new ArrayList<>();
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = record_iter.next();
            if (!first_cols.startsWith("#")) {
                //out.println("####### "+first_cols);
                columns_arr.add(first_cols);
                while (record_iter.hasNext()) {
                    String cols = record_iter.next();
                    if (cols.startsWith("m:")) {
                        cols = cols.substring(2);
                        columns_arr.add(processMapField(cols));
                    } else if (cols.startsWith("l:")) {
                        columns_arr.add(processListField(cols));
                    } else {
                        columns_arr.add(cols);
                    }
                }
                this.testCases.add(columns_arr.stream().toArray());
            }
        }
        return this;
    }
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
        return columns_arr;
    }
    public List<String> getFirstColumns(String filepath) throws IOException {
        return getFirstColumns(filepath, ',');
    }

    public CustomDataProvider load(String filepath) throws IOException {
        return load(filepath, ',');
    }

    public Iterator<Object[]> getData() throws IOException {
        return (Iterator<Object[]>) testCases.iterator();
    }

    public static void main(String[] args) throws IOException {
//        Iterator<Object[]> i = new CustomDataProvider().load("data/test.csv").getData();
//        while (i.hasNext()) {
//            for(Object r: i.next()) {
//                out.println(r.toString());
//            }
//            out.println("--------------");
//        }

    }
}
