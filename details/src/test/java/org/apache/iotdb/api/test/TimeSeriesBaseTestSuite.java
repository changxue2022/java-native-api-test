package org.apache.iotdb.api.test;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;

public class TimeSeriesBaseTestSuite extends BaseTestSuite{

    public List<Object> translateString2Type(String datatypeStr, String encodingStr, String compressStr) {
        datatypeStr = datatypeStr.toLowerCase();
        encodingStr = encodingStr.toUpperCase();
        compressStr = compressStr.toUpperCase();
        List<Object> result = new ArrayList<>(3);
        switch (datatypeStr) {
            case "boolean":
                result.add(TSDataType.BOOLEAN);
                break;
            case "int":
                result.add(TSDataType.INT32);
                break;
            case "long":
                result.add(TSDataType.INT64);
                break;
            case "float":
                result.add(TSDataType.FLOAT);
                break;
            case "double":
                result.add(TSDataType.DOUBLE);
                break;
            case "vector":
                result.add(TSDataType.VECTOR);
                break;
            case "text":
                result.add(TSDataType.TEXT);
                break;
            default:
                result.add(null);
                break;
        }
        switch (encodingStr) {
            case "PLAIN":
                result.add(TSEncoding.PLAIN);
                break;
            case "DICTIONARY":
                result.add(TSEncoding.DICTIONARY);
                break;
            case "RLE":
                result.add(TSEncoding.RLE);
                break;
            case "DIFF":
                result.add(TSEncoding.DIFF);
                break;
            case "TS_2DIFF":
                result.add(TSEncoding.TS_2DIFF);
                break;
            case "BITMAP":
                result.add(TSEncoding.BITMAP);
                break;
            case "GORILLA_V1":
                result.add(TSEncoding.GORILLA_V1);
                break;
            case "REGULAR":
                result.add(TSEncoding.REGULAR);
                break;
            case "GORILLA":
                result.add(TSEncoding.GORILLA);
                break;
            case "ZIGZAG":
                result.add(TSEncoding.ZIGZAG);
                break;
            case "FREQ":
                result.add(TSEncoding.FREQ);
                break;
            case "SPRINTZ":
                result.add(TSEncoding.SPRINTZ);
                break;
            case "RLBE":
                result.add(TSEncoding.RLBE);
                break;
            default:
                result.add(null);
                break;
        }
        switch (compressStr) {
            case "UNCOMPRESSED":
                result.add(CompressionType.UNCOMPRESSED);
                break;
            case "SNAPPY":
                result.add(CompressionType.SNAPPY);
                break;
            case "GZIP":
                result.add(CompressionType.GZIP);
                break;
            case "LZ4":
                result.add(CompressionType.LZ4);
                break;
            case "ZSTD":
                result.add(CompressionType.ZSTD);
                break;
            case "LZMA2":
                result.add(CompressionType.LZMA2);
                break;
            default:
                result.add(null);
                break;
        }
        return result;
    }

}
