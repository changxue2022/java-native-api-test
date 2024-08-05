package org.apache.iotdb.api.test.utils;

import java.util.List;
import java.util.Random;

public class Tools {
    public static <T> T getRandom(List<T> list) {
        int i = new Random().nextInt(list.size());
        return list.get(i);
    }
}
