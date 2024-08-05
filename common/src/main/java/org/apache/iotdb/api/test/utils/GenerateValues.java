package org.apache.iotdb.api.test.utils;

// com.github.javafaker.Faker 是 Java 编程语言中的一个库，它提供了一个简单的 API 来生成伪造的数据
import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;

import java.util.Locale;

/**
 * 构造值，用于生成各种类型的假数据
 */
public class GenerateValues {
    // 用于生成非中文的假数据
    private static Faker faker = new Faker();
    // 用于生成中文的假数据，并指定了中文简体的地区设置。
    private static Faker fakerChinese = new Faker(new Locale("zh-CN"));

    // 返回一个0到9之间的随机整数。
    public static int getInt() {
        return faker.number().randomDigit();
    }
    // 返回一个指定小数位数的随机长整型数。
    public static long getLong(int maxNumberOfDecimals) {
        // 参数maxNumberOfDecimals指定了小数位数，false表示不使用负数。
        return faker.number().randomNumber(maxNumberOfDecimals, false);
    }
    // 返回一个指定小数位数、最小值和最大值范围内的随机单精度浮点数。
    public static float getFloat(int maxNumberOfDecimals, int min, int max) {
        // 将生成的double类型数值强制转换为float。
        return (float) faker.number().randomDouble(maxNumberOfDecimals, min, max);
    }
    // 返回一个指定小数位数、最小值和最大值范围内的随机双精度浮点数。
    public static double getDouble(int maxNumberOfDecimals, int min, int max) {
        return faker.number().randomDouble(maxNumberOfDecimals, min, max);
    }
    // 返回一个随机的布尔值
    public static boolean getBoolean() {
        return faker.bool().bool();
    }
    // 返回一个指定长度的随机字符串。
    public static String getString(int max) {
        FakeValuesService fakeValuesService = new FakeValuesService(
                new Locale("en-GB"), new RandomService());
        // 使用正则表达式[a-zA-Z_0-9]生成只包含字母、数字和下划线的字符串。
        return fakeValuesService.regexify("[a-zA-Z_0-9]{" + max + "}");
    }
    // 返回一个随机的亚马逊标准识别码（ASIN）
    public static String getCombinedCode() {
        return faker.code().asin();
    }
    // 返回一个随机的欧洲商品编码（EAN-13）
    public static String getNumberCode() {
        return faker.code().ean13();
    }
    // 返回一个随机的中文城市名称
    public static String getChinese() {
        return fakerChinese.address().city();
    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);

    }
}
