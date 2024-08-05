package org.apache.iotdb.api.test.utils;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;

import java.util.Locale;

public class GenerateValues {
    private static Faker faker = new Faker();
    private static Faker fakerChinese = new Faker(new Locale("zh-CN"));

    public static int getInt() {
        return faker.number().randomDigit();
    }
    public static long getLong(int maxNumberOfDecimals) {
        return faker.number().randomNumber(maxNumberOfDecimals,false);
    }
    public static float getFloat(int maxNumberOfDecimals, int min, int max) {
        return (float) faker.number().randomDouble(maxNumberOfDecimals,min,max);
    }
    public static double getDouble(int maxNumberOfDecimals, int min, int max) {
        return faker.number().randomDouble(maxNumberOfDecimals,min,max);
    }
    public static boolean getBoolean() {
        return faker.bool().bool();
    }
    public static String getString(int max) {
        FakeValuesService fakeValuesService = new FakeValuesService(
                new Locale("en-GB"), new RandomService());
        return fakeValuesService.regexify("[a-zA-Z_0-9]{" + max + "}");
    }
    public static String getCombinedCode() {
        return faker.code().asin();
    }
    public static String getNumberCode() {
        return faker.code().ean13();
    }

    public static String getChinese() {
        return fakerChinese.address().city();
    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);

    }
}
