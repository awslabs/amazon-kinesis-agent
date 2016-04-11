/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;
import com.google.common.collect.Range;

/**
 * Unit tests for {@link Configuration}.
 */
public class ConfigurationTest {

    private Configuration getTestConfiguration(String fileName)
            throws IOException {
        try (InputStream i = this.getClass().getResourceAsStream(fileName)) {
            return Configuration.get(i);
        }
    }

    @Test
    public void testContainsKey() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        Assert.assertTrue(config.containsKey("double1"));
        Assert.assertTrue(config.containsKey("boolean_t1"));
        Assert.assertFalse(config.containsKey("nonexistent"));
    }

    @DataProvider(name = "readScalarNoFallback")
    public Object[][] testReadScalarNoFallbackData() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        return new Object[][] { { config, Double.class, "double2", 200.2 },
                { config, String.class, "string2", "s2" },
                { config, Integer.class, "integer1", 100 },
                { config, Integer.class, "integer2", 200 },
                { config, Long.class, "long1", 100000000000L },
                { config, Long.class, "long2", 200000000000L },
                { config, Boolean.class, "boolean_t1", true },
                { config, Boolean.class, "boolean_f1", false },
                { config, String.class, "null1", null },
                { config, Long.class, "null1", null },
                { config, Path.class, "path2", Paths.get("/tmp/f2.log") }, };
    }

    @Test(dataProvider = "readScalarNoFallback")
    public void testReadScalarNoFallback(Configuration config, Class<?> clazz,
            String key, Object expected) throws IOException {
        Assert.assertEquals(config.readScalar(key, clazz), expected);
    }

    @Test(expectedExceptions = ConfigurationException.class, expectedExceptionsMessageRegExp = "Required configuration value missing.*")
    public void testReadScalarNoFallbackWithMissingKey() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        config.readScalar("nonexistent", TestUtils.pickOne(String.class,
                Double.class, Path.class, Configuration.class));
    }

    @DataProvider(name = "readScalarWithFallback")
    public Object[][] testReadScalarWithFallbackData() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        return new Object[][] {
                // keys exist, should return value in config
                { config, Double.class, "double2", 1234.0, 200.2 },
                { config, String.class, "string2", "somethingelse", "s2" },
                { config, Integer.class, "integer2", 1234, 200 },
                { config, Long.class, "long2", 1234L, 200000000000L },
                { config, Boolean.class, "boolean_t1", false, true },
                { config, Boolean.class, "boolean_f1", true, false },
                { config, Path.class, "path2", Paths.get("/tmp/another.log"),
                        Paths.get("/tmp/f2.log") },
                // keys exist, should return value in config even if it's null
                { config, String.class, "null1", "somethingelse2", null },
                { config, Long.class, "null1", 1234L, null },
                // keys don't exists, should return fallback
                { config, Double.class, "missing_double", 1234.0, 1234.0 },
                { config, String.class, "missing_string", "s1234", "s1234" },
                { config, Integer.class, "missing_integer", 1234, 1234 },
                { config, Long.class, "missing_long", 123400000000L,
                        123400000000L },
                { config, Boolean.class, "missing_boolean_t", true, true },
                { config, Boolean.class, "missing_boolean_f", false, false },
                { config, Path.class, "missing_path",
                        Paths.get("/tmp/new.log"), Paths.get("/tmp/new.log") },
                // fallback is null, should still work
                { config, String.class, "missing_string", null, null },
                { config, Integer.class, "missing_integer", null, null }, };
    }

    @Test(dataProvider = "readScalarWithFallback")
    public <T> void testReadScalarWithFallback(Configuration config,
            Class<T> clazz, String key, T fallback, T expected)
            throws IOException {
        Assert.assertEquals(config.readScalar(key, clazz, fallback), expected);
    }

    @DataProvider(name = "readScalarTypeMismatch")
    public Object[][] testReadScalarTypeMismatchData() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        return new Object[][] { { config, Integer.class, "string1" },
                { config, Boolean.class, "string2" },
                { config, Double.class, "string1" },
                { config, Configuration.class, "integer1" }, };
    }

    @Test(dataProvider = "readScalarTypeMismatch", expectedExceptions = ConfigurationException.class, expectedExceptionsMessageRegExp = "Failed to convert value.*")
    public void testReadScalarTypeMismatch(Configuration config,
            Class<?> clazz, String key) {
        config.readScalar(key, clazz);
    }

    @Test
    public void testReadConfiguration() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        Configuration subConfig = config.readConfiguration("config1");
        Assert.assertEquals(subConfig.readString("config1_v1"), "v1");
        Assert.assertEquals(subConfig.readString("config1_v2"), "v2");
        Assert.assertEquals(subConfig.readInteger("config1_v3"),
                Integer.valueOf(3));
    }

    @DataProvider(name = "readScalarList")
    public Object[][] testReadScalarListData() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        return new Object[][] {
                { config, Integer.class, "intlist1",
                        new Object[] { 1, 2, 3, 4 } },
                { config, String.class, "stringlist1",
                        new Object[] { "abc", "def", "hij" } }, };
    }

    @Test(dataProvider="readScalarList")
    public <T> void testReadScalarList(Configuration config, Class<T> clazz, String key, T[] expected) {
        List<T> lst = config.readList(key, clazz);
        Assert.assertEquals(lst.toArray(), expected);
    }

    @Test(expectedExceptions=ConfigurationException.class, expectedExceptionsMessageRegExp="Failed to convert list item.*")
    public void testReadScalarListItemTypeMismatch() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        config.readList("stringlist1", Integer.class);
    }

    @Test(expectedExceptions=ConfigurationException.class, expectedExceptionsMessageRegExp="Configuration value is not a list.*")
    public <T> void testReadListValueIsNotList() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        config.readList("string1", String.class);
    }
    
    @Test
    public <T> void testReadListFallback() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        List<T> list = config.readList("stringlist_notexisted", String.class, Collections.EMPTY_LIST);
        Assert.assertNotNull(list);
        Assert.assertTrue(list.isEmpty());
    }
    
    @Test
    public <T> void testReadConfigurationList() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        List<Configuration> subConfigs = config.readList("configlist1", Configuration.class);
        Assert.assertEquals(subConfigs.size(), 2);
        // spot-check the expected data
        Assert.assertEquals(subConfigs.get(0).readString("v1"), "c1_v1");
        Assert.assertEquals(subConfigs.get(1).readString("v1"), "c2_v1");
        Assert.assertEquals(subConfigs.get(0).readLong("v3"), Long.valueOf(13L));
        Assert.assertEquals(subConfigs.get(1).readLong("v3"), Long.valueOf(23L));
    }

    @DataProvider(name="validateRangeSuccess")
    public Object[][] testValidateRangeSuccessData() {
        return new Object[][] {
                {0, 0, 100, "key1"},
                {100, 0, 100, "key2"},
                {50, -1000, 1000, "key3"},
                {-1, -100, 300, "key3"},
        };
    }

    @Test(dataProvider="validateRangeSuccess")
    public void testValidateRangeSuccess(int value, int lower, int upper, String key) {
        Configuration.validateRange(value, Range.closed(lower, upper), key);
    }

    @DataProvider(name="validateRangeFailure")
    public Object[][] testValidateRangeFailureData() {
        return new Object[][] {
                {0, 1, 100, "key1"},
                {-1, 1, 100, "key2"},
                {1001, 1, 1000, "key3"},
                {-1001, -1000, 1000, "key4"},
        };
    }

    @Test(dataProvider="validateRangeFailure",
            expectedExceptions=ConfigurationException.class,
            expectedExceptionsMessageRegExp=".*outside of valid range.*")
    public void testValidateRangeFailure(int value, int lower, int upper, String key) {
        Configuration.validateRange(value, Range.closed(lower, upper), key);
    }

    @Test
    public void testReadEnum() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        Assert.assertEquals(config.readEnum(Thread.State.class, "enumGood"), Thread.State.RUNNABLE);
        Assert.assertEquals(config.readEnum(Thread.State.class, "nonExistingEnum", Thread.State.TERMINATED), Thread.State.TERMINATED);
    }

    @Test(expectedExceptions=ConfigurationException.class)
    public void testReadBadEnum() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        config.readEnum(Thread.State.class, "enumBad");
    }

    @Test(expectedExceptions=ConfigurationException.class)
    public void testReadMissingEnumNoFallback() throws IOException {
        Configuration config = getTestConfiguration("config1.json");
        config.readEnum(Thread.State.class, "nonExistingEnum");
    }
}
