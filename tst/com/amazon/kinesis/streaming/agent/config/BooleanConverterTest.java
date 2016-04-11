/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.config;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.config.BooleanConverter;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.testing.TestUtils;

/**
 * Unit tests for {@link BooleanConverter}.
 */
public class BooleanConverterTest {
    @DataProvider(name="conversion")
    public Object[][] testConversionData() {
        return new Object[][] {
                {"true", true},
                {"t", true},
                {"1", true},
                {"yes", true},
                {"y", true},
                {"false", false},
                {"f", false},
                {"0", false},
                {"no", false},
                {"n", false},
                {null, null},
        };
    }
    @Test(dataProvider="conversion")
    public void testConversion(Object input, Boolean expected) {
        BooleanConverter converter = new BooleanConverter();
        Assert.assertEquals(converter.apply(input), expected);
    }

    @Test(dataProvider="conversion")
    public void testConversionCaseInsensitive(String input, Boolean expected) {
        BooleanConverter converter = new BooleanConverter();
        Assert.assertEquals(converter.apply(TestUtils.randomCase(input)), expected);
    }

    @Test(expectedExceptions=ConfigurationException.class, expectedExceptionsMessageRegExp="Cannot convert value to boolean.*")
    public void testConversionError() {
        BooleanConverter converter = new BooleanConverter();
        converter.apply("somerandomstring");
    }
}
