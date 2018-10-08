package com.amazon.kinesis.streaming.agent.processing.processors;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;

/**
 * Parse the log entries using an external pluggable class
 *
 * Configuration of this converter looks like:
 * {
 *     "optionName": "USINGPLUGGIN",
 *     "className": "org.example.MyDataConverter"
 * }
 *
 */
public class PluggableDataConverter implements IDataConverter {

  private final IDataConverter plugginConverter;

  public PluggableDataConverter(Configuration config) throws ConfigurationException {
    plugginConverter = readPlugginConverter(config);
  }

  private static IDataConverter readPlugginConverter(Configuration config) throws ConfigurationException {
    try {
      return unsafeReadPlugginConverter(config);
    }
    catch (ReflectiveOperationException e) {
      throw new ConfigurationException(e.getMessage(), e);
    }
  }

  private static IDataConverter unsafeReadPlugginConverter(Configuration config) throws ReflectiveOperationException {

    final Class<IDataConverter> clazz = readPlugginConverterClass(config);

    for (final Constructor<?> ctor : clazz.getConstructors()) {
      final Class<?>[] paramTypes = ctor.getParameterTypes();
      if (paramTypes.length == 1 && paramTypes[0].equals(Configuration.class)) {
        return (IDataConverter) ctor.newInstance(config);
      }
    }

    return clazz.newInstance();
  }

  @SuppressWarnings("unchecked")
  private static Class<IDataConverter> readPlugginConverterClass(Configuration config) {
    try {
      return (Class<IDataConverter>) Class.forName(config.readString("className"));
    }
    catch (ClassNotFoundException e) {
      throw new ConfigurationException(e.getMessage(), e);
    }
  }

  @Override
  public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
    return plugginConverter.convert(data);
  }

}
