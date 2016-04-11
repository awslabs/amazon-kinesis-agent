/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Class to handle reading configurations. A configuration is essentially a
 * <code>Map&lt;String, Object&gt;</code> that's interpreted by a configurator.
 *
 * This class construnct a configuration map from a JSON stream (read from a
 * file, e.g.):
 * <code><pre>
 *   ConfigReader reader = ConfigReader.get(Paths.get("filename.json"));
 * </pre></code>
 */
public class Configuration {

    /** The config map backing this reader instance. */
    private final Map<String, Object> configMap;

    /**
     * Build a configuration from a JSON file.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Configuration get(Path file) throws IOException {
        try (InputStream is = new FileInputStream(file.toString())) {
            return get(is);
        }
    }

    /**
     * Build a configuration from an input stream containing JSON.
     *
     * @param is
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static Configuration get(InputStream is) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.ALLOW_COMMENTS, true);
        return new Configuration((Map<String, Object>) mapper.readValue(is,
                new TypeReference<HashMap<String, Object>>() {
                }));
    }

    protected final Map<Class<?>, Function<Object, ?>> converters = new HashMap<>();
    protected final Map<Class<?>, ValueReader<?>> readers = new HashMap<>();

    /* Scalar converters */
    protected final Function<Object, String> stringConverter;
    protected final Function<Object, Double> doubleConverter;
    protected final Function<Object, Integer> integerConverter;
    protected final Function<Object, Long> longConverter;
    protected final BooleanConverter booleanConverter;
    protected final PathConverter pathConverter;
    protected final ConfigurationConverter configurationConverter;

    /* Scalar readers */
    protected final ValueReader<String> stringReader;
    protected final ValueReader<Double> doubleReader;
    protected final ValueReader<Integer> integerReader;
    protected final ValueReader<Long> longReader;
    protected final ValueReader<Boolean> booleanReader;
    protected final ValueReader<Path> pathReader;
    protected final ValueReader<Configuration> configurationReader;

    public Configuration(Configuration config) {
        this(config.getConfigMap());
    }

    /**
     * @param config
     */
    public Configuration(Map<String, Object> map) {
        Preconditions.checkNotNull(map);
        this.configMap = map;
        this.stringConverter = new Function<Object, String>() {
            @Override
            @Nullable
            public String apply(@Nullable Object input) {
                return (input == null || input instanceof String) ? ((String) input)
                        : input.toString();
            }
        };
        this.converters.put(String.class, this.stringConverter);
        this.doubleConverter = new StringConverterWrapper<>(
                Doubles.stringConverter());
        this.converters.put(String.class, this.stringConverter);
        this.integerConverter = new StringConverterWrapper<>(
                Ints.stringConverter());
        this.converters.put(Integer.class, this.integerConverter);
        this.longConverter = new StringConverterWrapper<>(
                Longs.stringConverter());
        this.converters.put(Long.class, this.longConverter);
        this.booleanConverter = new BooleanConverter();
        this.converters.put(Boolean.class, this.booleanConverter);
        this.pathConverter = new PathConverter();
        this.converters.put(Path.class, this.pathConverter);
        this.configurationConverter = new ConfigurationConverter();
        this.converters.put(Configuration.class, this.configurationConverter);

        this.stringReader = new ScalarValueReader<>(String.class,
                this.stringConverter);
        this.readers.put(String.class, this.stringReader);
        this.doubleReader = new ScalarValueReader<>(Double.class,
                this.doubleConverter);
        this.readers.put(Double.class, doubleReader);
        this.integerReader = new ScalarValueReader<>(Integer.class,
                this.integerConverter);
        this.readers.put(Integer.class, integerReader);
        this.longReader = new ScalarValueReader<>(Long.class,
                this.longConverter);
        this.readers.put(Long.class, longReader);
        this.booleanReader = new ScalarValueReader<>(Boolean.class,
                this.booleanConverter);
        this.readers.put(Boolean.class, booleanReader);
        this.pathReader = new ScalarValueReader<>(Path.class,
                this.pathConverter);
        this.readers.put(Path.class, pathReader);
        this.configurationReader = new ScalarValueReader<>(Configuration.class,
                this.configurationConverter);
        this.readers.put(Configuration.class, this.configurationReader);
    }

    @SuppressWarnings("unchecked")
    private <T> Function<Object, T> getConverter(Class<T> clazz) {
        if (this.converters.containsKey(clazz))
            return (Function<Object, T>) this.converters.get(clazz);
        else
            throw new ConfigurationException("There's no converter for type: "
                    + clazz.toString());
    }

    @SuppressWarnings("unchecked")
    private <T> ValueReader<T> getReader(Class<T> clazz) {
        if (this.readers.containsKey(clazz))
            return (ValueReader<T>) this.readers.get(clazz);
        else
            throw new ConfigurationException(
                    "Don't know how to read value of type: " + clazz.toString());
    }

    /**
     * @return an unmodifiable copy of the map that's backing this reader
     *         instance.
     */
    public Map<String, Object> getConfigMap() {
        return Collections.unmodifiableMap(this.configMap);
    }

    /**
     * @param key
     * @return <code>true</code> if configuration map contains the given key,
     *         <code>false</code> otherwise.
     */
    public boolean containsKey(String key) {
        return this.configMap.containsKey(key);
    }

    /**
     * Reads a required value of type <code>T</code> from the configuration map,
     * applying conversion as needed. If the key is not found in the
     * configuraiton, a {@link ConfigurationException} is raised.
     *
     * @param key
     * @param clazz
     * @return
     * @throws ConfigurationException
     *             if this class does not know how to read and convert values of
     *             the specified type, or if the provided key does not exist in
     *             the configuration.
     */
    public <T> T readScalar(String key, Class<T> clazz) {
        return getReader(clazz).read(key);
    }

    /**
     * Reads an optional value of type <code>T</code> from the configuration
     * map, applying conversion as needed.
     *
     * @param key
     * @param clazz
     * @param fallback
     * @return the config value at given key, or <code>fallback</code> if the
     *         key does not exist in the configuration. Note that if the key
     *         exists its value is returned, even if it's <code>null</code>.
     * @throws ConfigurationException
     *             if this class does not know how to read and convert values of
     *             the specified type.
     */
    public <T> T readScalar(String key, Class<T> clazz, T fallback) {
        return getReader(clazz).read(key, fallback);
    }

    /**
     *
     * @param enumType
     * @param key
     * @param fallback
     * @return the enum constant at given key, or <code>fallback</code> if the
     *         key does not exist in the configuration.
     * @throws ConfigurationException
     *             if the enum type is <code>null</code> or the specified enum type 
     *             has no constant with the specified value read from key
     */
    public <E extends Enum<E>> E readEnum(Class<E> enumType, String key, E fallback) {
        String stringVal = readString(key, null);
        if(stringVal != null) {
            try {
                return Enum.valueOf(enumType, stringVal);
            } catch (Exception e) {
                throw new ConfigurationException(
                        String.format(
                                "Value(%s) is not legally accepted by key: %s. " +
                                "Legal values are %s",
                                stringVal, key, Joiner.on(",").join(enumType.getEnumConstants())), e);
            }
        } else
            return fallback;
    }

    /**
     *
     * @param enumType
     * @param key
     * @return the enum constant at given key.
     * @throws ConfigurationException
     *             if the enum constant failed to be returned.
     */
    public <E extends Enum<E>> E readEnum(Class<E> enumType, String key) {
        String stringVal = readString(key, null);
        if(stringVal != null) {
            try {
                return Enum.valueOf(enumType, stringVal);
            } catch (Exception e) {
                throw new ConfigurationException(
                        String.format(
                                "Value(%s) is not legally accepted by key: %s. " +
                                "Legal values are %s",
                                stringVal, key, Joiner.on(",").join(enumType.getEnumConstants())), e);
            }
        } else
            throw new ConfigurationException(
                    "Required configuration value missing: " + key);
    }

    /**
     * @see #readScalar(String, Class, Object)
     */
    public String readString(String key, String fallback) {
        return readScalar(key, String.class, fallback);
    }

    /**
     * @see #readScalar(String, Class)
     */
    public String readString(String key) {
        return readScalar(key, String.class);
    }

    /**
     * @see #readScalar(String, Class, Object)
     */
    public Integer readInteger(String key, Integer fallback) {
        return readScalar(key, Integer.class, fallback);
    }

    /**
     * @see #readScalar(String, Class)
     */
    public Integer readInteger(String key) {
        return readScalar(key, Integer.class);
    }

    /**
     * @see #readScalar(String, Class)
     */
    public Long readLong(String key) {
        return readScalar(key, Long.class);
    }

    /**
     * @see #readScalar(String, Class, Object)
     */
    public Long readLong(String key, Long fallback) {
        return readScalar(key, Long.class, fallback);
    }

    /**
     * @see #readScalar(String, Class, Object)
     */
    public Boolean readBoolean(String key, Boolean fallback) {
        return readScalar(key, Boolean.class, fallback);
    }

    /**
     * @see #readScalar(String, Class)
     */
    public Boolean readBoolean(String key) {
        return readScalar(key, Boolean.class);
    }

    /**
     * @see #readScalar(String, Class, Object)
     */
    public Path readPath(String key, Path fallback) {
        return readScalar(key, Path.class, fallback);
    }

    /**
     * @see #readScalar(String, Class)
     */
    public Path readPath(String key) {
        return readScalar(key, Path.class);
    }

    /**
     * @see #readScalar(String, Class)
     */
    public Configuration readConfiguration(String key) {
        return readScalar(key, Configuration.class);
    }

    /**
     *
     * @param key
     * @param itemType
     * @return
     */
    public <T> List<T> readList(String key, Class<T> itemType) {
        return new ListReader<T>(itemType).read(key);
    }
    
    /**
    * Return the fallback value whenever an exception is thrown
    *
    * @param key
    * @param itemType
    * @param fallback
    * @return
    */
   public <T> List<T> readList(String key, Class<T> itemType, List<T> fallback) {
       return new ListReader<T>(itemType).read(key, fallback);
   }

    /**
     *
     * @param key
     * @param itemType
     * @param itemConverter
     * @return
     */
    public <T> List<T> readList(String key, Class<T> itemType,
            Function<Object, T> itemConverter) {
        return new ListReader<T>(itemType, itemConverter).read(key);
    }

    @Override
    public String toString() {
        return this.configMap.toString();
    }

    /**
     * Base interface for a configuration value reader.
     *
     * @param <T>
     */
    interface ValueReader<T> {
        T read(String key);

        T read(String key, T fallback);
    }

    /**
     * Helper class to parse configuration values and return expected types
     * after necessary conversion.
     *
     * @param <T>
     *            the expected return type.
     */
    class ScalarValueReader<T> implements ValueReader<T> {
        protected final Class<T> clazz;
        protected final Function<Object, T> converter;

        public ScalarValueReader(Class<T> clazz, Function<Object, T> converter) {
            this.clazz = clazz;
            this.converter = converter;
        }

        @Override
        public T read(String key) {
            Preconditions.checkNotNull(key);
            if (configMap.containsKey(key)) {
                return convert(key, configMap.get(key));
            } else
                throw new ConfigurationException(
                        "Required configuration value missing: " + key);
        }

        @Override
        public T read(String key, T fallback) {
            Preconditions.checkNotNull(key);
            if (configMap.containsKey(key))
                return convert(key, configMap.get(key));
            else
                return fallback;
        }

        @SuppressWarnings("unchecked")
        private T convert(String key, Object value) {
            if (value == null || value.getClass() == this.clazz)
                return (T) value;
            else {
                try {
                    return this.converter.apply(value);
                } catch (Exception e) {
                    throw new ConfigurationException(
                            String.format(
                                    "Failed to convert value (%s) to desired type (%s): %s",
                                    value.toString(), this.clazz.toString(),
                                    key), e);
                }
            }
        }
    }

    /**
     * Reader for lists in configuration. It will apply the converter for each
     * scalar value in the list.
     *
     * @param <T>
     *            the type of the elements of the list.
     */
    class ListReader<T> implements ValueReader<List<T>> {
        protected final Class<T> itemClazz;
        protected final Function<Object, T> itemConverter;

        public ListReader(Class<T> clazz) {
            this(clazz, getConverter(clazz));
        }

        public ListReader(Class<T> itemClazz, Function<Object, T> itemConverter) {
            this.itemClazz = itemClazz;
            this.itemConverter = itemConverter;
        }

        @Override
        public List<T> read(String key) {
            Preconditions.checkNotNull(key);
            if (configMap.containsKey(key)) {
                Object value = configMap.get(key);
                if (value == null) {
                    return null;
                } else if (value instanceof Iterable
                        || value.getClass().isArray()) {
                    List<?> inputList = value.getClass().isArray() ? Arrays
                            .asList(value) : (List<?>) (value);
                    List<T> result = new ArrayList<>(inputList.size());
                    for (Object item : inputList) {
                        result.add(convertItem(key, item));
                    }
                    return result;
                } else
                    throw new ConfigurationException(
                            "Configuration value is not a list: "
                                    + value.toString());
            } else
                throw new ConfigurationException("Required list missing: "
                        + key);
        }

        @Override
        public List<T> read(String key, List<T> fallback) {
            Preconditions.checkNotNull(key);
            if (configMap.containsKey(key)) {
                Object value = configMap.get(key);
                if (value == null) {
                    return null;
                } else if (value instanceof Iterable
                        || value.getClass().isArray()) {
                    List<?> inputList = value.getClass().isArray() ? Arrays
                            .asList(value) : (List<?>) (value);
                    List<T> result = new ArrayList<>(inputList.size());
                    for (Object item : inputList) {
                        result.add(convertItem(key, item));
                    }
                    return result;
                } else
                    return fallback;
            } else
                return fallback;
        }

        @SuppressWarnings("unchecked")
        private T convertItem(String key, Object item) {
            if (item == null || item.getClass() == this.itemClazz)
                return (T) item;
            else {
                try {
                    return this.itemConverter.apply(item);
                } catch (Exception e) {
                    throw new ConfigurationException(
                            String.format(
                                    "Failed to convert list item (%s) to desired type (%s): %s",
                                    item.toString(), this.itemClazz.toString(),
                                    key), e);
                }
            }
        }
    }

    /**
     * Validates that a given value falls withing a range.
     * @param value
     * @param range
     * @param messageTemplate
     * @param messageParameters
     * @throws ConfigurationException if the value doesn't fall within the range.
     */
    public static <T extends Comparable<T>> void validateRange(T value,
            Range<T> range, String label) {
        if(!range.contains(value)) {
            String message = String.format("'%s' value %s is outside of valid range (%s)",
                    label, value.toString(), range.toString());
            throw new ConfigurationException(message);
        }

    }
}
