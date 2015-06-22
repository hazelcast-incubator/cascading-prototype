package com.hazelcast.util;

import java.util.Map;
import java.util.LinkedHashMap;

public class LinkedMapBuilder<K, V> {
    private final Map<K, V> map;

    private LinkedMapBuilder() {
        map = new LinkedHashMap<K, V>();
    }

    public static <K, V> LinkedMapBuilder<K, V> builder() {
        return new LinkedMapBuilder<K, V>();
    }

    public LinkedMapBuilder<K, V> put(K key, V value) {
        map.put(key, value);
        return this;
    }

    public Map<K, V> build() {
        return map;
    }

    public static <K, V> Map<K, V> of(K key, V value) {
        return LinkedMapBuilder.<K, V>builder().put(key, value).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2) {
        return LinkedMapBuilder.<K, V>builder().put(key1, value1).put(key2, value2).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3) {
        return LinkedMapBuilder.<K, V>builder().put(key1, value1).put(key2, value2).put(key3, value3).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        return LinkedMapBuilder.<K, V>builder().put(key1, value1).put(key2, value2).put(key3, value3).put(key4, value4).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5) {
        return LinkedMapBuilder.<K, V>builder().
                put(key1, value1).
                put(key2, value2).
                put(key3, value3).
                put(key4, value4).
                put(key5, value5).
                build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5, K key6, V value6) {
        return LinkedMapBuilder.<K, V>builder().
                put(key1, value1).
                put(key2, value2).
                put(key3, value3).
                put(key4, value4).
                put(key5, value5).
                put(key6, value6).
                build();
    }

}
