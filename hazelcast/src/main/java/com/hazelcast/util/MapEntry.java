package com.hazelcast.util;

import java.util.Map;

public class MapEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private final V value;

    public MapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> Map.Entry<K, V> of(K key, V value) {
        return new MapEntry<K, V>(key, value);
    }

    @Override
    public K getKey() {
        return this.key;
    }

    @Override
    public V getValue() {
        return this.value;
    }

    @Override
    public V setValue(V value) {
        throw new IllegalStateException("Not supported - immutable");
    }
}
