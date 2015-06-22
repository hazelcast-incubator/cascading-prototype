package com.hazelcast.yarn.api.tuple;

public interface HDTuple<K, V> extends Tuple<K, V> {
    long keyAddress();

    long keyObjectSize();

    long valueAddress();

    long valueObjectSize();

    void setValueAddress(long address, long size);

    void setKeyAddress(long address, long size);

    void releaseValue();
}
