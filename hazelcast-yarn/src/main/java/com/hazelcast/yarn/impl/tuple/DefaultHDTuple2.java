package com.hazelcast.yarn.impl.tuple;

import java.io.IOException;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.core.PartitioningStrategy;

public class DefaultHDTuple2 implements com.hazelcast.yarn.api.tuple.HDTuple<Object, Object> {
    private long keyAddress = -1L;
    private long keyObjectSize = -1L;
    private long valueAddress = -1L;
    private long valueObjectSize = -1L;

    public DefaultHDTuple2(long keyAddress, long keyObjectSize) {
        this.keyAddress = keyAddress;
        this.keyObjectSize = keyObjectSize;
        this.valueAddress = -1;
        this.valueObjectSize = -1;
    }

    public DefaultHDTuple2(long keyAddress, long keyObjectSize, long valueAddress, long valueObjectSize) {
        this.keyAddress = keyAddress;
        this.valueAddress = valueAddress;
        this.keyObjectSize = keyObjectSize;
        this.valueObjectSize = valueObjectSize;
    }

    @Override
    public Data getKeyData(NodeEngine nodeEngine) {
        return null;
    }

    @Override
    public Data getValueData(NodeEngine nodeEngine) {
        return null;
    }

    @Override
    public Data getKeyData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return null;
    }

    @Override
    public Data getValueData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return null;
    }

    @Override
    public Data getKeyData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return null;
    }

    @Override
    public Data getValueData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return null;
    }

    @Override
    public Object[] cloneKeys() {
        return new Object[0];
    }

    @Override
    public Object[] cloneValues() {
        return new Object[0];
    }

    @Override
    public Object getKey(int index) {
        return this.keyAddress;
    }

    @Override
    public Object getValue(int index) {
        return null;
    }

    @Override
    public int keySize() {
        return 1;
    }

    @Override
    public int valueSize() {
        return 1;
    }

    @Override
    public void setKey(int index, Object value) {

    }

    @Override
    public void setValue(int index, Object value) {

    }

    @Override
    public int getPartitionId() {
        return 0;
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DefaultHDTuple2)) {
            return false;
        }

        DefaultHDTuple2 that = (DefaultHDTuple2) o;

        if (this.keyObjectSize != that.keyObjectSize) {
            return false;
        }

        if (this.valueObjectSize != that.valueObjectSize) {
            return false;
        }

        long thatKeyAddress = this.keyAddress;
        long thisKeyAddress = that.keyAddress;

        if (thisKeyAddress < 0) {
            if (thatKeyAddress > 0) {
                return false;
            }
        }

        if (thatKeyAddress < 0) {
            if (thisKeyAddress > 0) {
                return false;
            }
        }

        long thisValueAddress = this.valueAddress;
        long thatValueAddress = that.valueAddress;

        if (thisValueAddress < 0) {
            if (thatValueAddress > 0) {
                return false;
            }
        }

        if (thatValueAddress < 0) {
            if (thisValueAddress > 0) {
                return false;
            }
        }

        if (thatKeyAddress > 0) {
            for (long i = 0; i < this.keyObjectSize; i++) {
                if (YarnUtil.getUnsafe().getByte(thatKeyAddress + i) != YarnUtil.getUnsafe().getByte(thisKeyAddress + i)) {
                    return false;
                }
            }
        }

        if (thisValueAddress > 0) {
            for (long i = 0; i < this.valueObjectSize; i++) {
                if (YarnUtil.getUnsafe().getByte(thatValueAddress + i) != YarnUtil.getUnsafe().getByte(thisValueAddress + i)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 1;
        long keyAddress = this.keyAddress;

        for (long i = 0; i < this.keyObjectSize; i++) {
            h = 31 * h + (int) YarnUtil.getUnsafe().getByte(keyAddress + i);
        }

        long valueAddress = this.valueAddress;

        for (long i = 0; i < this.valueObjectSize; i++) {
            h = 31 * h + (int) YarnUtil.getUnsafe().getByte(valueAddress + i);
        }

        return h;
    }

    @Override
    public long keyAddress() {
        return this.keyAddress;
    }

    @Override
    public long keyObjectSize() {
        return this.keyObjectSize;
    }

    @Override
    public long valueAddress() {
        return this.valueAddress;
    }

    @Override
    public long valueObjectSize() {
        return this.valueObjectSize;
    }

    @Override
    public void setValueAddress(long address, long size) {
        if (valueAddress > 0) {
            YarnUtil.getUnsafe().freeMemory(valueAddress);
        }

        this.valueAddress = address;
        this.valueObjectSize = size;
    }

    @Override
    public void setKeyAddress(long address, long size) {
        if (keyAddress > 0) {
            YarnUtil.getUnsafe().freeMemory(keyAddress);
        }

        this.keyAddress = size;
        this.keyAddress = address;
    }

    @Override
    public void releaseValue() {
        if (valueAddress > 0) {
            YarnUtil.getUnsafe().freeMemory(valueAddress);
        }
    }
}
