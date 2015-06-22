package com.hazelcast.yarn.impl.tuple;

import java.util.Arrays;
import java.io.IOException;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.impl.HeapData;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTuple<K, V> implements Tuple<K, V> {
    private Object[] data;

    private int keySize;
    private int valueSize;
    private int partitionId;

    private PartitioningStrategy partitioningStrategy;

    public DefaultTuple() {

    }

    DefaultTuple(K key, V value) {
        this(key, value, -1, null);
    }

    DefaultTuple(K key, V value, int partitionId, PartitioningStrategy partitioningStrategy) {
        checkNotNull(key);
        checkNotNull(value);

        this.data = new Object[2];

        this.keySize = 1;
        this.valueSize = 1;

        this.data[0] = key;
        this.data[1] = value;
        this.partitionId = partitionId;
        this.partitioningStrategy = partitioningStrategy;
    }

    DefaultTuple(K key, V[] values) {
        this(key, values, -1, null);
    }

    DefaultTuple(K key, V[] values, int partitionId, PartitioningStrategy partitioningStrategy) {
        checkNotNull(key);
        checkNotNull(values);

        this.data = new Object[1 + values.length];
        this.keySize = 1;
        this.valueSize = values.length;
        this.data[0] = key;
        System.arraycopy(values, 0, data, this.keySize, values.length);
        this.partitionId = partitionId;
        this.partitioningStrategy = partitioningStrategy;
    }

    DefaultTuple(K[] key, V[] values) {
        this(key, values, -1, null);
    }

    DefaultTuple(K[] key, V[] values, int partitionId, PartitioningStrategy partitioningStrategy) {
        checkNotNull(key);
        checkNotNull(values);

        this.data = new Object[key.length + values.length];

        this.keySize = key.length;
        this.valueSize = values.length;

        System.arraycopy(key, 0, this.data, 0, key.length);
        System.arraycopy(values, 0, this.data, this.keySize, values.length);

        this.partitionId = partitionId;
        this.partitioningStrategy = partitioningStrategy;
    }

    @Override
    public Data getKeyData(NodeEngine nodeEngine) {
        checkNotNull(this.partitioningStrategy);
        return this.getKeyData(this.partitioningStrategy, nodeEngine);
    }

    @Override
    public Data getValueData(NodeEngine nodeEngine) {
        checkNotNull(this.partitioningStrategy);
        return this.getValueData(this.partitioningStrategy, nodeEngine);
    }

    @Override
    public Data getKeyData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return this.toData(this.keySize, 0, partitioningStrategy, nodeEngine);
    }

    @Override
    public Data getValueData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return this.toData(this.valueSize, this.keySize, partitioningStrategy, nodeEngine);
    }

    @Override
    public Data getKeyData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        this.checkKeyIndex(index);
        return nodeEngine.getSerializationService().toData(this.data[index], partitioningStrategy);
    }

    @Override
    public Data getValueData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        this.checkValueIndex(index);
        return nodeEngine.getSerializationService().toData(this.data[this.keySize + index], partitioningStrategy);
    }

    @Override
    public K[] cloneKeys() {
        K[] tmp = (K[]) new Object[this.keySize];
        System.arraycopy(this.data, 0, tmp, 0, this.keySize);
        return tmp;
    }

    @Override
    public V[] cloneValues() {
        V[] tmp = (V[]) new Object[this.valueSize];
        System.arraycopy(this.data, this.keySize, tmp, 0, this.valueSize);
        return tmp;
    }

    @Override
    public K getKey(int index) {
        this.checkKeyIndex(index);
        return (K) data[index];
    }

    @Override
    public V getValue(int index) {
        this.checkValueIndex(index);
        return (V) data[this.keySize + index];
    }

    @Override
    public int keySize() {
        return this.keySize;
    }

    @Override
    public int valueSize() {
        return this.valueSize;
    }

    @Override
    public void setKey(int index, K value) {
        this.checkKeyIndex(index);
        this.partitionId = -1;
        this.data[index] = value;
    }

    @Override
    public void setValue(int index, V value) {
        this.checkValueIndex(index);
        this.data[this.keySize + index] = value;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(this.data);
        out.writeInt(this.keySize);
        out.writeInt(this.valueSize);
        out.writeInt(this.partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.data = in.readObject();
        this.keySize = in.readInt();
        this.valueSize = in.readInt();
        this.partitionId = in.readInt();
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultTuple<?, ?> tuple = (DefaultTuple<?, ?>) o;

        if (keySize != tuple.keySize) return false;
        if (valueSize != tuple.valueSize) return false;

        return Arrays.equals(data, tuple.data);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(data);
        result = 31 * result + keySize;
        result = 31 * result + valueSize;
        return result;
    }

    private Data toData(int size, int offset, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        if (size == 1) {
            return nodeEngine.getSerializationService().toData(this.data[offset], partitioningStrategy);
        } else {
            BufferObjectDataOutput output = nodeEngine.getSerializationService().createObjectDataOutput();

            for (int i = 0; i < size - 1; i++) {
                try {
                    output.writeObject(this.data[offset + i]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return new HeapData(output.toByteArray());
        }
    }

    private void checkKeyIndex(int index) {
        if ((index < 0) || (index >= this.keySize)) {
            throw new IllegalStateException("Invalid index for tupleKey");
        }
    }

    private void checkValueIndex(int index) {
        if ((index < 0) || (index >= this.valueSize)) {
            throw new IllegalStateException("Invalid index for tupleValue");
        }
    }
}