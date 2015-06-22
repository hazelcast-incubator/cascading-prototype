package com.hazelcast.yarn.impl.tuple;


import java.io.IOException;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.core.PartitioningStrategy;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class Tuple2<K, V> implements Tuple<K, V> {
    private K key;
    private V value;

    private int keySize;
    private int valueSize;
    private int partitionId;

    private PartitioningStrategy partitioningStrategy;

    public Tuple2() {

    }

    public Tuple2(K key, V value) {
        this(key, value, -1, null);
    }

    Tuple2(K key, V value, int partitionId, PartitioningStrategy partitioningStrategy) {
        checkNotNull(key);
        checkNotNull(value);

        this.keySize = 1;
        this.valueSize = 1;

        this.key = key;
        this.value = value;
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
        return this.toData(this.key, partitioningStrategy, nodeEngine);
    }

    @Override
    public Data getValueData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return this.toData(this.value, partitioningStrategy, nodeEngine);
    }

    @Override
    public Data getKeyData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        this.checkKeyIndex(index);
        return nodeEngine.getSerializationService().toData(key, partitioningStrategy);
    }

    @Override
    public Data getValueData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        this.checkValueIndex(index);
        return nodeEngine.getSerializationService().toData(value, partitioningStrategy);
    }

    @Override
    public K[] cloneKeys() {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public V[] cloneValues() {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public K getKey(int index) {
        this.checkKeyIndex(index);
        return key;
    }

    @Override
    public V getValue(int index) {
        this.checkValueIndex(index);
        return value;
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
        this.key = value;
    }

    @Override
    public void setValue(int index, V value) {
        this.checkValueIndex(index);
        this.value = value;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(this.key);
        out.writeObject(this.value);
        out.writeInt(this.keySize);
        out.writeInt(this.valueSize);
        out.writeInt(this.partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.key = in.readObject();
        this.value = in.readObject();
        this.keySize = in.readInt();
        this.valueSize = in.readInt();
        this.partitionId = in.readInt();
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    private Data toData(Object obj, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return nodeEngine.getSerializationService().toData(obj, partitioningStrategy);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (!key.equals(tuple2.key)) return false;
        return value.equals(tuple2.value);

    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
