package com.hazelcast.yarn.api.tuple;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.DataSerializable;

public interface Tuple<K, V> extends DataSerializable {
    Data getKeyData(NodeEngine nodeEngine);

    Data getValueData(NodeEngine nodeEngine);

    Data getKeyData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine);

    Data getValueData(PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine);

    Data getKeyData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine);

    Data getValueData(int index, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine);

    K[] cloneKeys();

    V[] cloneValues();

    K getKey(int index);

    V getValue(int index);

    int keySize();

    int valueSize();

    void setKey(int index, K value);

    void setValue(int index, V value);

    int getPartitionId();

    PartitioningStrategy getPartitioningStrategy();
}
