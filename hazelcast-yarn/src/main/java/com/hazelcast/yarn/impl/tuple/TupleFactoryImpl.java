package com.hazelcast.yarn.impl.tuple;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.api.tuple.TupleFactory;

public class TupleFactoryImpl implements TupleFactory {
    @Override
    public <K, V> Tuple<K, V> tuple(K k, V v) {
        return new Tuple2<K, V>(k, v);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K k, V v, int partitionID, PartitioningStrategy partitioningStrategy) {
        return new Tuple2<K, V>(k, v, partitionID, partitioningStrategy);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K k, V[] v) {
        return new DefaultTuple<K, V>(k, v);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K k, V[] v, int partitionId, PartitioningStrategy partitioningStrategy) {
        return new DefaultTuple<K, V>(k, v, partitionId, partitioningStrategy);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K[] k, V[] v) {
        return new DefaultTuple<K, V>(k, v);
    }

    @Override
    public <K, V> Tuple<K, V> tuple(K[] k, V[] v, int partitionID, PartitioningStrategy partitioningStrategy) {
        return new DefaultTuple<K, V>(k, v, partitionID, partitioningStrategy);
    }
}
