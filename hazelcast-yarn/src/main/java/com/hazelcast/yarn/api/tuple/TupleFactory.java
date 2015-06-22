package com.hazelcast.yarn.api.tuple;

import com.hazelcast.core.PartitioningStrategy;

public interface TupleFactory {
    <K, V> Tuple<K, V> tuple(K k, V v);

    <K, V> Tuple<K, V> tuple(K k, V v, int partitionId, PartitioningStrategy partitioningStrategy);

    <K, V> Tuple<K, V> tuple(K k, V[] v);

    <K, V> Tuple<K, V> tuple(K k, V[] v, int partitionId, PartitioningStrategy partitioningStrategy);

    <K, V> Tuple<K, V> tuple(K[] k, V[] v);

    <K, V> Tuple<K, V> tuple(K[] k, V[] v, int partitionId, PartitioningStrategy partitioningStrategy);
}
