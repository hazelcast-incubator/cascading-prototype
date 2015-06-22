package com.hazelcast.yarn.api.tuple;

import com.hazelcast.internal.serialization.SerializationService;


public interface TupleConvertor<R, K, V> {
    Tuple<K, V> convert(R object, SerializationService ss);
}
