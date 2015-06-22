package com.hazelcast.yarn.api.tuple.io;

import com.hazelcast.yarn.api.actor.Producer;

public interface ProducerInputStream<T> extends Producer<T> {
    T get(int idx);

    int size();
}
