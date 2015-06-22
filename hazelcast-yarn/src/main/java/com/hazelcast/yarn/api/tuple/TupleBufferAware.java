package com.hazelcast.yarn.api.tuple;

public interface TupleBufferAware {
    Tuple[] getBuffer();
}
