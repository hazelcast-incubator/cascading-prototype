package com.hazelcast.yarn.api.tuple;

import com.hazelcast.yarn.api.actor.TupleProducer;

public interface TupleReader extends TupleProducer {
    boolean hasNext();

    int getPartitionId();
}
