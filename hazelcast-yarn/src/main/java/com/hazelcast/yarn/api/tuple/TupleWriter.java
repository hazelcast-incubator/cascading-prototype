package com.hazelcast.yarn.api.tuple;

import com.hazelcast.yarn.api.actor.Shuffler;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;

public interface TupleWriter extends TupleConsumer, Shuffler {
    SinkTapWriteStrategy getSinkTapWriteStrategy();

    boolean isPartitioned();

    PartitioningStrategy getPartitionStrategy();

    int getPartitionId();

    boolean isClosed();
}
