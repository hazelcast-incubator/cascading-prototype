package com.hazelcast.yarn.impl.actor.shuffling;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.container.ContainerContext;

public class ShufflingWriter extends ShufflingConsumer implements TupleWriter {
    private final TupleWriter tupleWriter;

    public ShufflingWriter(TupleWriter tupleWriter, NodeEngine nodeEngine, ContainerContext containerContext) {
        super(tupleWriter, nodeEngine, containerContext);
        this.tupleWriter = tupleWriter;
    }

    @Override
    public SinkTapWriteStrategy getSinkTapWriteStrategy() {
        return tupleWriter.getSinkTapWriteStrategy();
    }

    @Override
    public int getPartitionId() {
        return tupleWriter.getPartitionId();
    }

    @Override
    public boolean isPartitioned() {
        return tupleWriter.isPartitioned();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return tupleWriter.getPartitionStrategy();
    }

    @Override
    public void open() {
        this.tupleWriter.open();
    }

    @Override
    public void close() {
        this.tupleWriter.close();
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return tupleWriter.getShufflingStrategy();
    }

    @Override
    public boolean isFlushed() {
        return tupleWriter.isFlushed();
    }

    @Override
    public boolean isClosed() {
        return this.tupleWriter.isClosed();
    }

    @Override
    public int flush() {
        return tupleWriter.flush();
    }
}
