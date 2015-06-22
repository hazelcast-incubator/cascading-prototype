package com.hazelcast.yarn.impl.actor.shuffling;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;

public class ShufflingConsumer implements TupleConsumer {
    private final TupleConsumer baseConsumer;
    private final NodeEngineImpl nodeEngine;

    public ShufflingConsumer(TupleConsumer baseConsumer,
                             NodeEngine nodeEngine,
                             ContainerContext containerContext) {
        this.baseConsumer = baseConsumer;
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public int consumeChunk(TupleInputStream chunk) throws Exception {
        return this.baseConsumer.consumeChunk(chunk);
    }

    @Override
    public int consumeTuple(Tuple tuple) throws Exception {
        return this.baseConsumer.consumeTuple(tuple);
    }

    @Override
    public int flush() {
        return this.baseConsumer.flush();
    }

    @Override
    public boolean isFlushed() {
        return this.baseConsumer.isFlushed();
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public void open() {
        this.baseConsumer.open();
    }

    @Override
    public void close() {
        this.baseConsumer.close();
    }

    @Override
    public int lastConsumedCount() {
        return this.baseConsumer.lastConsumedCount();
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.baseConsumer.getShufflingStrategy();
    }

    @Override
    public boolean consume(TupleInputStream chunk) throws Exception {
        return this.consumeChunk(chunk) > 0;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }
}