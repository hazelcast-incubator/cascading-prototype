package com.hazelcast.yarn.impl.actor.shuffling;


import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.actor.TupleActor;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;

public class ShufflingActor extends ShufflingProducer implements TupleActor {
    private final TupleActor baseActor;
    private final TupleConsumer tupleConsumer;

    public ShufflingActor(TupleActor baseActor, NodeEngine nodeEngine, ContainerContext containerContext) {
        super(baseActor, nodeEngine, containerContext);
        this.baseActor = baseActor;
        this.tupleConsumer = new ShufflingConsumer(baseActor, nodeEngine, containerContext);
    }

    @Override
    public ContainerTask getTask() {
        return this.baseActor.getTask();
    }

    @Override
    public int consumeChunk(TupleInputStream chunk) throws Exception {
        return this.tupleConsumer.consumeChunk(chunk);
    }

    @Override
    public int consumeTuple(Tuple tuple) throws Exception {
        return this.tupleConsumer.consumeTuple(tuple);
    }

    @Override
    public boolean isShuffled() {
        return this.tupleConsumer.isShuffled();
    }

    @Override
    public String getName() {
        return baseActor.getName();
    }

    @Override
    public int flush() {
        return this.tupleConsumer.flush();
    }

    @Override
    public boolean isFlushed() {
        return this.tupleConsumer.isFlushed();
    }

    @Override
    public int lastConsumedCount() {
        return tupleConsumer.lastConsumedCount();
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.tupleConsumer.getShufflingStrategy();
    }

    @Override
    public boolean consume(TupleInputStream inputStream) throws Exception {
        return this.tupleConsumer.consume(inputStream);
    }
}