package com.hazelcast.yarn.impl.actor.shuffling;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.actor.ProducerCompletionHandler;

public class ShufflingProducer implements TupleProducer {
    private final TupleProducer tupleProducer;
    private final NodeEngineImpl nodeEngine;
    private final ContainerContext containerContext;

    public ShufflingProducer(TupleProducer tupleProducer, NodeEngine nodeEngine, ContainerContext containerContext) {
        this.tupleProducer = tupleProducer;
        this.containerContext = containerContext;
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public Tuple[] produce() {
        return this.tupleProducer.produce();
    }

    @Override
    public int lastProducedCount() {
        return this.tupleProducer.lastProducedCount();
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public Vertex getVertex() {
        return tupleProducer.getVertex();
    }

    @Override
    public String getName() {
        return tupleProducer.getName();
    }

    @Override
    public boolean isClosed() {
        return tupleProducer.isClosed();
    }

    @Override
    public void open() {
        tupleProducer.open();
    }

    @Override
    public void close() {
        tupleProducer.close();
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        tupleProducer.registerCompletionHandler(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        tupleProducer.handleProducerCompleted();
    }
}
