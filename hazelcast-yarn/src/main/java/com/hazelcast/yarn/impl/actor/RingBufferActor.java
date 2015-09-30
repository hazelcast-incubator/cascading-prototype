package com.hazelcast.yarn.impl.actor;

import java.util.List;
import java.util.Arrays;

import com.hazelcast.spi.NodeEngine;

import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.actor.TupleActor;
import com.hazelcast.yarn.api.ShufflingStrategy;

import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.impl.actor.ringbuffer.RingBuffer;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.impl.tuple.io.DefaultTupleIOStream;
import com.hazelcast.yarn.api.application.ApplicationListener;
import com.hazelcast.yarn.api.actor.ProducerCompletionHandler;


public class RingBufferActor implements TupleActor {
    private int producedCount;
    private int lastConsumedCount;
    private int currentFlushedCount;

    private final Edge edge;
    private final Vertex vertex;
    private final ContainerTask task;
    private final Tuple[] producerChunk;
    private final RingBuffer<Tuple> ringBuffer;
    private final DefaultTupleIOStream flushBuffer;
    private final List<ProducerCompletionHandler> completionHandlers;

    private volatile boolean isClosed;

    public RingBufferActor(NodeEngine nodeEngine,
                           ApplicationContext applicationContext,
                           ContainerTask task,
                           Vertex vertex,
                           Edge edge) {
        this.edge = edge;
        this.task = task;
        this.vertex = vertex;
        YarnApplicationConfig yarnApplicationConfig = nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName());
        int tupleChunkSize = yarnApplicationConfig.getTupleChunkSize();
        this.producerChunk = new Tuple[tupleChunkSize];
        int containerQueueSize = yarnApplicationConfig.getContainerQueueSize();
        this.flushBuffer = new DefaultTupleIOStream(new Tuple[tupleChunkSize]);
        this.completionHandlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();
        this.ringBuffer = new RingBuffer<Tuple>(containerQueueSize, nodeEngine.getLogger(RingBufferActor.class));

        applicationContext.registerApplicationListener(new ApplicationListener() {
            @Override
            public void onApplicationExecuted(ApplicationContext applicationContext) {
                Arrays.fill(producerChunk, null);
            }
        });
    }

    private int flushChunk() {
        if (this.currentFlushedCount >= this.flushBuffer.size()) {
            return 0;
        }

        int acquired = this.ringBuffer.acquire(this.flushBuffer.size() - this.currentFlushedCount);

        if (acquired <= 0) {
            return 0;
        }

        this.ringBuffer.commit(this.flushBuffer, this.currentFlushedCount);
        return acquired;
    }

    @Override
    public boolean consume(TupleInputStream chunk) throws Exception {
        return this.consumeChunk(chunk) > 0;
    }

    @Override
    public int consumeChunk(TupleInputStream chunk) throws Exception {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.consumeStream(chunk);
        return chunk.size();
    }

    @Override
    public int consumeTuple(Tuple tuple) throws Exception {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.consume(tuple);
        return 1;
    }

    @Override
    public int flush() {
        if (this.flushBuffer.size() > 0) {
            try {
                int flushed = this.flushChunk();
                this.lastConsumedCount = flushed;
                this.currentFlushedCount += flushed;
                return flushed;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return 0;
    }

    @Override
    public boolean isFlushed() {
        if (this.flushBuffer.size() == 0) {
            return true;
        }

        if (this.currentFlushedCount < this.flushBuffer.size()) {
            this.flush();
        }

        boolean flushed = this.currentFlushedCount >= this.flushBuffer.size();

        if (flushed) {
            this.currentFlushedCount = 0;
            this.flushBuffer.reset();
        }

        return flushed;
    }

    @Override
    public Tuple[] produce() {
        this.producedCount = this.ringBuffer.fetch(this.producerChunk);

        if (this.producedCount <= 0) {
            return null;
        }

        return this.producerChunk;
    }

    @Override
    public int lastProducedCount() {
        return this.producedCount;
    }

    @Override
    public ContainerTask getTask() {
        return this.task;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        this.completionHandlers.add(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.completionHandlers) {
            handler.onComplete(this);
        }
    }

    @Override
    public boolean isShuffled() {
        return false;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String getName() {
        return this.getVertex().getName();
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public void open() {
        this.isClosed = false;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return edge.getShufflingStrategy();
    }
}
