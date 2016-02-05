/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.actor;

import java.util.List;
import java.util.Arrays;


import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.api.actor.RingBuffer;
import com.hazelcast.jet.api.actor.ObjectActor;
import com.hazelcast.core.PartitioningStrategy;

import java.util.concurrent.CopyOnWriteArrayList;


import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.spi.application.ApplicationListener;
import com.hazelcast.jet.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.jet.impl.actor.ringbuffer.RingBufferWithValueStrategy;
import com.hazelcast.jet.impl.actor.ringbuffer.RingBufferWithReferenceStrategy;

public class RingBufferActor implements ObjectActor {
    private int producedCount;
    private int lastConsumedCount;
    private int currentFlushedCount;

    private final Edge edge;
    private final Vertex vertex;
    private final ContainerTask sourceTask;
    private final Object[] producerChunk;
    private final RingBuffer<Object> ringBuffer;
    private final DefaultObjectIOStream<Object> flushBuffer;
    private final List<ProducerCompletionHandler> completionHandlers;

    private volatile boolean isClosed;

    public RingBufferActor(NodeEngine nodeEngine,
                           ApplicationContext applicationContext,
                           ContainerTask sourceTask,
                           Vertex vertex) {
        this(nodeEngine, applicationContext, sourceTask, vertex, null, false);
    }

    public RingBufferActor(NodeEngine nodeEngine,
                           ApplicationContext applicationContext,
                           ContainerTask sourceTask,
                           Vertex vertex,
                           Edge edge) {
        this(nodeEngine, applicationContext, sourceTask, vertex, edge, true);
    }

    public RingBufferActor(NodeEngine nodeEngine,
                           ApplicationContext applicationContext,
                           ContainerTask sourceTask,
                           Vertex vertex,
                           Edge edge,
                           boolean registerListener) {
        this.edge = edge;
        this.sourceTask = sourceTask;
        this.vertex = vertex;
        JetApplicationConfig jetApplicationConfig = applicationContext.getJetApplicationConfig();
        int objectChunkSize = jetApplicationConfig.getChunkSize();
        this.producerChunk = new Object[objectChunkSize];
        int containerQueueSize = jetApplicationConfig.getContainerQueueSize();
        this.flushBuffer = new DefaultObjectIOStream<Object>(new Object[objectChunkSize]);
        this.completionHandlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();
        boolean byReference = edge == null || edge.getDataTransferringStrategy().byReference();

        this.ringBuffer = byReference
                ?
                new RingBufferWithReferenceStrategy<Object>(
                        containerQueueSize,
                        nodeEngine.getLogger(RingBufferActor.class)
                )
                :
                new RingBufferWithValueStrategy<Object>(
                        containerQueueSize,
                        nodeEngine.getLogger(RingBufferActor.class),
                        edge.getDataTransferringStrategy()
                );

        if (!byReference) {
            for (int i = 0; i < this.producerChunk.length; i++) {
                this.producerChunk[i] = edge.getDataTransferringStrategy().newInstance();
            }
        }

        if (registerListener) {
            applicationContext.registerApplicationListener(new ApplicationListener() {
                @Override
                public void onApplicationExecuted(ApplicationContext applicationContext) {
                    clear();
                }
            });
        }
    }

    public void clear() {
        Arrays.fill(this.producerChunk, null);
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
    public boolean consume(ProducerInputStream<Object> chunk) throws Exception {
        return consumeChunk(chunk) > 0;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.consumeStream(chunk);
        return chunk.size();
    }

    @Override
    public int consumeObject(Object object) throws Exception {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.consume(object);
        return 1;
    }

    @Override
    public int flush() {
        if (this.flushBuffer.size() > 0) {
            try {
                int flushed = flushChunk();
                this.lastConsumedCount = flushed;
                this.currentFlushedCount += flushed;
                return flushed;
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
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
            flush();
        }

        boolean flushed = this.currentFlushedCount >= this.flushBuffer.size();

        if (flushed) {
            this.currentFlushedCount = 0;
            this.flushBuffer.reset();
        }

        return flushed;
    }

    @Override
    public Object[] produce() {
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
    public ContainerTask getSourceTask() {
        return this.sourceTask;
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
        return this.vertex;
    }

    @Override
    public String getName() {
        return getVertex().getName();
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public void open() {
        this.ringBuffer.reset();
        this.isClosed = false;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    @Override
    public DataTransferringStrategy getDataTransferringStrategy() {
        return edge == null ? ByReferenceDataTransferringStrategy.INSTANCE : edge.getDataTransferringStrategy();
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.edge == null ? null : this.edge.getShufflingStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return this.edge == null ? StringPartitioningStrategy.INSTANCE : this.edge.getPartitioningStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return this.edge == null ? DefaultHashingStrategy.INSTANCE : this.edge.getHashingStrategy();
    }
}
