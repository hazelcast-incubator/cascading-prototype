/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.actor.shuffling.io;

import java.util.List;
import java.io.IOException;

import com.hazelcast.jet.impl.JetUtil;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.jet.api.actor.Consumer;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.data.CompletionAwareProducer;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.api.strategy.DataTransferringStrategy;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;

public class ShufflingReceiver implements ObjectProducer, Consumer<JetPacket>, CompletionAwareProducer {

    private final ObjectDataInput in;

    private final ContainerContext containerContext;

    private final List<ProducerCompletionHandler> handlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();

    private volatile int lastProducedCount;

    private volatile int dataChunkLength = -1;

    private Object[] dataChunkBuffer;

    private final ChunkedInputStream chunkReceiver;

    private volatile boolean interrupted;

    private volatile boolean closed;

    private final RingBufferActor ringBufferActor;

    private final DefaultObjectIOStream<JetPacket> packetBuffers;

    private Object[] packets;

    private int lastPacketIdx;

    private int lastProducedPacketsCount;

    private ReceiverObjectReader receiverObjectReader;

    public ShufflingReceiver(ContainerContext containerContext,
                             ContainerTask containerTask) {
        this.containerContext = containerContext;

        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        ApplicationContext applicationContext = containerContext.getApplicationContext();
        JetApplicationConfig jetApplicationConfig = applicationContext.getJetApplicationConfig();
        int chunkSize = jetApplicationConfig.getChunkSize();

        this.ringBufferActor = new RingBufferActor(
                nodeEngine,
                containerContext.getApplicationContext(),
                containerTask,
                containerContext.getVertex()
        );

        this.packetBuffers = new DefaultObjectIOStream<JetPacket>(new JetPacket[chunkSize]);
        this.chunkReceiver = new ChunkedInputStream(this.packetBuffers);
        this.in = new ObjectDataInputStream(this.chunkReceiver, nodeEngine.getSerializationService());

        this.receiverObjectReader = new ReceiverObjectReader(
                this.in,
                containerContext.getApplicationContext().getObjectReaderFactory()
        );
    }

    @Override
    public void open() {
        this.interrupted = false;
        this.closed = false;
        this.chunkReceiver.onOpen();
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public DataTransferringStrategy getDataTransferringStrategy() {
        return ByReferenceDataTransferringStrategy.INSTANCE;
    }

    @Override
    public boolean consume(JetPacket packet) throws Exception {
        this.ringBufferActor.consumeObject(packet);
        return true;
    }

    @Override
    public Object[] produce() {
        if (this.interrupted) {
            return null;
        }

        if (this.packets != null) {
            return processPackets();
        }

        this.packets = this.ringBufferActor.produce();
        this.lastProducedPacketsCount = this.ringBufferActor.lastProducedCount();

        if ((JetUtil.isEmpty(this.packets))) {
            if (this.closed) {
                for (ProducerCompletionHandler handler : this.handlers) {
                    handler.onComplete(this);
                }

                this.interrupted = true;
            }

            return null;
        }

        return processPackets();
    }

    private Object[] processPackets() {
        for (int i = this.lastPacketIdx; i < this.lastProducedPacketsCount; i++) {
            JetPacket packet = (JetPacket) this.packets[i];

            try {
                if (packet.getHeader() == JetPacket.HEADER_JET_DATA_CHUNK_SENT) {
                    deserializePackets();

                    if (i == this.lastProducedPacketsCount - 1) {
                        reset();
                    } else {
                        this.lastPacketIdx = i + 1;
                    }

                    return this.dataChunkBuffer;
                } else {
                    this.packetBuffers.consume(packet);
                }
            } catch (Throwable error) {
                error.printStackTrace(System.out);
                this.containerContext.getApplicationContext().getApplicationMaster().invalidateApplicationInCluster(error);
            }
        }

        reset();
        return null;
    }

    private void reset() {
        this.packets = null;
        this.lastPacketIdx = 0;
        this.lastProducedPacketsCount = 0;

    }

    private void deserializePackets() throws IOException {
        if (this.dataChunkLength == -1) {
            this.dataChunkLength = this.in.readInt();
            this.dataChunkBuffer = new Object[this.dataChunkLength];
        }

        try {
            this.lastProducedCount = this.receiverObjectReader.read(this.dataChunkBuffer, this.dataChunkLength);
        } finally {
            this.dataChunkLength = -1;
        }

        this.packetBuffers.reset();
    }

    @Override
    public int lastProducedCount() {
        return this.lastProducedCount;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public Vertex getVertex() {
        return containerContext.getVertex();
    }

    @Override
    public String getName() {
        return getVertex().getName();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        this.handlers.add(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.handlers) {
            handler.onComplete(this);
        }
    }

    public void markInterrupted() {
        this.interrupted = true;
    }

    public RingBufferActor getRingBufferActor() {
        return ringBufferActor;
    }
}
