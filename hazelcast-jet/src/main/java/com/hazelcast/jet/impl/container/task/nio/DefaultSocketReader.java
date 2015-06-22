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

package com.hazelcast.jet.impl.container.task.nio;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;

import java.nio.channels.SocketChannel;

import com.hazelcast.jet.api.executor.Payload;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;

public class DefaultSocketReader implements SocketReader {
    protected volatile ByteBuffer receiveBuffer;

    protected volatile SocketChannel socketChannel;

    private JetPacket packet;

    private final int chunkSize;

    private final Address jetAddress;

    private final ILogger logger;

    private final ApplicationContext applicationContext;

    private final DefaultObjectIOStream<JetPacket> buffer;

    private final List<RingBufferActor> consumers = new ArrayList<RingBufferActor>();

    private final Map<Address, SocketWriter> writers = new HashMap<Address, SocketWriter>();

    private volatile long lastExecutionTimeOut = -1;

    public DefaultSocketReader(ApplicationContext applicationContext, Address jetAddress) {
        this.jetAddress = jetAddress;
        this.applicationContext = applicationContext;
        this.logger = applicationContext.getNodeEngine().getLogger(DefaultSocketReader.class);
        this.chunkSize = applicationContext.getJetApplicationConfig().getChunkSize();
        this.buffer = new DefaultObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public DefaultSocketReader(NodeEngine nodeEngine) {
        this.jetAddress = null;
        this.applicationContext = null;
        this.logger = nodeEngine.getLogger(DefaultSocketReader.class);
        this.chunkSize = JetApplicationConfig.DEFAULT_STREAM_CHUNK_SIZE;
        this.buffer = new DefaultObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    @Override
    public long lastExecutionTimeOut() {
        return lastExecutionTimeOut;
    }

    @Override
    public boolean executeTask(Payload payload) {
        this.lastExecutionTimeOut = System.currentTimeMillis();

        if (!isFlushed()) {
            payload.set(false);
            return true;
        }

        try {
            if (this.socketChannel != null) {
                try {
                    int readBytes = this.socketChannel.read(this.receiveBuffer);

                    if (readBytes <= 0) {
                        if (readBytes < 0) {
                            finalizeTask();
                            return true;
                        }

                        payload.set(false);
                        return readBytes == 0;
                    }

                    this.receiveBuffer.flip();

                    payload.set(true);

                    if (!readPackets()) {
                        return true;
                    }

                    flush();
                    clarifyBuffer(this.receiveBuffer);
                } catch (java.io.IOException e) {
                    closeSocket();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return true;
            } else {
                payload.set(false);
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void clarifyBuffer(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            buffer.compact();
        } else {
            buffer.clear();
        }
    }

    protected boolean consumePacket(JetPacket packet) throws Exception {
        this.buffer.consume(packet);
        return true;
    }

    protected boolean readPackets() throws Exception {
        while (this.receiveBuffer.hasRemaining()) {
            if (this.packet == null) {
                this.packet = new JetPacket();
            }

            if (!this.packet.readFrom(this.receiveBuffer)) {
                return true;
            }

            if (!consumePacket(this.packet)) {
                this.packet = null;
                return false;
            }

            this.packet = null;

            if (this.buffer.size() >= this.chunkSize) {
                flush();
                return true;
            }
        }

        return true;
    }

    private void flush() throws Exception {
        if (this.buffer.size() > 0) {
            for (JetPacket packet : this.buffer) {
                int header = resolvePacket(packet);

                if (header > 0) {
                    sendResponse(packet, header);
                }
            }

            this.buffer.reset();
        }
    }

    private void sendResponse(JetPacket jetPacket, int header) throws Exception {
        jetPacket.reset();
        jetPacket.setHeader(header);
        this.writers.get(this.jetAddress).sendServicePacket(jetPacket);
    }

    @Override
    public boolean isFlushed() {
        boolean isFlushed = true;

        for (int i = 0; i < this.consumers.size(); i++) {
            isFlushed &= this.consumers.get(i).isFlushed();
        }

        return isFlushed;
    }

    @Override
    public void finalizeTask() {
        try {
            for (RingBufferActor consumer : this.consumers) {
                consumer.clear();
            }
        } finally {
            if (this.socketChannel != null) {
                try {
                    this.socketChannel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void setSocketChannel(SocketChannel socketChannel,
                                 ByteBuffer receiveBuffer) {
        this.receiveBuffer = receiveBuffer;
        this.socketChannel = socketChannel;
        try {
            this.socketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerConsumer(RingBufferActor ringBufferActor) {
        this.consumers.add(ringBufferActor);
    }

    //CHECKSTYLE:OFF
    public int resolvePacket(JetPacket packet) throws Exception {
        int header = packet.getHeader();

        switch (header) {
            /*Request - bytes for tuple chunk*/
            /*Request shuffling channel closed*/
            case JetPacket.HEADER_JET_DATA_CHUNK:
            case JetPacket.HEADER_JET_SHUFFLER_CLOSED:
            case JetPacket.HEADER_JET_DATA_CHUNK_SENT:
                return notifyShufflingReceiver(packet);

            case JetPacket.HEADER_JET_INVALIDATE_APPLICATION:
                packet.setRemoteMember(this.jetAddress);
                this.applicationContext.getApplicationMaster().invalidateApplicationLocal(packet);
                return 0;

            case JetPacket.HEADER_JET_DATA_NO_APP_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_MEMBER_FAILURE:
            case JetPacket.HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE:
            case JetPacket.HEADER_JET_UNKNOWN_EXCEPTION_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE:
            case JetPacket.HEADER_JET_APPLICATION_IS_NOT_EXECUTING:
                invalidateAll();
                return 0;

            case JetPacket.HEADER_JET_CONTAINER_STARTED:
                return applicationContext.getApplicationMaster().notifyContainersExecution(packet);

            default:
                return 0;
        }
    }
    //CHECKSTYLE:ON

    private void invalidateAll() throws Exception {
        for (SocketWriter sender : this.writers.values()) {
            JetPacket jetPacket = new JetPacket(
                    this.applicationContext.getName().getBytes()
            );

            jetPacket.setHeader(JetPacket.HEADER_JET_INVALIDATE_APPLICATION);
            sender.sendServicePacket(jetPacket);
        }
    }

    private int notifyShufflingReceiver(JetPacket packet) throws Exception {
        ApplicationMaster applicationMaster = this.applicationContext.getApplicationMaster();
        DataContainer dataContainer = applicationMaster.getContainersCache().get(packet.getContainerId());

        if (dataContainer == null) {
            this.logger.warning("No such container with containerId="
                            + packet.getContainerId()
                            + " jetPacket="
                            + packet
                            + ". Application will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE;
        }

        ContainerTask containerTask = dataContainer.getTasksCache().get(packet.getTaskID());

        if (containerTask == null) {
            this.logger.warning("No such task in container with containerId="
                            + packet.getContainerId()
                            + " taskId="
                            + packet.getTaskID()
                            + " jetPacket="
                            + packet
                            + ". Application will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE;
        }

        ShufflingReceiver receiver = containerTask.getShufflingReceiver(this.jetAddress);

        if (packet.getHeader() == JetPacket.HEADER_JET_SHUFFLER_CLOSED) {
            receiver.close();
        } else {
            receiver.consume(packet);
        }

        return 0;
    }

    @Override
    public void assignWriter(Address writeAddress,
                             SocketWriter socketWriter) {
        this.writers.put(writeAddress, socketWriter);
    }

    @Override
    public void closeSocket() {
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (IOException e) {
                logger.warning(e.getMessage(), e);
            }
            this.socketChannel = null;
        }
    }
}
