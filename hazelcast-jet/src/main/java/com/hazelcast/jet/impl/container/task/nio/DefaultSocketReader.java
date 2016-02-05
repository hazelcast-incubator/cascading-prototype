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

package com.hazelcast.jet.impl.container.task.nio;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.nio.channels.SocketChannel;

import com.hazelcast.jet.api.executor.Payload;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;

public class DefaultSocketReader
        extends AbstractNetworkTask implements SocketReader {
    protected volatile ByteBuffer receiveBuffer;

    private JetPacket packet;

    private final int chunkSize;

    private final Address jetAddress;

    private final ApplicationContext applicationContext;

    private final DefaultObjectIOStream<JetPacket> buffer;

    private final List<RingBufferActor> consumers = new ArrayList<RingBufferActor>();

    private final Map<Address, SocketWriter> writers = new HashMap<Address, SocketWriter>();

    public DefaultSocketReader(ApplicationContext applicationContext,
                               Address jetAddress) {
        super(applicationContext.getNodeEngine());
        this.jetAddress = jetAddress;
        this.applicationContext = applicationContext;
        this.chunkSize = applicationContext.getJetApplicationConfig().getChunkSize();
        this.buffer = new DefaultObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public DefaultSocketReader(NodeEngine nodeEngine) {
        super(nodeEngine);
        System.out.println("DefaultSocketAcceptor.constructor");
        this.jetAddress = null;
        this.applicationContext = null;
        this.chunkSize = JetApplicationConfig.DEFAULT_CHUNK_SIZE;
        this.buffer = new DefaultObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    @Override
    public boolean onExecute(Payload payload) throws Exception {
        if (this.destroyed) {
            closeSocket();
            return false;
        }

        if (this.interrupted) {
            closeSocket();
            this.finalized = true;
            notifyAMTaskFinished();
            return false;
        }

        if (!isFlushed()) {
            payload.set(false);
            return true;
        }

        process(payload);

        if (this.waitingForFinish) {
            if ((!payload.produced()) && (isFlushed())) {
                this.finished = true;
            }
        }

        return checkFinished();
    }

    @Override
    protected void notifyAMTaskFinished() {
        this.applicationContext.getApplicationMaster().notifyNetworkTaskFinished();
    }

    private boolean process(Payload payload) {
        if ((this.socketChannel != null) && (this.socketChannel.isConnected())) {
            try {
                int readBytes = this.socketChannel.read(this.receiveBuffer);

                if (readBytes <= 0) {
                    if (readBytes < 0) {
                        closeSocket();
                        return false;
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
            } catch (IOException e) {
                closeSocket();
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }

            return true;
        } else {
            payload.set(false);
            return true;
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
    public void setSocketChannel(SocketChannel socketChannel,
                                 ByteBuffer receiveBuffer) {
        this.receiveBuffer = receiveBuffer;
        this.socketChannel = socketChannel;
        try {
            this.socketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public void registerConsumer(RingBufferActor ringBufferActor) {
        this.consumers.add(ringBufferActor);
    }

    @Override
    public void assignWriter(Address writeAddress,
                             SocketWriter socketWriter) {
        this.writers.put(writeAddress, socketWriter);
    }

    public int resolvePacket(JetPacket packet) throws Exception {
        int header = packet.getHeader();

        switch (header) {
            /*Request - bytes for tuple chunk*/
            /*Request shuffling channel closed*/
            case JetPacket.HEADER_JET_DATA_CHUNK:
            case JetPacket.HEADER_JET_SHUFFLER_CLOSED:
            case JetPacket.HEADER_JET_DATA_CHUNK_SENT:
                return notifyShufflingReceiver(packet);

            case JetPacket.HEADER_JET_EXECUTION_ERROR:
                packet.setRemoteMember(this.jetAddress);
                this.applicationContext.getApplicationMaster().notifyContainers(packet);
                return 0;

            case JetPacket.HEADER_JET_DATA_NO_APP_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_MEMBER_FAILURE:
            case JetPacket.HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE:
            case JetPacket.HEADER_JET_APPLICATION_IS_NOT_EXECUTING:
                invalidateAll();
                return 0;

            default:
                return 0;
        }
    }

    private void invalidateAll() throws Exception {
        for (SocketWriter sender : this.writers.values()) {
            JetPacket jetPacket = new JetPacket(
                    this.applicationContext.getName().getBytes()
            );

            jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
            sender.sendServicePacket(jetPacket);
        }
    }

    private int notifyShufflingReceiver(JetPacket packet) throws Exception {
        ApplicationMaster applicationMaster = this.applicationContext.getApplicationMaster();
        ProcessingContainer processingContainer = applicationMaster.getContainersCache().get(packet.getContainerId());

        if (processingContainer == null) {
            this.logger.warning("No such container with containerId="
                            + packet.getContainerId()
                            + " jetPacket="
                            + packet
                            + ". Application will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE;
        }

        ContainerTask containerTask = processingContainer.getTasksCache().get(packet.getTaskID());

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

        containerTask.getShufflingReceiver(this.jetAddress).consume(packet);
        return 0;
    }
}
