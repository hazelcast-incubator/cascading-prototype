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

import java.nio.ByteBuffer;
import java.io.IOException;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;

import com.hazelcast.jet.api.executor.Payload;
import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.api.JetApplicationManager;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.api.application.ApplicationContext;

public class DefaultSocketThreadAcceptor extends DefaultSocketReader {
    private final ServerSocketChannel serverSocketChannel;
    private final JetApplicationManager jetApplicationManager;

    private long lastConnectionsTimeChecking = -1;

    public DefaultSocketThreadAcceptor(
            JetApplicationManager jetApplicationManager,
            NodeEngine nodeEngine,
            ServerSocketChannel serverSocketChannel
    ) {
        super(nodeEngine);
        this.serverSocketChannel = serverSocketChannel;
        this.jetApplicationManager = jetApplicationManager;
    }

    protected boolean consumePacket(JetPacket packet) throws Exception {
        if (packet.getHeader() == JetPacket.HEADER_JET_MEMBER_EVENT) {
            ApplicationContext applicationContext = this.jetApplicationManager.getApplicationContext(
                    new String(packet.getApplicationNameBytes())
            );
            Address address = applicationContext.getNodeEngine().getSerializationService().toObject(
                    new HeapData(packet.toByteArray())
            );
            clarifyBuffer(this.receiveBuffer);
            SocketReader reader = applicationContext.getSocketReaders().get(address);
            reader.setSocketChannel(this.socketChannel, this.receiveBuffer);
            this.socketChannel = null;
            this.receiveBuffer = null;
        }

        return false;
    }

    @Override
    public boolean executeTask(Payload payload) {
        if ((this.lastConnectionsTimeChecking > 0)
                &&
                (System.currentTimeMillis() - this.lastConnectionsTimeChecking
                        >=
                        JetApplicationConfig.DEFAULT_CONNECTIONS_CHECKING_INTERVAL_MS)
                ) {
            checkSocketChannels();
            this.lastConnectionsTimeChecking = System.currentTimeMillis();
        }

        if (this.socketChannel != null) {
            if (this.receiveBuffer == null) {
                this.receiveBuffer = ByteBuffer.allocateDirect(JetApplicationConfig.DEFAULT_TCP_BUFFER_SIZE);
            }

            super.executeTask(payload);
        }

        try {
            SocketChannel socketChannel = this.serverSocketChannel.accept();

            if (socketChannel != null) {
                payload.set(true);
                this.socketChannel = socketChannel;
            } else {
                payload.set(false);
            }
        } catch (IOException e) {
            return true;
        }

        return true;
    }

    private void checkSocketChannels() {
        for (ApplicationContext applicationContext : this.jetApplicationManager.getApplicationContexts()) {
            for (SocketReader socketReader : applicationContext.getSocketReaders().values()) {
                if ((socketReader.lastExecutionTimeOut() > 0)
                        &&
                        (System.currentTimeMillis() - socketReader.lastExecutionTimeOut()
                                >
                                JetApplicationConfig.DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS)) {
                    socketReader.closeSocket();
                }
            }

            for (SocketWriter socketWriter : applicationContext.getSocketWriters().values()) {
                if ((socketWriter.lastExecutionTimeOut() > 0)
                        &&
                        (System.currentTimeMillis() - socketWriter.lastExecutionTimeOut()
                                >
                                JetApplicationConfig.DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS)) {
                    socketWriter.closeSocket();
                }
            }
        }
    }

    @Override
    public void finalizeTask() {
        try {
            this.serverSocketChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
