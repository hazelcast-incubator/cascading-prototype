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

package com.hazelcast.jet.api.data.io;

import java.nio.ByteBuffer;

import com.hazelcast.nio.Address;

import java.nio.channels.SocketChannel;

import com.hazelcast.jet.impl.actor.RingBufferActor;

/**
 * Represents abstract task to read from network socket;
 * <p/>
 * The architecture is following:
 * <p/>
 * <p/>
 * <pre>
 *  SocketChannel (JetAddress)  -> SocketReader -> Consumer(ringBuffer)
 * </pre>
 */
public interface SocketReader extends NetworkTask {
    /**
     * @return - true of last write to the consumer-queue has been flushed;
     */
    boolean isFlushed();

    /**
     * Assign corresponding socketChannel to reader-task;
     *
     * @param socketChannel - network socketChannel;
     * @param receiveBuffer - byteBuffer to be used to read data from socket;
     */
    void setSocketChannel(SocketChannel socketChannel,
                          ByteBuffer receiveBuffer);

    /**
     * Register output ringBuffer consumer;
     *
     * @param ringBufferActor - corresponding output consumer;
     */
    void registerConsumer(RingBufferActor ringBufferActor);

    /**
     * Assigns corresponding socket writer on the opposite node;
     *
     * @param writeAddress - JET's member node address;
     * @param socketWriter - SocketWriter task;
     */
    void assignWriter(Address writeAddress,
                      SocketWriter socketWriter);
}
