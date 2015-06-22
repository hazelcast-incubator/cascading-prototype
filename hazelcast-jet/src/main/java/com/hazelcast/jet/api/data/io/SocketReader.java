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

package com.hazelcast.jet.api.data.io;


import java.nio.ByteBuffer;

import com.hazelcast.nio.Address;

import java.nio.channels.SocketChannel;

import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.jet.impl.actor.RingBufferActor;

public interface SocketReader extends Task {
    long lastExecutionTimeOut();

    boolean isFlushed();

    void setSocketChannel(SocketChannel socketChannel,
                          ByteBuffer receiveBuffer);

    void registerConsumer(RingBufferActor ringBufferActor);

    void assignWriter(Address writeAddress,
                      SocketWriter socketWriter);

    void closeSocket();
}
