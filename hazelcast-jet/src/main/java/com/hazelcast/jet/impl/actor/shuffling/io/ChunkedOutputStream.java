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

import java.util.Arrays;
import java.io.IOException;
import java.io.OutputStream;

import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.internal.serialization.impl.HeapData;

public class ChunkedOutputStream extends OutputStream {
    private static final int BUFFER_OFFSET = HeapData.DATA_OFFSET;

    private final int taskID;

    private int bufferSize;

    private final byte[] buffer;

    private final int containerID;

    private final int shufflingBytesSize;

    private final byte[] applicationName;

    private volatile boolean interrupted;

    private final RingBufferActor ringBufferActor;

    public ChunkedOutputStream(RingBufferActor ringBufferActor, ContainerContext containerContext, int taskID) {
        this.taskID = taskID;
        this.ringBufferActor = ringBufferActor;
        this.shufflingBytesSize = containerContext.getApplicationContext().getJetApplicationConfig().getShufflingBatchSizeBytes();
        this.buffer = new byte[BUFFER_OFFSET + this.shufflingBytesSize];
        this.applicationName = containerContext.getApplicationContext().getName().getBytes();
        this.containerID = containerContext.getID();
    }

    @Override
    public void write(int b) throws IOException {
        if (this.interrupted) {
            return;
        }

        this.buffer[BUFFER_OFFSET + this.bufferSize++] = (byte) b;

        if (this.bufferSize >= this.shufflingBytesSize) {
            try {
                flushBuffer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void flushBuffer() throws Exception {
        try {
            if (this.interrupted) {
                return;
            }

            if (this.bufferSize > 0) {
                byte[] buffer = new byte[BUFFER_OFFSET + this.bufferSize];
                System.arraycopy(this.buffer, 0, buffer, 0, BUFFER_OFFSET + this.bufferSize);

                JetPacket packet = new JetPacket(
                        this.taskID,
                        this.containerID,
                        this.applicationName,
                        buffer
                );

                packet.setHeader(JetPacket.HEADER_JET_DATA_CHUNK);

                this.ringBufferActor.consumeObject(packet);
            }
        } finally {
            Arrays.fill(this.buffer, (byte) 0);
            this.bufferSize = 0;
        }
    }

    public void onOpen() {
        this.bufferSize = 0;
        this.interrupted = false;
        Arrays.fill(this.buffer, (byte) 0);
    }

    public void flushSender() throws Exception {
        flushBuffer();
    }

    public void markInterrupted() {
        this.interrupted = true;
    }
}
