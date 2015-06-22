package com.hazelcast.yarn.impl.actor.shuffling.io;

import java.util.Arrays;
import java.io.IOException;
import java.io.OutputStream;

import com.hazelcast.nio.Address;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.impl.hazelcast.YarnPacket;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.internal.serialization.impl.HeapData;

public class ChunkedOutputStream extends OutputStream {
    private static final int BUFFER_OFFSET = HeapData.DATA_OFFSET;

    private final Node node;

    private final int taskID;

    private int bufferSize = 0;

    private final byte[] buffer;

    private final Address target;

    private final int containerID;

    private final int shufflingBytesSize;

    private final byte[] applicationName;

    private final NodeEngineImpl nodeEngine;

    private volatile boolean interrupted = false;

    public ChunkedOutputStream(ContainerContext containerContext, Member member, int taskID) {
        this.taskID = taskID;
        this.target = member.getAddress();
        this.nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        this.node = this.nodeEngine.getNode();
        this.shufflingBytesSize = containerContext.getApplicationContext().getYarnApplicationConfig().getShufflingBatchSizeBytes();
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
            this.flushBuffer();
        }
    }

    private void flushBuffer() throws IOException {
        try {
            if (this.interrupted) {
                return;
            }
            if (this.bufferSize <= 0) {
                return;
            }

            Connection connection = this.node.getConnectionManager().getOrConnect(this.target);

            byte[] buffer = new byte[BUFFER_OFFSET + this.bufferSize];
            System.arraycopy(this.buffer, 0, buffer, 0, BUFFER_OFFSET + this.bufferSize);

            YarnPacket packet = new YarnPacket(
                    this.taskID,
                    this.containerID,
                    this.applicationName,
                    buffer
            );

            packet.setHeader(YarnPacket.HEADER_YARN_TUPLE_CHUNK);

            if (!this.nodeEngine.getNode().getConnectionManager().transmit(packet, connection)) {
                throw new IOException();
            }
        } finally {
            this.bufferSize = 0;
        }
    }

    public void onOpen() {
        this.bufferSize = 0;
        this.interrupted = false;
        Arrays.fill(this.buffer, (byte) 0);
    }

    public void flushSender() throws IOException {
        this.flushBuffer();
    }

    public void markInterrupted() {
        this.interrupted = true;
    }
}
