package com.hazelcast.yarn.impl.actor.shuffling.io;

import com.hazelcast.nio.Address;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.impl.hazelcast.YarnPacket;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.impl.tap.sink.AbstractHazelcastWriter;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.internal.serialization.ObjectDataOutputStream;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ShufflingSender extends AbstractHazelcastWriter {
    private final Node node;
    private final int taskID;
    private final Address target;
    private final int containerID;
    private final byte[] applicationName;
    private final NodeEngineImpl nodeEngine;
    private final ChunkedOutputStream chunkSender;
    private final ObjectDataOutputStream outPutStream;

    private volatile boolean closed = false;

    public ShufflingSender(Member member, ContainerContext containerContext, int taskID) {
        super(containerContext, -1, SinkTapWriteStrategy.APPEND);
        this.taskID = taskID;
        this.target = member.getAddress();
        this.nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        this.node = this.nodeEngine.getNode();
        this.chunkSender = new ChunkedOutputStream(containerContext, member, taskID);
        this.outPutStream = new ObjectDataOutputStream(this.chunkSender, this.nodeEngine.getSerializationService());
        this.containerID = containerContext.getID();
        String name = containerContext.getApplicationContext().getName();
        this.applicationName = name.getBytes();
    }

    public static AtomicInteger send = new AtomicInteger(0);
    public static AtomicInteger sendT = new AtomicInteger(0);

    @Override
    public int consumeChunk(TupleInputStream chunk) throws Exception {
        if (chunk.size() == 0) {
            return 0;
        }

        this.outPutStream.writeInt(chunk.size());

        for (int i = 0; i < chunk.size(); i++) {
            Tuple tuple = chunk.get(i);

            if (this.closed) {
                return -1;
            }

            sendT.incrementAndGet();

            this.outPutStream.writeObject(tuple);
        }

        this.chunkSender.flushSender();

        int consumedSize = chunk.size();

        this.resetBuffer();
        return consumedSize;
    }

    @Override
    public boolean isFlushed() {
        return true;
    }

    @Override
    public void open() {
        this.onOpen();
    }

    @Override
    protected void onOpen() {
        this.closed = false;
        this.reset();
        this.chunkSender.onOpen();
    }

    @Override
    protected void processChunk(TupleInputStream chunk) {
        try {
            this.consumeChunk(chunk);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static AtomicInteger counter = new AtomicInteger(0);

    private final int i = counter.incrementAndGet();

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            this.reset();

            YarnPacket packet = new YarnPacket(
                    this.taskID,
                    this.containerID,
                    this.applicationName
            );

            packet.setHeader(YarnPacket.HEADER_YARN_SHUFFLER_CLOSED);
            Connection connection = this.node.getConnectionManager().getOrConnect(this.target);
            this.nodeEngine.getNode().getConnectionManager().transmit(packet, connection);
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    private void reset() {
        this.isFlushed = true;
    }

    public void markInterrupted() {
        this.chunkSender.markInterrupted();
        this.closed = true;
    }

    public void notifyProducersFinalizing() {
        YarnPacket packet = new YarnPacket(
                this.taskID,
                this.containerID,
                this.applicationName
        );

        this.reset();
        packet.setHeader(YarnPacket.HEADER_YARN_SHUFFLER_FINALIZING);
        Connection connection = this.node.getConnectionManager().getOrConnect(this.target);

        if (!this.nodeEngine.getNode().getConnectionManager().transmit(packet, connection)) {
            throw new RuntimeException(new IOException());
        }
    }
}