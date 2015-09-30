package com.hazelcast.yarn.impl.actor.shuffling.io;

import java.util.List;
import java.io.IOException;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.api.actor.Consumer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.tuple.CompletionAwareProducer;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.actor.ProducerCompletionHandler;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;

public class ShufflingReceiver implements TupleProducer, Consumer<byte[]>, CompletionAwareProducer {
    private final int chunkSize;
    private final ILogger logger;
    private final ObjectDataInput in;
    private final BlockingQueue<byte[]> bytesQueue;
    private final ContainerContext containerContext;
    private final ApplicationMaster applicationMaster;
    private final List<ProducerCompletionHandler> handlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();
    private final List<ProducerCompletionHandler> finalizingHandlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();

    private volatile int lastProducedCount = 0;
    private volatile int tupleChunkLength = -1;

    private int tupleChunkIndex = 0;
    private Tuple[] tupleChunkBuffer;

    private final ChunkedInputStream chunkReceiver;

    private volatile boolean interrupted = false;

    private volatile boolean closed = false;

    public static final AtomicInteger receivedT = new AtomicInteger(0);

    public ShufflingReceiver(ContainerContext containerContext) {
        this.containerContext = containerContext;
        this.applicationMaster = this.containerContext.getApplicationContext().getApplicationMaster();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        ApplicationContext applicationContext = containerContext.getApplicationContext();
        YarnApplicationConfig yarnApplicationConfig = nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName());
        this.chunkSize = yarnApplicationConfig.getTupleChunkSize();
        this.bytesQueue = new ArrayBlockingQueue<byte[]>(this.chunkSize);
        this.logger = nodeEngine.getLogger(ShufflingReceiver.class);
        this.chunkReceiver = new ChunkedInputStream(this.bytesQueue , yarnApplicationConfig.getYarnSecondsToAwait());
        this.in = nodeEngine.getSerializationService().createObjectDataInputStream(this.chunkReceiver);
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
    public boolean consume(byte[] entry) throws Exception {
        return this.bytesQueue.offer(entry);
    }

    @Override
    public Tuple[] produce() {
        if (this.interrupted) {
            return null;
        }

        if ((this.bytesQueue.size() == 0) && (this.chunkReceiver.remainingBytes() <= 0)) {
            if (this.closed) {
                for (int i = 0; i < this.handlers.size(); i++) {
                    this.handlers.get(i).onComplete(this);
                }

                this.interrupted = true;
            }
            return null;
        }

        try {
            if (this.tupleChunkLength == -1) {
                this.tupleChunkIndex = 0;
                this.tupleChunkLength = this.in.readInt();
                this.tupleChunkBuffer = new Tuple[this.chunkSize];
            }

            try {
                while (this.tupleChunkIndex < this.tupleChunkLength) {
                    Tuple tuple = this.in.readObject();
                    this.tupleChunkBuffer[this.tupleChunkIndex] = tuple;
                    this.tupleChunkIndex++;
                }
            } finally {
                this.tupleChunkLength = -1;
            }

            this.lastProducedCount = this.tupleChunkIndex;
            receivedT.addAndGet(this.lastProducedCount);
            return this.tupleChunkBuffer;
        } catch (IOException e) {
            this.logger.warning(e.getMessage(), e);
            this.applicationMaster.invalidateApplication(e);
            return null;
        }
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
        return this.getVertex().getName();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        this.handlers.add(runnable);
    }

    public void registerFinalizingHandler(ProducerCompletionHandler runnable) {
        this.finalizingHandlers.add(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.handlers) {
            handler.onComplete(this);
        }
    }

    public void handleReceiverFinalizing() {
        for (ProducerCompletionHandler handler : this.finalizingHandlers) {
            handler.onComplete(this);
        }
    }

    public void markInterrupted() {
        this.chunkReceiver.markInterrupted();
        this.interrupted = true;
    }
}
