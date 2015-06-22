package com.hazelcast.yarn.impl.tap.sink;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.Dummy;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tap.TapEvent;
import com.hazelcast.yarn.api.tap.TapState;
import com.hazelcast.yarn.api.tap.TapResponse;
import com.hazelcast.yarn.impl.SettableFuture;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.statemachine.StateMachine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.impl.tuple.io.DefaultTupleIOStream;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.StateMachineFactory;
import com.hazelcast.yarn.api.statemachine.StateMachineRequest;
import com.hazelcast.yarn.impl.tap.HazelcastTapStateMachineFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;


import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class AbstractHazelcastWriter implements TupleWriter {
    private final String name;

    private final int partitionId;

    protected volatile TupleInputStream chunkInputStream;

    private final NodeEngine nodeEngine;

    protected final SettableFuture<Boolean> future = SettableFuture.create();

    protected final InternalOperationService internalOperationService;

    private final StateMachine<TapEvent, TapState, TapResponse> stateMachine;

    protected volatile boolean isFlushed = true;

    private final int awaitInSecondsTime;

    private final ContainerContext containerContext;

    private final DefaultTupleIOStream chunkBuffer;

    private final SinkTapWriteStrategy sinkTapWriteStrategy;

    private final ApplicationContext applicationContext;

    private final ShufflingStrategy shufflingStrategy;

    private int lastConsumedCount = 0;

    private static final StateMachineFactory<TapEvent, TapState, TapResponse> stateMachineFactory = new HazelcastTapStateMachineFactory();

    private final PartitionSpecificRunnable partitionSpecificRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                processChunk(chunkInputStream);
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };

    private final PartitionSpecificRunnable partitionSpecificOpenRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                onOpen();
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };

    public AbstractHazelcastWriter(ContainerContext containerContext, int partitionId, SinkTapWriteStrategy sinkTapWriteStrategy) {
        checkNotNull(containerContext);

        this.partitionId = partitionId;
        this.applicationContext = containerContext.getApplicationContext();
        this.name = applicationContext.getName();
        this.sinkTapWriteStrategy = sinkTapWriteStrategy;
        this.containerContext = containerContext;
        this.nodeEngine = applicationContext.getNodeEngine();
        this.stateMachine = stateMachineFactory.newStateMachine(name, null, nodeEngine, applicationContext);
        this.awaitInSecondsTime = nodeEngine.getConfig().getYarnApplicationConfig(name).getYarnSecondsToAwait();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        int tupleChunkSize = nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName()).getTupleChunkSize();
        this.chunkBuffer = new DefaultTupleIOStream(new Tuple[tupleChunkSize]);
        this.shufflingStrategy = null;
    }

    @Override
    public boolean consume(TupleInputStream chunk) throws Exception {
        return this.consumeChunk(chunk) > 0;
    }

    private void pushWriteRequest() {
        this.future.reset();
        this.internalOperationService.execute(this.partitionSpecificRunnable);
        this.isFlushed = false;
    }

    @Override
    public int consumeChunk(TupleInputStream chunk) throws Exception {
        this.chunkInputStream = chunk;
        this.pushWriteRequest();
        this.lastConsumedCount = chunk.size();
        return chunk.size();
    }

    @Override
    public int consumeTuple(Tuple tuple) throws Exception {
        this.chunkBuffer.consume(tuple);
        this.lastConsumedCount = 1;
        return 1;
    }

    @Override
    public int flush() {
        try {
            if (this.chunkBuffer.size() > 0) {
                return this.consumeChunk(this.chunkBuffer);
            } else {
                return 0;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void processChunk(TupleInputStream entry);

    public String getName() {
        return this.name;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    private void sendStateMachineEvent(final TapEvent event) {
        this.stateMachine.handleRequest(
                new StateMachineRequest<TapEvent, Dummy>() {
                    @Override
                    public TapEvent getContainerEvent() {
                        return event;
                    }

                    @Override
                    public Dummy getPayLoad() {
                        return Dummy.INSTANCE;
                    }
                }
        );

        this.applicationContext.getTapStateMachineExecutor().wakeUp();

        try {
            this.future.get(this.awaitInSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (!isClosed()) {
            try {
                flush();
            } finally {
                sendStateMachineEvent(TapEvent.CLOSE);
                onClose();
            }
        }
    }

    protected void onOpen() {

    }

    protected void onClose() {

    }

    protected void resetBuffer() {
        chunkBuffer.reset();
    }

    public void open() {
        this.future.reset();
        this.isFlushed = true;

        this.internalOperationService.execute(this.partitionSpecificOpenRunnable);

        try {
            this.future.get(this.awaitInSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.sendStateMachineEvent(TapEvent.OPEN);
    }

    public boolean isFlushed() {
        if (this.isFlushed) {
            return true;
        } else {
            try {
                if (this.future.isDone()) {
                    try {
                        future.get();
                        return true;
                    } finally {
                        this.chunkBuffer.reset();
                        this.isFlushed = true;
                        this.chunkInputStream=null;
                    }
                }

                return false;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public boolean isClosed() {
        return this.stateMachine.currentState() == TapState.CLOSED;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    public ContainerContext getContainerContext() {
        return this.containerContext;
    }

    @Override
    public SinkTapWriteStrategy getSinkTapWriteStrategy() {
        return this.sinkTapWriteStrategy;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.shufflingStrategy;
    }
}
