package com.hazelcast.yarn.impl.tap.source;

import java.util.List;
import java.util.Iterator;
import java.util.concurrent.Future;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tap.TapEvent;
import com.hazelcast.yarn.api.tap.TapState;
import com.hazelcast.yarn.impl.SettableFuture;
import com.hazelcast.yarn.api.tap.TapResponse;
import com.hazelcast.yarn.api.tuple.TupleReader;

import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.yarn.api.statemachine.StateMachine;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.actor.ProducerCompletionHandler;
import com.hazelcast.yarn.api.statemachine.StateMachineFactory;
import com.hazelcast.yarn.api.statemachine.StateMachineRequest;

import com.hazelcast.yarn.impl.tap.HazelcastTapStateMachineFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;


public abstract class AbstractHazelcastReader<K, V> implements TupleReader {
    private final String name;

    private volatile int lastProducedCount;

    private final int chunkSize;

    private final int partitionId;

    private final NodeEngine nodeEngine;

    protected Iterator<Tuple<K, V>> iterator;

    private final StateMachine<TapEvent, TapState, TapResponse> stateMachine;

    private final List<ProducerCompletionHandler> completionHandlers;

    private final InternalOperationService internalOperationService;

    private final Vertex vertex;

    private final ILogger logger;

    private final int awaitSecondsTime;

    private final TupleFactory tupleFactory;

    private volatile Tuple[] chunkBuffer;

    private final ApplicationContext applicationContext;

    private volatile boolean isReadRequested = false;

    private static final StateMachineFactory<TapEvent, TapState, TapResponse> stateMachineFactory = new HazelcastTapStateMachineFactory();

    protected final SettableFuture<Boolean> future = SettableFuture.create();

    private final PartitionSpecificRunnable partitionSpecificRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                read();
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

    public AbstractHazelcastReader(ApplicationContext applicationContext,
                                   String name,
                                   int partitionId,
                                   TupleFactory tupleFactory,
                                   Vertex vertex
    ) {
        this.name = name;
        this.vertex = vertex;
        this.partitionId = partitionId;
        this.tupleFactory = tupleFactory;
        this.applicationContext = applicationContext;
        this.nodeEngine = applicationContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(this.getClass());
        this.completionHandlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        this.stateMachine = stateMachineFactory.newStateMachine(name, null, nodeEngine, applicationContext);
        this.awaitSecondsTime = nodeEngine.getConfig().getYarnApplicationConfig(name).getYarnSecondsToAwait();
        this.chunkSize = nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName()).getTupleChunkSize();
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    private void sendStateMachineEvent(final TapEvent event) {
        Future<TapResponse> future = this.stateMachine.handleRequest(
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
            future.get(this.awaitSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            this.logger.warning(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        this.onClose();
        this.sendStateMachineEvent(TapEvent.CLOSE);
    }

    protected abstract void onClose();

    protected abstract void onOpen();

    @Override
    public void open() {
        this.future.reset();

        this.internalOperationService.execute(this.partitionSpecificOpenRunnable);

        try {
            this.future.get(this.awaitSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.sendStateMachineEvent(TapEvent.OPEN);
    }

    @Override
    public int lastProducedCount() {
        return this.lastProducedCount;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler handler) {
        this.completionHandlers.add(handler);
    }

    @Override
    public boolean isClosed() {
        return this.stateMachine.currentState() == TapState.CLOSED;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    private void pushReadRequest() {
        this.isReadRequested = true;
        this.future.reset();
        this.internalOperationService.execute(this.partitionSpecificRunnable);
    }

    private void read() {
        if (!this.hasNext()) {
            if (stateMachine.currentState() == TapState.OPENED) {
                try {
                    this.close();
                } finally {
                    this.handleProducerCompleted();
                }
            }

            this.chunkBuffer = null;
            this.lastProducedCount = -1;
            return;
        }

        Tuple[] buffer = new Tuple[this.chunkSize];
        int idx = 0;

        while ((this.hasNext()) && (idx < this.chunkSize)) {
            buffer[idx++] = this.iterator.next();
        }

        if ((idx > 0) && (idx < this.chunkSize)) {
            Tuple[] trunkedChunk = new Tuple[idx];
            System.arraycopy(buffer, 0, trunkedChunk, 0, idx);
            this.chunkBuffer = trunkedChunk;
            this.lastProducedCount = idx;
        } else {
            this.chunkBuffer = buffer;
            this.lastProducedCount = buffer.length;
        }
    }


    @Override
    public Tuple[] produce() {
        if (this.isReadRequested) {
            if (this.future.isDone()) {
                try {
                    this.future.get();
                    Tuple[] chunk = this.chunkBuffer;
                    return chunk;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                } finally {
                    this.future.reset();
                    this.chunkBuffer = null;
                    this.isReadRequested = false;
                }
            } else {
                return null;
            }
        } else {
            if (this.isClosed()) {
                return null;
            }

            this.pushReadRequest();
            return null;
        }
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.completionHandlers) {
            handler.onComplete(this);
        }
    }

    public String getName() {
        return this.name;
    }

    public TupleFactory getTupleFactory() {
        return tupleFactory;
    }

    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    public Iterator<Tuple<K, V>> getIterator() {
        return this.iterator;
    }
}
