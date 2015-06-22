package com.hazelcast.yarn.impl.container.task;

import java.util.Map;
import java.util.List;
import java.util.Collection;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;

import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.yarn.impl.SettableFuture;
import com.hazelcast.yarn.api.executor.Payload;
import com.hazelcast.yarn.api.actor.TupleActor;
import com.hazelcast.yarn.api.tuple.TupleWriter;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.TupleChannel;
import com.hazelcast.yarn.impl.actor.RingBufferActor;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.task.TaskEvent;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.impl.actor.shuffling.ShufflingActor;
import com.hazelcast.yarn.api.actor.ProducerCompletionHandler;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.container.task.TaskProcessorFactory;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;


public class DefaultTupleTask implements ContainerTask {
    private final ILogger logger;

    private final int taskID;

    private final Vertex vertex;

    private final int timeAwaiting;

    private final NodeEngine nodeEngine;

    private final TupleContainer container;

    private volatile boolean interrupted = true;

    private volatile boolean invalidated = false;

    private volatile TaskProcessor taskProcessor;

    private volatile boolean sendersClosed = false;

    private volatile ShufflingSender[] sendersArray;

    private final ContainerContext containerContext;

    private final TupleContainerProcessor processor;

    private final ApplicationContext applicationContext;

    private final TaskProcessorFactory taskProcessorFactory;

    private volatile SettableFuture<Boolean> interruptionFuture;

    private volatile boolean sendersFinalizationNotified = false;

    private final AtomicInteger activeProducersCounter = new AtomicInteger(0);

    private final AtomicInteger closedReceiversCounter = new AtomicInteger(0);

    private final AtomicInteger finalizedReceiversCounter = new AtomicInteger(0);

    private final Collection<TupleConsumer> consumers = new CopyOnWriteArrayList<TupleConsumer>();

    private final Collection<TupleProducer> producers = new CopyOnWriteArrayList<TupleProducer>();

    private final Map<Address, ShufflingReceiver> shufflingReceivers = new ConcurrentHashMap<Address, ShufflingReceiver>();

    private final Map<Address, ShufflingSender> shufflingSenders = new ConcurrentHashMap<Address, ShufflingSender>();

    private boolean containerFinalizationNotified;

    private volatile boolean finalizationStarted;

    public DefaultTupleTask(TupleContainer container,
                            Vertex vertex,
                            TaskProcessorFactory taskProcessorFactory,
                            int taskID) {
        this.taskID = taskID;
        this.vertex = vertex;
        this.container = container;
        this.taskProcessorFactory = taskProcessorFactory;
        this.nodeEngine = container.getApplicationContext().getNodeEngine();
        this.containerContext = container.getContainerContext();
        this.applicationContext = container.getApplicationContext();
        this.logger = this.nodeEngine.getLogger(DefaultTupleTask.class);
        TupleContainerProcessorFactory processorFactory = container.getContainerProcessorFactory();
        this.processor = processorFactory == null ? null : processorFactory.getProcessor(vertex);
        this.timeAwaiting = nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName()).getYarnSecondsToAwait();
    }

    @Override
    public void start(List<? extends TupleProducer> producers) {
        if (producers != null) {
            for (TupleProducer producer : producers) {
                this.producers.add(producer);

                producer.registerCompletionHandler(new ProducerCompletionHandler() {
                    @Override
                    public void onComplete(TupleProducer producer) {
                        handleProducerCompleted(producer);
                    }
                });
            }
        }

        this.onStart();
    }

    private void onStart() {
        TupleProducer[] producers = this.producers.toArray(new TupleProducer[this.producers.size()]);
        TupleConsumer[] consumers = this.consumers.toArray(new TupleConsumer[this.consumers.size()]);

        this.taskProcessor = this.taskProcessorFactory.getTaskProcessor(
                producers,
                consumers,
                this.containerContext,
                this.processor,
                this.vertex,
                this.taskID
        );

        this.containerFinalizationNotified = false;
        this.finalizationStarted = false;
        this.sendersArray = this.shufflingSenders.values().toArray(new ShufflingSender[this.shufflingSenders.values().size()]);
    }

    @Override
    public void interrupt() {
        if (!this.interrupted) {
            try {
                this.interruptTask();
                this.container.handleTaskEvent(this, TaskEvent.TASK_SUCCESSFULLY_INTERRUPTED);
            } finally {
                this.closeActors();
            }
        }
    }

    @Override
    public void markInvalidated() {
        this.invalidated = true;
    }

    @Override
    public void destroy() {
        this.interruptTask();
    }

    @Override
    public void beforeExecution() {
        this.sendersClosed = false;
        this.sendersFinalizationNotified = false;
        this.activeProducersCounter.set(this.producers.size());
        this.closedReceiversCounter.set(this.shufflingReceivers.values().size());
        this.finalizedReceiversCounter.set(this.shufflingReceivers.values().size());
        this.interrupted = false;

        this.taskProcessor.onOpen();
        this.processor.beforeProcessing(this.containerContext);
    }

    @Override
    public void registerSinkWriters(List<TupleWriter> sinkWriters) {
        this.consumers.addAll(sinkWriters);
    }

    @Override
    public TupleActor registerOutputChannel(TupleChannel channel, Edge edge) {
        TupleActor actor = new RingBufferActor(this.nodeEngine, this.applicationContext, this, this.vertex, edge);

        if (channel.isShuffled()) {
            //output
            actor = new ShufflingActor(actor, this.nodeEngine, this.containerContext);
        }

        this.consumers.add(actor);
        return actor;
    }

    @Override
    public void handleProducerCompleted(TupleProducer actor) {
        this.activeProducersCounter.decrementAndGet();
    }

    @Override
    public void handleShufflingReceiverCompleted(TupleProducer actor) {
        this.closedReceiversCounter.decrementAndGet();
    }

    @Override
    public void handleShufflingReceiverFinalizing(TupleProducer actor) {
        this.finalizedReceiversCounter.decrementAndGet();
    }

    @Override
    public void registerShufflingReceiver(Member member, ShufflingReceiver receiver) {
        this.shufflingReceivers.put(member.getAddress(), receiver);

        receiver.registerCompletionHandler(new ProducerCompletionHandler() {
            @Override
            public void onComplete(TupleProducer producer) {
                handleShufflingReceiverCompleted(producer);
            }
        });

        receiver.registerFinalizingHandler(new ProducerCompletionHandler() {
            @Override
            public void onComplete(TupleProducer producer) {
                handleShufflingReceiverFinalizing(producer);
            }
        });
    }

    @Override
    public ShufflingReceiver getShufflingReceiver(Address endPoint) {
        return this.shufflingReceivers.get(endPoint);
    }

    @Override
    public ShufflingSender getShufflingSender(Address endPoint) {
        return this.shufflingSenders.get(endPoint);
    }

    @Override
    public Vertex getVertex() {
        return this.containerContext.getVertex();
    }

    @Override
    public void startFinalization() {
        this.finalizationStarted = true;
    }

    @Override
    public void registerShufflingSender(Member member, ShufflingSender sender) {
        this.shufflingSenders.put(member.getAddress(), sender);
    }

    @Override
    public boolean executeTask(Payload payload) {
        TaskProcessor processor = this.taskProcessor;

        if (this.invalidated) {
            this.closeActors();
            this.interrupted = true;
            return false;
        }

        if (this.interrupted) {
            processor.reset();

            if (this.interruptionFuture != null) {
                this.interruptionFuture.set(true);
            }

            return false;
        }

        try {
            return this.process(payload, processor);
        } catch (Throwable error) {
            return this.catchProcessingError(payload, error);
        }
    }

    private boolean isFinalizationAwaited() {
        return ((this.containerFinalizationNotified) && (!this.finalizationStarted));
    }

    private boolean process(Payload payload, TaskProcessor processor) throws Exception {
        if (this.isFinalizationAwaited()) {
            payload.set(false);
            return true;
        }

        boolean success = processor.process();
        boolean activity = processor.consumed() || processor.produced();
        payload.set(activity);

        if (((!activity) && (success))) {
            if (processor.isFinalized()) {
                this.closeSenders();

                if (this.closedReceiversCounter.get() <= 0) {
                    this.completeTaskExecution();
                    return false;
                }
            } else {
                if (this.activeProducersCounter.get() <= 0) {
                    this.finalizeSenders();

                    if ((!processor.hasActiveConsumers()) && (!processor.hasActiveProducers())) {
                        this.finalizedReceiversCounter.set(0);
                    }

                    if (this.finalizedReceiversCounter.get() <= 0) {
                        if (!this.containerFinalizationNotified) {
                            this.container.handleTaskEvent(this, TaskEvent.TASK_READY_FOR_FINALIZATION);
                            this.containerFinalizationNotified = true;
                        }

                        if (this.finalizationStarted) {
                            processor.startFinalization();
                        }
                    }
                }
            }
        }

        return true;
    }


    private void finalizeSenders() {
        if (!this.sendersFinalizationNotified) {
            for (ShufflingSender sender : this.sendersArray) {
                sender.notifyProducersFinalizing();
            }

            this.sendersFinalizationNotified = true;
        }
    }

    private boolean catchProcessingError(Payload payload, Throwable error) {
        this.interrupted = true;

        if (this.interruptionFuture != null) {
            this.interruptionFuture.set(true);
        }

        this.logger.warning("Exception in the tupleTask message=" + error.getMessage(), error);

        try {
            payload.set(false);
            this.completeExecutionWithError(error);
            return false;
        } catch (Throwable e) {
            try {
                this.logger.warning("Exception in the tupleTask message=" + e.getMessage(), e);
            } finally {
                payload.set(false);
            }

            return false;
        }
    }

    private void closeSenders() {
        if (!this.sendersClosed) {
            for (ShufflingSender sender : this.sendersArray) {
                sender.close();
            }

            this.sendersClosed = true;
        }
    }

    private void completeTaskExecution() {
        this.interrupted = true;

        try {
            this.closeActors();
        } finally {
            this.container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_COMPLETED);
        }
    }

    private void interruptTask() {
        for (ShufflingSender shufflingSender : this.shufflingSenders.values()) {
            shufflingSender.markInterrupted();
        }

        for (ShufflingReceiver shufflingReceiver : this.shufflingReceivers.values()) {
            shufflingReceiver.markInterrupted();
        }

        if (!this.interrupted) {
            this.interruptionFuture = SettableFuture.create();
            this.interrupted = true;

            try {
                this.interruptionFuture.get(this.timeAwaiting, TimeUnit.SECONDS);
            } catch (Throwable e) {
                this.container.handleTaskEvent(this, TaskEvent.TASK_INTERRUPTION_FAILED);
                throw new RuntimeException(e);
            }
        }
    }

    private void closeActors() {
        this.taskProcessor.onClose();
    }

    private void completeExecutionWithError(Throwable error) {
        this.container.handleTaskEvent(this, TaskEvent.TASK_EXECUTION_ERROR, error);
        this.interrupted = true;
    }
}
