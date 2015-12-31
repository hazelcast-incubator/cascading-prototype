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

package com.hazelcast.jet.impl.container.task;


import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;


import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;

import com.hazelcast.jet.api.dag.Edge;
import com.hazelcast.jet.api.dag.Vertex;


import com.hazelcast.jet.impl.SettableFuture;
import com.hazelcast.jet.api.data.DataWriter;
import com.hazelcast.jet.api.executor.Payload;

import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.jet.api.actor.ObjectActor;
import com.hazelcast.jet.api.actor.ComposedActor;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.api.executor.TaskContext;
import com.hazelcast.jet.api.container.DataChannel;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.container.task.TaskEvent;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.impl.actor.DefaultComposedActor;
import com.hazelcast.jet.api.processor.ContainerProcessor;
import com.hazelcast.jet.api.container.task.TaskProcessor;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.application.ApplicationListener;
import com.hazelcast.jet.impl.actor.shuffling.ShufflingActor;
import com.hazelcast.jet.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.impl.container.DefaultProcessorContext;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.api.container.task.TaskProcessorFactory;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;

//CHECKSTYLE:OFF
public class DefaultContainerTask implements ContainerTask {
    private final ILogger logger;

    private final int taskID;

    private final Vertex vertex;

    private final int timeAwaiting;

    private final NodeEngine nodeEngine;

    private final DataContainer container;

    private volatile boolean interrupted = true;

    private volatile TaskProcessor taskProcessor;

    private volatile boolean sendersClosed;

    private volatile ShufflingSender[] sendersArray;

    private final ContainerContext containerContext;

    private final ContainerProcessor processor;

    private final ApplicationContext applicationContext;

    private final TaskProcessorFactory taskProcessorFactory;

    private volatile SettableFuture<Boolean> interruptionFuture;

    private volatile boolean sendersFlushed;

    private final AtomicInteger activeProducersCounter = new AtomicInteger(0);

    private final AtomicInteger closedReceiversCounter = new AtomicInteger(0);

    private final AtomicInteger finalizedReceiversCounter = new AtomicInteger(0);

    private final Collection<ObjectConsumer> consumers = new CopyOnWriteArrayList<ObjectConsumer>();

    private final Collection<ObjectProducer> producers = new CopyOnWriteArrayList<ObjectProducer>();

    private final Map<Address, ShufflingReceiver> shufflingReceivers = new ConcurrentHashMap<Address, ShufflingReceiver>();

    private final Map<Address, ShufflingSender> shufflingSenders = new ConcurrentHashMap<Address, ShufflingSender>();

    private boolean containerFinalizationNotified;

    private volatile boolean finalizationStarted;

    private final ProcessorContext processorContext;

    public DefaultContainerTask(DataContainer container,
                                Vertex vertex,
                                TaskProcessorFactory taskProcessorFactory,
                                int taskID,
                                TaskContext taskContext) {
        this.taskID = taskID;
        this.vertex = vertex;
        this.container = container;
        this.taskProcessorFactory = taskProcessorFactory;
        this.containerContext = container.getContainerContext();
        this.applicationContext = container.getApplicationContext();
        this.nodeEngine = container.getApplicationContext().getNodeEngine();
        this.logger = this.nodeEngine.getLogger(DefaultContainerTask.class);
        ContainerProcessorFactory processorFactory = container.getContainerProcessorFactory();
        this.processor = processorFactory == null ? null : processorFactory.getProcessor(vertex);
        this.processorContext = new DefaultProcessorContext(taskContext, this.containerContext);

        if (this.processor != null) {
            this.applicationContext.registerApplicationListener(new ApplicationListener() {
                @Override
                public void onApplicationExecuted(ApplicationContext applicationContext) {
                    processor.afterProcessing(processorContext);
                }
            });
        }

        JetApplicationConfig config = applicationContext.getJetApplicationConfig();
        this.timeAwaiting = config.getJetSecondsToAwait();
    }

    @Override
    public void start(List<? extends ObjectProducer> producers) {
        if ((producers != null) && (producers.size() > 0)) {
            for (ObjectProducer producer : producers) {
                this.producers.add(producer);

                producer.registerCompletionHandler(new ProducerCompletionHandler() {
                    @Override
                    public void onComplete(ObjectProducer producer) {
                        handleProducerCompleted(producer);
                    }
                });
            }
        }

        onStart();
    }

    private void onStart() {
        ObjectProducer[] producers = this.producers.toArray(new ObjectProducer[this.producers.size()]);
        ObjectConsumer[] consumers = this.consumers.toArray(new ObjectConsumer[this.consumers.size()]);

        this.taskProcessor = this.taskProcessorFactory.getTaskProcessor(
                producers,
                consumers,
                this.containerContext,
                this.processorContext,
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
                interruptTask();
                this.container.handleTaskEvent(this, TaskEvent.TASK_SUCCESSFULLY_INTERRUPTED);
            } finally {
                closeActors();
            }
        }
    }

    @Override
    public void destroy() {
        interruptTask();
    }

    @Override
    public void beforeExecution() {
        this.sendersClosed = false;
        this.sendersFlushed = false;
        this.activeProducersCounter.set(this.producers.size());
        this.closedReceiversCounter.set(this.shufflingReceivers.values().size());
        this.finalizedReceiversCounter.set(this.shufflingReceivers.values().size());
        this.interrupted = false;

        this.taskProcessor.onOpen();
        this.processor.beforeProcessing(this.processorContext);
    }

    @Override
    public void registerSinkWriters(List<DataWriter> sinkWriters) {
        this.consumers.addAll(sinkWriters);
    }

    @Override
    public ComposedActor registerOutputChannel(DataChannel channel, Edge edge, DataContainer targetContainer) {
        List<ObjectActor> actors = new ArrayList<ObjectActor>(targetContainer.getContainerTasks().length);

        for (int i = 0; i < targetContainer.getContainerTasks().length; i++) {
            ObjectActor actor = new RingBufferActor(this.nodeEngine, this.applicationContext, this, this.vertex, edge);

            if (channel.isShuffled()) {
                //output
                actor = new ShufflingActor(actor, this.nodeEngine, this.containerContext);
            }

            actors.add(actor);
        }

        ComposedActor composed = new DefaultComposedActor(this, actors, this.vertex, edge, this.containerContext);
        this.consumers.add(composed);

        return composed;
    }

    @Override
    public void handleProducerCompleted(ObjectProducer actor) {
        this.activeProducersCounter.decrementAndGet();
    }

    @Override
    public void handleShufflingReceiverCompleted(ObjectProducer actor) {
        this.closedReceiversCounter.decrementAndGet();
    }

    @Override
    public void registerShufflingReceiver(Address address, ShufflingReceiver receiver) {
        this.shufflingReceivers.put(address, receiver);

        receiver.registerCompletionHandler(new ProducerCompletionHandler() {
            @Override
            public void onComplete(ObjectProducer producer) {
                handleShufflingReceiverCompleted(producer);
            }
        });
    }

    @Override
    public ShufflingReceiver getShufflingReceiver(Address endPoint) {
        return this.shufflingReceivers.get(endPoint);
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
    public void registerShufflingSender(Address address, ShufflingSender sender) {
        this.shufflingSenders.put(address, sender);
    }

    @Override
    public boolean executeTask(Payload payload) {
        TaskProcessor processor = this.taskProcessor;

        if (this.interrupted) {
            processor.reset();

            if (this.interruptionFuture != null) {
                try {
                    this.processor.afterProcessing(this.processorContext);
                } finally {
                    this.interruptionFuture.set(true);
                }
            }

            return false;
        }

        boolean result;

        try {
            result = process(payload, processor);
        } catch (Throwable error) {
            result = catchProcessingError(payload, error);
        }

        handleResult(result);
        return result;
    }

    private void handleResult(boolean result) {
        if (!result) {
            try {
                this.processor.afterProcessing(this.processorContext);
            } catch (Throwable e) {
                try {
                    this.logger.warning(e.getMessage(), e);
                } catch (Throwable ee) {
                    //Noting to do
                }
            }
        }
    }

    @Override
    public void finalizeTask() {
        destroy();
    }

    private boolean isFinalizationAwaited() {
        return ((this.containerFinalizationNotified) && (!this.finalizationStarted));
    }

    private boolean process(Payload payload, TaskProcessor processor) throws Exception {
        if (!this.sendersFlushed) {
            if (!checkIfSendersFlushed()) {
                return true;
            }
        }

        if (isFinalizationAwaited()) {
            payload.set(false);
            return true;
        }

        boolean success = processor.process();
        boolean activity = processor.consumed() || processor.produced();
        payload.set(activity);

        if (((!activity) && (success))) {
            checkActiveProducers(processor);

            if (processor.isFinalized()) {
                closeSenders();

                if (!checkIfSendersFlushed()) {
                    return true;
                }

                if (this.closedReceiversCounter.get() <= 0) {
                    completeTaskExecution();
                    return false;
                }
            } else {
                if (this.activeProducersCounter.get() <= 0) {
                    notifyFinalizationStarted();
                    if (this.finalizationStarted) {
                        processor.startFinalization();
                    }
                }
            }
        }

        return true;
    }

    private void checkActiveProducers(TaskProcessor processor) {
        if ((!processor.hasActiveProducers())) {
            this.activeProducersCounter.set(0);
            this.finalizedReceiversCounter.set(0);
            this.closedReceiversCounter.set(0);
        }
    }

    private void notifyFinalizationStarted() {
        if (!this.containerFinalizationNotified) {
            this.container.handleTaskEvent(this, TaskEvent.TASK_READY_FOR_FINALIZATION);
            this.containerFinalizationNotified = true;
        }
    }

    private boolean checkIfSendersFlushed() {
        boolean success = true;

        for (ShufflingSender sender : this.sendersArray) {
            success &= sender.isFlushed();
        }

        this.sendersFlushed = success;
        return success;
    }

    private boolean catchProcessingError(Payload payload, Throwable error) {
        this.interrupted = true;

        if (this.interruptionFuture != null) {
            this.interruptionFuture.set(true);
        }

        this.logger.warning("Exception in the tupleTask message=" + error.getMessage(), error);

        try {
            payload.set(false);
            completeExecutionWithError(error);
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
            closeActors();
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
//CHECKSTYLE:ON
