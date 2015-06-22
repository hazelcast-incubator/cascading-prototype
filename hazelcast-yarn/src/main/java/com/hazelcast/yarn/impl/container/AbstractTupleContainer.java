package com.hazelcast.yarn.impl.container;


import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tap.SinkTap;
import com.hazelcast.yarn.api.tap.SourceTap;

import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.yarn.api.actor.TupleActor;
import com.hazelcast.yarn.api.tuple.TupleWriter;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.TupleChannel;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.task.TaskEvent;
import com.hazelcast.yarn.api.processor.ProcessorDescriptor;
import com.hazelcast.yarn.api.application.ApplicationContext;

import com.hazelcast.yarn.impl.container.task.DefaultTupleTask;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.task.TaskProcessorFactory;
import com.hazelcast.yarn.api.statemachine.TupleContainerStateMachine;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;

import com.hazelcast.yarn.impl.statemachine.container.TupleContainerStateMachineImpl;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerState;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerResponse;
import com.hazelcast.yarn.impl.container.task.processors.factory.DefaultTaskProcessorFactory;
import com.hazelcast.yarn.impl.container.task.processors.factory.ShuffledTaskProcessorFactory;
import com.hazelcast.yarn.impl.statemachine.container.processors.TupleContainerPayLoadFactory;
import com.hazelcast.yarn.impl.statemachine.container.requests.NotifyExecutionCompletedRequest;
import com.hazelcast.yarn.impl.statemachine.container.requests.NotifyContainerFinalizedRequest;
import com.hazelcast.yarn.impl.statemachine.container.requests.NotifyContainerInterruptedRequest;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.NotifyContainerExecutionError;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerStateMachineFactory;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.NotifyInterruptionFailedRequest;

public abstract class AbstractTupleContainer extends AbstractContainer<TupleContainerEvent, TupleContainerState, TupleContainerResponse>
        implements TupleContainer {
    private final Vertex vertex;

    private final ContainerTask[] containerTasks;

    private final Map<Integer, ContainerTask> containerTasksCache = new ConcurrentHashMap<Integer, ContainerTask>();

    private final int tasksCount;

    private final List<TupleProducer> sourcesProducers;

    private final List<TupleChannel> inputChannels;

    private final List<TupleChannel> outputChannels;

    private final AtomicInteger completedTasks = new AtomicInteger(0);

    private final AtomicInteger interruptedTasks = new AtomicInteger(0);

    private final AtomicInteger readyForFinalizationTasksCounter = new AtomicInteger(0);

    private final ApplicationMaster applicationMaster;

    private final ProcessorDescriptor processorDescriptor;

    private final int awaitSecondsTimeOut;

    private final TupleFactory tupleFactory;

    private final TaskProcessorFactory taskProcessorFactory;

    private final TupleContainerProcessorFactory containerProcessorFactory;

    private final AtomicInteger taskIdGenerator = new AtomicInteger(0);

    private static final TupleContainerStateMachineFactory STATE_MACHINE_FACTORY = new TupleContainerStateMachineFactory() {
        @Override
        public TupleContainerStateMachine newStateMachine(String name,
                                                          StateMachineRequestProcessor<TupleContainerEvent> processor,
                                                          NodeEngine nodeEngine,
                                                          ApplicationContext applicationContext) {
            return new TupleContainerStateMachineImpl(
                    name,
                    processor,
                    nodeEngine,
                    applicationContext
            );
        }
    };

    public AbstractTupleContainer(Vertex vertex,
                                  TupleContainerProcessorFactory containerProcessorFactory,
                                  NodeEngine nodeEngine,
                                  ApplicationContext applicationContext,
                                  TupleFactory tupleFactory) {
        super(vertex, STATE_MACHINE_FACTORY, nodeEngine, applicationContext, tupleFactory);

        this.vertex = vertex;
        this.tupleFactory = tupleFactory;

        this.inputChannels = new ArrayList<TupleChannel>();
        this.outputChannels = new ArrayList<TupleChannel>();
        this.taskProcessorFactory = vertex.hasOutputShuffler() || vertex.getSinks().size() > 0
                ?
                new ShuffledTaskProcessorFactory() :
                new DefaultTaskProcessorFactory();
        this.processorDescriptor = vertex.getDescriptor();
        this.tasksCount = processorDescriptor.getTaskCount();
        this.sourcesProducers = new ArrayList<TupleProducer>();
        this.containerProcessorFactory = containerProcessorFactory;
        this.containerTasks = new ContainerTask[this.tasksCount];
        this.applicationMaster = getApplicationContext().getApplicationMaster();
        this.awaitSecondsTimeOut = nodeEngine.getConfig().getYarnApplicationConfig(vertex.getName()).getYarnSecondsToAwait();

        this.buildTasks();
        this.buildTaps();
    }

    private void buildTasks() {
        ContainerTask[] containerTasks = this.getContainerTasks();

        for (int i = 0; i < this.tasksCount; i++) {
            int taskID = this.taskIdGenerator.incrementAndGet();
            containerTasks[i] = new DefaultTupleTask(this, this.getVertex(), this.taskProcessorFactory, taskID);
            this.containerTasksCache.put(taskID, containerTasks[i]);
        }

        this.getApplicationContext().getProcessingExecutor().addDistributed(containerTasks);
    }

    @Override
    public void handleTaskEvent(ContainerTask containerTask, TaskEvent event) {
        this.handleTaskEvent(containerTask, event, null);
    }

    @Override
    public void handleTaskEvent(ContainerTask containerTask, TaskEvent event, Throwable error) {
        switch (event) {
            case TASK_EXECUTION_COMPLETED:
                if (this.completedTasks.incrementAndGet() >= this.containerTasks.length) {
                    this.completedTasks.set(0);
                    this.handleContainerRequest(new NotifyExecutionCompletedRequest());
                }
                break;
            case TASK_SUCCESSFULLY_INTERRUPTED:
                if (this.interruptedTasks.incrementAndGet() >= this.containerTasks.length) {
                    this.interruptedTasks.set(0);
                    this.handleContainerRequest(new NotifyContainerInterruptedRequest(this));
                }
                break;
            case TASK_READY_FOR_FINALIZATION:
                if (this.readyForFinalizationTasksCounter.decrementAndGet() <= 0) {
                    for (ContainerTask task : this.getContainerTasks()) {
                        task.startFinalization();
                    }
                }
                break;
            case TASK_INTERRUPTION_FAILED:
                this.applicationMaster.handleContainerRequest(new NotifyInterruptionFailedRequest(this));
                break;
            case TASK_EXECUTION_ERROR:
                this.applicationMaster.handleContainerRequest(new NotifyContainerExecutionError(this, error));
                this.applicationMaster.invalidateApplication(error);
                break;
        }
    }

    @Override
    public ProcessorDescriptor getProcessorDescriptor() {
        return this.processorDescriptor;
    }

    @Override
    public final TupleContainerProcessorFactory getContainerProcessorFactory() {
        return this.containerProcessorFactory;
    }

    @Override
    public ContainerTask[] getContainerTasks() {
        return this.containerTasks;
    }

    @Override
    public Map<Integer, ContainerTask> getTasksCache() {
        return this.containerTasksCache;
    }

    @Override
    public Vertex getVertex() {
        return this.vertex;
    }

    @Override
    public List<TupleChannel> getInputChannels() {
        return this.inputChannels;
    }

    @Override
    public List<TupleChannel> getOutputChannels() {
        return this.outputChannels;
    }

    @Override
    public void addInputChannel(TupleChannel channel) {
        this.inputChannels.add(channel);
    }

    @Override
    public void addOutputChannel(TupleChannel channel) {
        this.outputChannels.add(channel);
    }

    @Override
    public void execute() {
        this.readyForFinalizationTasksCounter.set(this.tasksCount);
        for (ContainerTask containerTask : this.getContainerTasks()) {
            containerTask.beforeExecution();
        }
    }

    @Override
    public void start() {
        int taskCount = this.getContainerTasks().length;

        if (taskCount == 0) {
            throw new IllegalStateException("No containerTasks for container");
        }

        List<TupleProducer> inputProducers = inputProducers();

        int actorsCount = inputProducers.size();

        if (actorsCount > 0) {
            int idx = 0;
            int actorsPerTask = actorsCount / taskCount;

            if (actorsPerTask == 0) {
                this.getContainerTasks()[0].start(inputProducers);
                for (int i = 1; i < this.getContainerTasks().length; i++) {
                    this.getContainerTasks()[i].start(null);
                }
            } else {
                for (ContainerTask containerTask : this.getContainerTasks()) {
                    int startIdx = idx * actorsPerTask;

                    if (startIdx > actorsCount - 1) {
                        break;
                    }

                    int toIndex;

                    if (idx == taskCount - 1) {
                        toIndex = actorsCount;
                    } else {
                        toIndex = startIdx + actorsPerTask;
                    }

                    containerTask.start(inputProducers.subList(startIdx, toIndex));

                    idx++;
                }
            }
        } else {
            for (int i = 0; i < this.getContainerTasks().length; i++) {
                this.getContainerTasks()[i].start(null);
            }
        }
    }

    @Override
    public void processRequest(TupleContainerEvent event, Object payLoad) throws Exception {
        ContainerPayLoadProcessor processor = TupleContainerPayLoadFactory.getProcessor(event, this);

        if (processor != null) {
            processor.process(payLoad);
        }
    }

    @Override
    public void destroy() throws Exception {
        for (ContainerTask containerTask : this.getContainerTasks()) {
            containerTask.destroy();
        }

        this.handleContainerRequest(new NotifyContainerFinalizedRequest(this)).get(this.awaitSecondsTimeOut, TimeUnit.SECONDS);
    }

    @Override
    public void invalidate() {
        for (ContainerTask task : this.containerTasks) {
            task.markInvalidated();
        }
    }

    private void buildTaps() {
        if (this.getVertex().getSources().size() > 0) {
            for (SourceTap sourceTap : this.getVertex().getSources()) {
                this.sourcesProducers.addAll(Arrays.asList(sourceTap.getReaders(this.getApplicationContext(), this.getVertex(), this.tupleFactory)));
            }
        }

        List<SinkTap> sinks = this.getVertex().getSinks();

        if (sinks.size() > 0) {
            for (ContainerTask containerTask : this.getContainerTasks()) {
                List<TupleWriter> sinkWriters = new ArrayList<TupleWriter>(sinks.size());

                for (SinkTap sinkTap : sinks) {
                    sinkWriters.addAll(Arrays.asList(sinkTap.getWriters(getNodeEngine(), getContainerContext())));
                }

                containerTask.registerSinkWriters(sinkWriters);
            }
        }
    }

    private List<TupleProducer> inputProducers() {
        List<TupleProducer> producers = new ArrayList<TupleProducer>(this.getInputChannels().size() + this.sourcesProducers.size());

        for (TupleChannel channel : getInputChannels()) {
            for (TupleActor actor : channel.getActors()) {
                producers.add(actor);
            }
        }

        producers.addAll(this.sourcesProducers);

        return producers;
    }
}