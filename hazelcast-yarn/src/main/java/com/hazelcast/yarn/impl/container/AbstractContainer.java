package com.hazelcast.yarn.impl.container;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.container.Container;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.ContainerResponse;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.ContainerStateMachine;
import com.hazelcast.yarn.api.statemachine.container.ContainerState;
import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.impl.processor.context.DefaultContainerContext;
import com.hazelcast.yarn.api.statemachine.container.ContainerStateMachineFactory;

public abstract class AbstractContainer<SI extends ContainerEvent, SS extends ContainerState, SO extends ContainerResponse> implements Container<SI, SS, SO> {
    private final int id;

    private final Vertex vertex;
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final List<TupleContainer> followers;
    private final List<TupleContainer> predecessors;
    private final ContainerContext containerContext;
    private final ApplicationContext applicationContext;
    private final ContainerStateMachine<SI, SS, SO> stateMachine;

    public AbstractContainer(ContainerStateMachineFactory<SI, SS, SO> stateMachineFactory,
                             NodeEngine nodeEngine,
                             ApplicationContext applicationContext,
                             TupleFactory tupleFactory) {
        this(null, stateMachineFactory, nodeEngine, applicationContext, tupleFactory);
    }

    public AbstractContainer(Vertex vertex,
                             ContainerStateMachineFactory<SI, SS, SO> stateMachineFactory,
                             NodeEngine nodeEngine,
                             ApplicationContext applicationContext,
                             TupleFactory tupleFactory) {
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        String name = vertex == null ? applicationContext.getName() : vertex.getName();
        this.stateMachine = stateMachineFactory.newStateMachine(name, this, nodeEngine, applicationContext);
        this.applicationContext = applicationContext;
        this.followers = new ArrayList<TupleContainer>();
        this.predecessors = new ArrayList<TupleContainer>();
        this.id = applicationContext.getContainerIDGenerator().incrementAndGet();
        this.containerContext = new DefaultContainerContext(nodeEngine, applicationContext, this.id, vertex, tupleFactory);
        this.logger = nodeEngine.getLogger(AbstractContainer.class);
    }

    @Override
    public List<TupleContainer> getFollowers() {
        return followers;
    }

    @Override
    public List<TupleContainer> getPredecessors() {
        return predecessors;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public ContainerStateMachine<SI, SS, SO> getStateMachine() {
        return this.stateMachine;
    }

    @Override
    public void addFollower(TupleContainer container) {
        this.followers.add(container);
    }

    @Override
    public void addPredecessor(TupleContainer container) {
        this.predecessors.add(container);
    }

    public <P> Future<SO> handleContainerRequest(ContainerRequest<SI, P> request) {
        try {
            return this.stateMachine.handleRequest(request);
        } finally {
            this.wakeUpExecutor();
        }
    }

    protected abstract void wakeUpExecutor();

    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public ContainerContext getContainerContext() {
        return containerContext;
    }

    public String getApplicationName() {
        return applicationContext.getName();
    }

    @Override
    public int getID() {
        return id;
    }
}
