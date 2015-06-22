package com.hazelcast.yarn.impl.container;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.actor.TupleActor;
import com.hazelcast.yarn.api.container.TupleChannel;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.container.TupleContainer;

public class DefaultTupleChannel implements TupleChannel {
    private final List<TupleActor> actors;
    private final TupleContainer sourceContainer;
    private final TupleContainer targetContainer;
    private final boolean isShuffled;
    private final Edge edge;

    public DefaultTupleChannel(TupleContainer sourceContainer,
                               TupleContainer targetContainer,
                               Edge edge) {
        this.edge = edge;

        this.sourceContainer = sourceContainer;
        this.targetContainer = targetContainer;

        this.isShuffled = edge.isShuffled();
        this.actors = new ArrayList<TupleActor>();

        this.init();
    }

    private void init() {
        for (ContainerTask containerTask : sourceContainer.getContainerTasks()) {
            actors.add(containerTask.registerOutputChannel(this,edge));
        }
    }

    @Override
    public List<TupleActor> getActors() {
        return actors;
    }

    @Override
    public TupleContainer getSourceContainer() {
        return sourceContainer;
    }

    @Override
    public TupleContainer getTargetContainer() {
        return targetContainer;
    }

    @Override
    public boolean isShuffled() {
        return isShuffled;
    }

    @Override
    public void close() {
        for (TupleActor actor : getActors()) {
            actor.handleProducerCompleted();
        }
    }
}
