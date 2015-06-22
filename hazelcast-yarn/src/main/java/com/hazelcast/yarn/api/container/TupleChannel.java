package com.hazelcast.yarn.api.container;

import java.util.List;

import com.hazelcast.yarn.api.actor.TupleActor;

public interface TupleChannel {
    List<TupleActor> getActors();

    TupleContainer getSourceContainer();

    TupleContainer getTargetContainer();

    boolean isShuffled();

    void close();
}
