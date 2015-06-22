package com.hazelcast.yarn.impl.container;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ExecutionErrorHolder;

public class DefaultExecutionErrorHolder implements ExecutionErrorHolder {
    private final Throwable error;
    private final TupleContainer tupleContainer;

    public DefaultExecutionErrorHolder(TupleContainer tupleContainer, Throwable error) {
        this.error = error;
        this.tupleContainer = tupleContainer;

    }

    @Override
    public TupleContainer getContainer() {
        return this.tupleContainer;
    }

    @Override
    public Throwable getError() {
        return this.error;
    }
}
