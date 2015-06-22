package com.hazelcast.yarn.api.container;

public interface ExecutionErrorHolder {
    TupleContainer getContainer();

    Throwable getError();
}
