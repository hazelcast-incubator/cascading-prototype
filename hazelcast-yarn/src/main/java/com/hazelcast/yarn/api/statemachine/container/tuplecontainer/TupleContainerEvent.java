package com.hazelcast.yarn.api.statemachine.container.tuplecontainer;

import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;

public enum TupleContainerEvent implements ContainerEvent {
    START,
    EXECUTE,
    EXECUTION_COMPLETED,
    INTERRUPT,
    INTERRUPTED,
    INVALIDATE_CONTAINER,
    FINALIZE
}
