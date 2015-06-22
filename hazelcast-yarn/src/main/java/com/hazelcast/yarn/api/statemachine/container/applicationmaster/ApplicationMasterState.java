package com.hazelcast.yarn.api.statemachine.container.applicationmaster;

import com.hazelcast.yarn.api.statemachine.container.ContainerState;

public enum ApplicationMasterState implements ContainerState {
    NEW,
    DAG_SUBMITTED,
    READY_FOR_EXECUTION,
    INVALID_DAG_FOR_EXECUTION,
    EXECUTING,
    EXECUTION_INTERRUPTING,
    EXECUTION_INTERRUPTED,
    EXECUTION_FAILED,
    EXECUTION_SUCCESS,
    INVALIDATED,
    FINALIZED
}
