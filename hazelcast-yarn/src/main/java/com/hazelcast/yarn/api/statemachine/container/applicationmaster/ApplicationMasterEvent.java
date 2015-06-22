package com.hazelcast.yarn.api.statemachine.container.applicationmaster;

import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;

public enum ApplicationMasterEvent implements ContainerEvent {
    SUBMIT_DAG,
    EXECUTION_PLAN_BUILD_FAILED,
    EXECUTION_PLAN_READY,
    EXECUTE,
    INVALIDATE,
    INTERRUPT_EXECUTION,
    EXECUTION_INTERRUPTED,
    INTERRUPTION_FAILURE,
    EXECUTION_ERROR,
    EXECUTION_COMPLETED,
    FINALIZE,
}
