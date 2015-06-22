package com.hazelcast.yarn.impl.statemachine.applicationmaster.requests;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ExecutionErrorHolder;
import com.hazelcast.yarn.impl.container.DefaultExecutionErrorHolder;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;

public class NotifyContainerExecutionError implements ContainerRequest<ApplicationMasterEvent, ExecutionErrorHolder> {
    private final ExecutionErrorHolder executionErrorHolder;

    public NotifyContainerExecutionError(TupleContainer container, Throwable error) {
        this.executionErrorHolder = new DefaultExecutionErrorHolder(container, error);
    }

    @Override
    public ApplicationMasterEvent getContainerEvent() {
        return ApplicationMasterEvent.EXECUTION_ERROR;
    }

    @Override
    public ExecutionErrorHolder getPayLoad() {
        return executionErrorHolder;
    }
}
