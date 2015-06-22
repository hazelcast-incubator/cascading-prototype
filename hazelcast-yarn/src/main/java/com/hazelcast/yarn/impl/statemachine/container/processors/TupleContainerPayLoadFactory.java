package com.hazelcast.yarn.impl.statemachine.container.processors;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;

public class TupleContainerPayLoadFactory {
    public static ContainerPayLoadProcessor getProcessor(TupleContainerEvent event, TupleContainer container) {
        switch (event) {
            case START:
                return new StartTupleContainerProcessor(container);
            case EXECUTE:
                return new ExecuteContainerProcessor(container);
            case INTERRUPT:
                return new InterruptContainerProcessor(container);
            case INTERRUPTED:
                return new InterruptedContainerProcessor(container);
            case EXECUTION_COMPLETED:
                return new ExecutionCompletedProcessor(container);
            default:
                return null;
        }
    }
}
