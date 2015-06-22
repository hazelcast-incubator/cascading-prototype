package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;

public class ApplicationMasterPayLoadFactory {
    public static ContainerPayLoadProcessor getProcessor(ApplicationMasterEvent event, ApplicationMaster applicationMaster) {
        switch (event) {
            case SUBMIT_DAG:
                return new ExecutionPlanBuilderProcessor(applicationMaster);
            case EXECUTE:
                return new ExecuteApplicationProcessor(applicationMaster);
            case INTERRUPT_EXECUTION:
                return new InterrupterApplicationProcessor(applicationMaster);
            case EXECUTION_ERROR:
                return new ExecutionErrorProcessor(applicationMaster);
            case INTERRUPTION_FAILURE:
                return new InterruptionFailureProcessor(applicationMaster);
            case INVALIDATE:
                return new InvalidateApplicationProcessor(applicationMaster);
            case EXECUTION_COMPLETED:
                return new ExecutionCompletionProcessor(applicationMaster);
            case FINALIZE:
                return new ApplicationFinalizerProcessor(applicationMaster);
            default:
                return null;
        }
    }
}
