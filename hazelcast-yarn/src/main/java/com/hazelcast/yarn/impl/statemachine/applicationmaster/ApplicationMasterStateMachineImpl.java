package com.hazelcast.yarn.impl.statemachine.applicationmaster;

import java.util.Map;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.LinkedMapBuilder;
import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.AppMasterStateMachine;
import com.hazelcast.yarn.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;

public class ApplicationMasterStateMachineImpl extends AbstractStateMachineImpl<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>
        implements AppMasterStateMachine {
    private static final Map<ApplicationMasterState, Map<ApplicationMasterEvent, ApplicationMasterState>> stateTransitionMatrix =
            LinkedMapBuilder.<ApplicationMasterState, Map<ApplicationMasterEvent, ApplicationMasterState>>builder().
                    put(
                            ApplicationMasterState.NEW, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.SUBMIT_DAG, ApplicationMasterState.DAG_SUBMITTED,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.DAG_SUBMITTED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.EXECUTION_PLAN_READY, ApplicationMasterState.READY_FOR_EXECUTION,
                                    ApplicationMasterEvent.EXECUTION_PLAN_BUILD_FAILED, ApplicationMasterState.INVALID_DAG_FOR_EXECUTION,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.READY_FOR_EXECUTION, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_FAILED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_INTERRUPTED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTING, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTION_ERROR, ApplicationMasterState.EXECUTION_FAILED,
                                    ApplicationMasterEvent.EXECUTION_COMPLETED, ApplicationMasterState.EXECUTION_SUCCESS,
                                    ApplicationMasterEvent.INTERRUPT_EXECUTION, ApplicationMasterState.EXECUTION_INTERRUPTING,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_INTERRUPTING, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.INTERRUPTION_FAILURE, ApplicationMasterState.INVALIDATED,
                                    ApplicationMasterEvent.EXECUTION_INTERRUPTED, ApplicationMasterState.EXECUTION_INTERRUPTED,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.INVALID_DAG_FOR_EXECUTION, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.INVALIDATE, ApplicationMasterState.INVALIDATED
                            )
                    ).
                    put(
                            ApplicationMasterState.INVALIDATED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED
                            )
                    ).
                    build();

    public ApplicationMasterStateMachineImpl(String name, StateMachineRequestProcessor<ApplicationMasterEvent> processor, NodeEngine nodeEngine, ApplicationContext applicationContext) {
        super(name, stateTransitionMatrix, processor, nodeEngine, applicationContext);
    }

    @Override
    protected ApplicationExecutor getExecutor() {
        return getApplicationContext().getApplicationMasterStateMachineExecutor();
    }

    @Override
    protected ApplicationMasterState defaultState() {
        return ApplicationMasterState.NEW;
    }

    @Override
    protected ApplicationMasterResponse output(ApplicationMasterEvent applicationMasterEvent, ApplicationMasterState nextState) {
        if (nextState == null)
            return ApplicationMasterResponse.FAILURE;
        else
            return ApplicationMasterResponse.SUCCESS;
    }
}
