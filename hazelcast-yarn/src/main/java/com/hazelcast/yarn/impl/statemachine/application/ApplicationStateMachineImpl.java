package com.hazelcast.yarn.impl.statemachine.application;

import java.util.Map;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.LinkedMapBuilder;
import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.ApplicationStateMachine;
import com.hazelcast.yarn.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.statemachine.application.ApplicationState;
import com.hazelcast.yarn.api.statemachine.application.ApplicationEvent;
import com.hazelcast.yarn.api.statemachine.application.ApplicationResponse;

public class ApplicationStateMachineImpl extends AbstractStateMachineImpl<ApplicationEvent, ApplicationState, ApplicationResponse> implements
        ApplicationStateMachine {
    private static final Map<ApplicationState, Map<ApplicationEvent, ApplicationState>> stateTransitionMatrix =
            LinkedMapBuilder.<ApplicationState, Map<ApplicationEvent, ApplicationState>>builder().
                    put(
                            ApplicationState.NEW, LinkedMapBuilder.of(
                                    ApplicationEvent.INIT_START, ApplicationState.INIT_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.INIT_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.INIT_SUCCESS, ApplicationState.INIT_SUCCESS,
                                    ApplicationEvent.INIT_FAILURE, ApplicationState.INIT_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.INIT_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.LOCALIZATION_START, ApplicationState.LOCALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.INIT_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.LOCALIZATION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.LOCALIZATION_SUCCESS, ApplicationState.LOCALIZATION_SUCCESS,
                                    ApplicationEvent.LOCALIZATION_FAILURE, ApplicationState.LOCALIZATION_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.LOCALIZATION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.SUBMIT_START, ApplicationState.SUBMIT_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.LOCALIZATION_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.SUBMIT_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.SUBMIT_SUCCESS, ApplicationState.SUBMIT_SUCCESS,
                                    ApplicationEvent.SUBMIT_FAILURE, ApplicationState.SUBMIT_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.SUBMIT_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_START, ApplicationState.EXECUTION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.INTERRUPTION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_START, ApplicationState.EXECUTION_IN_PROGRESS,
                                    ApplicationEvent.EXECUTION_FAILURE, ApplicationState.INTERRUPTION_SUCCESS
                            )
                    ).
                    put(
                            ApplicationState.SUBMIT_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.EXECUTION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_SUCCESS, ApplicationState.EXECUTION_SUCCESS,
                                    ApplicationEvent.EXECUTION_FAILURE, ApplicationState.EXECUTION_FAILURE,
                                    ApplicationEvent.INTERRUPTION_SUCCESS, ApplicationState.INTERRUPTION_SUCCESS,
                                    ApplicationEvent.INTERRUPTION_FAILURE, ApplicationState.INTERRUPTION_FAILURE,
                                    ApplicationEvent.INTERRUPTION_START, ApplicationState.INTERRUPTION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.INTERRUPTION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_FAILURE, ApplicationState.INTERRUPTION_IN_PROGRESS,
                                    ApplicationEvent.INTERRUPTION_SUCCESS, ApplicationState.INTERRUPTION_SUCCESS
                            )
                    ).
                    put(
                            ApplicationState.EXECUTION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_START, ApplicationState.EXECUTION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.EXECUTION_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.FINALIZATION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_FAILURE, ApplicationState.FINALIZATION_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.FINALIZATION_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS
                            )
                    ).
                    build();

    public ApplicationStateMachineImpl(String name, StateMachineRequestProcessor<ApplicationEvent> processor, NodeEngine nodeEngine, ApplicationContext applicationContext) {
        super(name, stateTransitionMatrix, processor, nodeEngine, applicationContext);
    }

    @Override
    protected ApplicationResponse output(ApplicationEvent applicationEvent, ApplicationState nextState) {
        if (nextState == null)
            return ApplicationResponse.FAILURE;
        else
            return ApplicationResponse.SUCCESS;
    }

    @Override
    protected ApplicationExecutor getExecutor() {
        return getApplicationContext().getApplicationStateMachineExecutor();
    }

    @Override
    protected ApplicationState defaultState() {
        return ApplicationState.NEW;
    }
}

