package com.hazelcast.yarn.api.statemachine.application;

import com.hazelcast.yarn.api.statemachine.StateMachineEvent;

public enum ApplicationEvent implements StateMachineEvent {
    INIT_START,
    INIT_SUCCESS,
    INIT_FAILURE,

    LOCALIZATION_START,
    LOCALIZATION_SUCCESS,
    LOCALIZATION_FAILURE,

    SUBMIT_START,
    SUBMIT_SUCCESS,
    SUBMIT_FAILURE,

    INTERRUPTION_START,
    INTERRUPTION_SUCCESS,
    INTERRUPTION_FAILURE,

    EXECUTION_START,
    EXECUTION_SUCCESS,
    EXECUTION_FAILURE,

    FINALIZATION_START,
    FINALIZATION_FAILURE
}
