package com.hazelcast.yarn.api.tap;

import com.hazelcast.yarn.api.statemachine.StateMachineOutput;

public enum TapResponse implements StateMachineOutput {
    SUCCESS,
    FAILURE
}
