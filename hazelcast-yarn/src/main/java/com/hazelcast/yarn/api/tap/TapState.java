package com.hazelcast.yarn.api.tap;

import com.hazelcast.yarn.api.statemachine.StateMachineState;

public enum TapState implements StateMachineState {
    OPENED,
    CLOSED
}
